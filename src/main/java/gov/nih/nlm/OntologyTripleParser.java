package gov.nih.nlm;

import static gov.nih.nlm.OntologyElementParser.createURI;
import static gov.nih.nlm.OntologyElementParser.parseOntologyElements;
import static gov.nih.nlm.OntologyGraphBuilder.parsePredicate;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntStatement;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.CollectorStreamTriples;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;

/**
 * Parses each ontology file in the data/obo directory to produce triple sets
 * sorted by the types of nodes the triples contain.
 */
public class OntologyTripleParser {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	// Assign selected predicate namespaces
	private static final List<String> predicateNameSpaces = List.of("http://www.w3.org/2000/01/rdf-schema#",
			"http://purl.obolibrary.org/obo/", "http://purl.org/dc/", "http://www.geneontology.org/formats/oboInOwl#");

	/**
	 * Get the filled node of a triple containing either a blank subject or object
	 * node.
	 *
	 * @param triple Triple containing a blank subject or object node
	 * @return Filled node
	 */
	public static Node getFNode(Triple triple) {
		if (triple.getSubject().isBlank() && triple.getObject().isBlank()) {
			throw new IllegalArgumentException("Triple subject and object are both blank");
		} else if (!triple.getSubject().isBlank() && !triple.getObject().isBlank()) {
			throw new IllegalArgumentException("Triple subject and object are both filled");
		} else if (triple.getSubject().isBlank()) {
			return triple.getObject();
		} else {
			return triple.getSubject();
		}
	}

	/**
	 * Read an OWL file and identify the root namespace. Collect triples from
	 * statements which contain a named object and a predicate in one of the
	 * specified name spaces. Handle statements which contain an anonymous object
	 * and an rdfs:subClassOf predicate by flattening all statements about the
	 * anonymous object into a single statement with a named subject and object,
	 * then collecting the triples from the single statement.
	 * 
	 * @param owlFile Path to OWL file
	 * @return List of triples with named subject and object nodes
	 */
	public static List<Triple> collectTriplesFromFile(Path owlFile) {
		List<Triple> triples = new ArrayList<>();
		System.out.println("Collecting triples from within " + owlFile);
		long startTime = System.nanoTime();

		// Read the OWL file
		OntModel ontModel = OntModelFactory.createModel();
		RDFDataMgr.read(ontModel, owlFile.toString());

		// Identify the root namespace
		Resource ontology = ontModel.listResourcesWithProperty(RDF.type, OWL.Ontology).nextOptional().orElse(null);
		// Statement versionIRI = null;
		Statement rootTerm;
		String rootNS;
		if (ontology != null) {
			// versionIRI = ontology.getProperty(OWL.versionIRI);
			rootTerm = ontology.getProperty(ontModel.createProperty("http://purl.obolibrary.org/obo/IAO_0000700"));
			if (rootTerm != null) {
				rootNS = rootTerm.getResource().getURI().split("_")[0];
			} else {
				if (ontModel.classes().findFirst().isPresent()) {
					rootNS = ontModel.classes().findFirst().get().getURI().split("_")[0];
				} else {
					rootNS = null;
					System.out.println("No root NS found: first class found is null");
					System.exit(1);
				}
			}
		} else {
			rootNS = null;
			System.out.println("No root NS found: no ontology resource found");
			System.exit(1);
		}

		// Consider each statement about each class in the root name space
		ontModel.classes().filter(ontClass -> ontClass.getURI().startsWith(rootNS)).forEach(ontClass -> {
			ontClass.statements().forEach(classStatement -> {
				String predicateURI = classStatement.getPredicate().getURI();
				if (!classStatement.getObject().isAnon()) {
					// Handle statements which contain a named object
					if (predicateNameSpaces.stream().anyMatch(ns -> predicateURI.startsWith(ns))) {
						// Collect statements as triples which contain a predicate in one of the
						// specified name spaces
						triples.add(classStatement.asTriple());
					}
				} else if (predicateURI.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
					// Handle statements which contain an anonymous object, and an rdfs:subClassOf
					// predicate by considering each statement about the anonymous object in order
					// to flatten these statements into a single statement with a named subject and
					// object
					Resource subject = classStatement.getSubject();
					Property predicate = null;
					RDFNode object = null;
					for (OntStatement objectStatement : ontModel
							.statements(classStatement.getObject().asResource(), null, null).toList()) {
						String predicateResource = objectStatement.getPredicate().getURI();
						if (predicateResource.equals("http://www.w3.org/2002/07/owl#onProperty")) {
							predicate = ontModel.getProperty(objectStatement.getObject().asResource().getURI());
						} else if (predicateResource.equals("http://www.w3.org/2002/07/owl#someValuesFrom")) {
							object = objectStatement.getObject();
						}
					}
					// Create the single statement, and collect it as a triple
					if (predicate != null && object != null) {
						triples.add(ontModel.createStatement(subject, predicate, object).asTriple());
					}
				}
			});
		});
		long stopTime = System.nanoTime();
		System.out.println("Collected triples from within " + owlFile + " in " + (stopTime - startTime) / 1e9 + " s");
		return triples;
	}

	/**
	 * Collect unique triples with named subject and object nodes.
	 *
	 * @param files Paths to ontology files
	 *
	 * @return Set of unique triples with named subject and object nodes
	 */
	public static HashSet<Triple> collectUniqueTriples(List<Path> files) {
		HashSet<Triple> uniqueTriplesSet = new HashSet<>();
		System.out.println("Collecting unique triples from within " + files.size() + " files");
		long startTime = System.nanoTime();
		for (Path file : files) {
			if (file.getFileName().toString().equals("ro.owl"))
				continue;
			List<Triple> triples = collectTriplesFromFile(file);
			uniqueTriplesSet.addAll(triples);
		}
		long stopTime = System.nanoTime();
		System.out.println("Collected " + uniqueTriplesSet.size() + " unique triples from within " + files.size()
				+ " in " + (stopTime - startTime) / 1e9 + " s");
		return uniqueTriplesSet;
	}

	/**
	 * Parse each ontology file in the data/obo directory to collect unique triples
	 *
	 * @param args (None expected)
	 */
	public static void main(String[] args) {

		// List onotology files
		String oboPattern = ".*\\.owl";
		List<Path> oboFiles;
		try {
			oboFiles = listFilesMatchingPattern(oboDir.toString(), oboPattern);
			if (oboFiles.isEmpty()) {
				System.out.println("No OBO files found matching the pattern " + oboPattern);
				System.exit(1);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// Map terms and labels
		String roPattern = "ro.owl";
		List<Path> roFile;
		try {
			roFile = listFilesMatchingPattern(oboDir.toString(), roPattern);
			if (roFile.isEmpty()) {
				System.out.println("No RO file found");
				System.exit(2);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Map<String, OntologyElementMap> ontologyElementMaps = parseOntologyElements(roFile);

		// Collect unique triples
		HashSet<Triple> uniqueTriplesSet = collectUniqueTriples(oboFiles);
	}
}
