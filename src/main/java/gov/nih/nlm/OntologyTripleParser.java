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
import org.apache.jena.vocabulary.RDFS;

/**
 * Parses each ontology file in the data/obo directory to produce triple sets
 * sorted by the types of nodes the triples contain.
 */
public class OntologyTripleParser {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	private static final List<String> predicateNameSpaces = List.of("http://www.w3.org/2000/01/rdf-schema#",
			"http://purl.obolibrary.org/obo/", "http://purl.org/dc/", "http://www.geneontology.org/formats/oboInOwl#");

	/**
	 * Collect all triples into sets containing triples with:
	 * <ul>
	 * <li>Filled subject and object</li>
	 * <li>Blank subject and filled object</li>
	 * <li>Filled subject and blank object</li>
	 * <li>Blank subject and object</li>
	 * </ul>
	 * 
	 * @param oboPth         Path to ontology file
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void populateTripleTypeSets(Path oboPth, TripleTypeSets tripleTypeSets) {
		// Only populate empty triple type sets
		if (tripleTypeSets.size() != 0) {
			throw new RuntimeException("TripleTypeSets have already been populated");
		}
		// Parse ontology file, and process each triple
		CollectorStreamTriples inputStream = new CollectorStreamTriples();
		RDFParser.source(oboPth).parse(inputStream);
		int nTriples = 0;
		for (Triple triple : inputStream.getCollected()) {
			nTriples++;
			Node s = triple.getSubject();
			Node o = triple.getObject();
			if (!s.isBlank() && !o.isBlank()) {
				// Collect filled node triples
				tripleTypeSets.soFNodeTriples.add(triple);
			} else if (s.isBlank() && !o.isBlank()) {
				// Collect blank subject node triples in sets identified by the blank subject
				// node
				if (!tripleTypeSets.sBNodeTriples.containsKey(s)) {
					tripleTypeSets.sBNodeTriples.put(s, new ArrayList<>());
				}
				tripleTypeSets.sBNodeTriples.get(s).add(triple);
			} else if (!s.isBlank() && o.isBlank()) {
				// Collect blank object node triples in sets identified by the blank object node
				if (!tripleTypeSets.oBNodeTriples.containsKey(o)) {
					tripleTypeSets.oBNodeTriples.put(o, new ArrayList<>());
				}
				tripleTypeSets.oBNodeTriples.get(o).add(triple);
			} else {
				// Collect blank subject and object node triples
				tripleTypeSets.soBNodeTriples.add(triple);
			}
		}
		// Ensure all triples typed, and assign number of triples
		if (tripleTypeSets.size() != nTriples) {
			System.err.println("Warning: populateTripleTypeSets: tripleTypeSets.size() != nTriples");
		} else {
			tripleTypeSets.nTriples = nTriples;
		}
	}

	/**
	 * Move blank object triple sets into blank subject triple sets. If this method
	 * is called after populating the triple type sets, the blank subject triple
	 * sets will each use a single blank node throughout the set.
	 *
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void moveOtoSBNodeTriples(TripleTypeSets tripleTypeSets) {
		for (Node key : tripleTypeSets.oBNodeTriples.keySet().toArray(new Node[0])) {
			if (tripleTypeSets.sBNodeTriples.containsKey(key)) {
				tripleTypeSets.sBNodeTriples.get(key).addAll(tripleTypeSets.oBNodeTriples.get(key));
				tripleTypeSets.oBNodeTriples.remove(key);
			}
		}
		// Ensure no triples have been added or removed
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: moveOtoSBNodeTriples: tripleTypeSets.size() != nTriples");
		}
	}

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
	 * Flatten Axiom triple sets by identifying the components of the Axiom triple
	 * contained in the triples of the set, creating a corresponding triple, and
	 * adding it to the list of triples with filled subject and object nodes.
	 * <p>
	 * Use triples remaining after flattening to add annotations corresponding to
	 * the identified subject node.
	 * </p>
	 *
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 * @param key            Node identifying triple lists containing the same blank
	 *                       node
	 */
	public static void flattenAxiomTripleSets(TripleTypeSets tripleTypeSets, Node key,
			Map<String, OntologyElementMap> ontologyElementMaps) throws RuntimeException {
		Node flattenedSubject = null;
		Node flattenedPredicate = null;
		Node flattenedObject = null;
		ArrayList<Triple> flattenedTriples = new ArrayList<>();
		ArrayList<Triple> remainingTriples = new ArrayList<>();
		for (Triple triple : tripleTypeSets.sBNodeTriples.get(key)) {
			String predicateFragment = createURI(triple.getPredicate().getURI()).getFragment();
			// Identify components of the Axiom triple
			if (predicateFragment != null && predicateFragment.equals("annotatedSource")) {
				flattenedTriples.add(triple);
				flattenedSubject = getFNode(triple);
			} else if (predicateFragment != null && predicateFragment.equals("annotatedProperty")) {
				flattenedTriples.add(triple);
				flattenedPredicate = getFNode(triple);
			} else if (predicateFragment != null && predicateFragment.equals("annotatedTarget")) {
				flattenedTriples.add(triple);
				flattenedObject = getFNode(triple);
			} else if (predicateFragment != null && !predicateFragment.equals("type")) {
				remainingTriples.add(triple);
			}
		}
		if (flattenedSubject != null && flattenedPredicate != null && flattenedObject != null) {
			Triple flattenedTripleToAdd = Triple.create(flattenedSubject, flattenedPredicate, flattenedObject);

			String flattenedPredicateLabel = parsePredicate(ontologyElementMaps, flattenedPredicate);
			String flattenedObjectLabel = null;
			if (flattenedObject.isURI()) {
				flattenedObjectLabel = parsePredicate(ontologyElementMaps, flattenedObject);
			} else if (flattenedObject.isLiteral()) {
				flattenedObjectLabel = flattenedObject.getLiteral().toString();
			}
			Node combinedObject;

			// Handle remaining triples
			StringBuilder combinedObjectLabel = new StringBuilder();
			for (Triple remainingTriple : remainingTriples) {
				Triple remainingTripleToAdd = null;

				Node remainingPredicate = remainingTriple.getPredicate();
				Node remainingObject = remainingTriple.getObject();

				String remainingPredicateLabel = parsePredicate(ontologyElementMaps, remainingPredicate);
				String remainingObjectLabel = null;
				if (remainingObject.isURI()) {
					remainingObjectLabel = parsePredicate(ontologyElementMaps, remainingObject);
				} else if (remainingObject.isLiteral()) {
					remainingObjectLabel = remainingObject.getLiteral().toString();
				}

				if (flattenedObject.isURI()) {
					// Append flattened predicate and object label to remaining object label
					combinedObject = NodeFactory.createLiteral(
							remainingObjectLabel + ", for '" + flattenedPredicateLabel + "': " + flattenedObjectLabel);
					remainingTripleToAdd = Triple.create(flattenedSubject, remainingPredicate, combinedObject);

				} else if (flattenedObject.isLiteral() && remainingObject.isURI()) {
					// Append remaining predicate and object label to flattened object label
					combinedObject = NodeFactory.createLiteral(
							flattenedObjectLabel + " (" + remainingPredicateLabel + ": " + remainingObjectLabel + ")");
					flattenedTripleToAdd = Triple.create(flattenedSubject, flattenedPredicate, combinedObject);

				} else if (flattenedObject.isLiteral() && remainingObject.isLiteral()) {
					// Append remaining predicate and object label to combined object label
					combinedObjectLabel.append(remainingPredicateLabel).append(": ").append(remainingObjectLabel)
							.append(", ");
				}
				flattenedTriples.add(remainingTriple);

				// Add remaining triple to the list of triples with filled subject and object
				// nodes
				if (remainingTripleToAdd != null) {
					tripleTypeSets.nTriples++;
					tripleTypeSets.soFNodeTriples.add(remainingTripleToAdd);
					tripleTypeSets.flattenedBNodeTriples.add(remainingTriple);
				}
			}

			// Add flattened triple to the list of triples with filled subject and object
			// nodes
			if (combinedObjectLabel.length() > 0) {
				combinedObject = NodeFactory.createLiteral(flattenedObjectLabel + " ("
						+ combinedObjectLabel.substring(0, combinedObjectLabel.lastIndexOf(",")) + ")");
				flattenedTripleToAdd = Triple.create(flattenedSubject, flattenedPredicate, combinedObject);
			}
			tripleTypeSets.nTriples++;
			tripleTypeSets.soFNodeTriples.add(flattenedTripleToAdd);

			// Keep track of flattened triples
			tripleTypeSets.flattenedBNodeTriples.addAll(flattenedTriples);
			tripleTypeSets.sBNodeTriples.get(key).removeAll(flattenedTriples);
		}
	}

	/**
	 * Flatten Restriction triple sets by identifying the components of the
	 * Restriction triple contained in the triples of the set, creating a
	 * corresponding triple, and adding it to the list of triples with filled
	 * subject and object nodes.
	 *
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 * @param key            Node identifying triple lists containing the same blank
	 *                       node
	 */
	public static void flattenRestrictionTripleSets(TripleTypeSets tripleTypeSets, Node key) throws RuntimeException {
		Node flattenedSubject = null;
		Node flattenedPredicate = null;
		Node flattenedObject = null;
		ArrayList<Triple> flattenedTriples = new ArrayList<>();
		ArrayList<Triple> remainingTriples = new ArrayList<>(); // For debugging
		for (Triple triple : tripleTypeSets.sBNodeTriples.get(key)) {
			String predicateFragment = createURI(triple.getPredicate().getURI()).getFragment();
			// Identify components of the Restriction triple
			if (predicateFragment.equals("subClassOf")) {
				flattenedTriples.add(triple);
				flattenedSubject = getFNode(triple);
			} else if (predicateFragment.equals("onProperty")) {
				flattenedTriples.add(triple);
				flattenedPredicate = getFNode(triple);
			} else if (predicateFragment.equals("someValuesFrom")) {
				flattenedTriples.add(triple);
				flattenedObject = getFNode(triple);
			} else if (!predicateFragment.equals("type")) {
				remainingTriples.add(triple);
			}
		}
		if (flattenedSubject != null && flattenedPredicate != null && flattenedObject != null) {
			// Create triple, and add it to the list of triples with filled subject and
			// object nodes
			tripleTypeSets.nTriples++;
			tripleTypeSets.soFNodeTriples.add(Triple.create(flattenedSubject, flattenedPredicate, flattenedObject));

			// Keep track of flattened triples
			tripleTypeSets.flattenedBNodeTriples.addAll(flattenedTriples);
			tripleTypeSets.sBNodeTriples.get(key).removeAll(flattenedTriples);
		}
	}

	/**
	 * Flatten Axiom and Restriction triple sets.
	 * 
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void flattenSBNodeTriples(TripleTypeSets tripleTypeSets,
			Map<String, OntologyElementMap> ontologyElementMaps) throws RuntimeException {
		// Process each triple list containing the same blank node successively without
		// assuming Axiom and Restriction triple sets are not contained in the same list
		String[] fragments = { "Axiom", "Restriction" };
		for (String fragment : fragments) {
			for (Node key : tripleTypeSets.sBNodeTriples.keySet().toArray(new Node[0])) {
				for (Triple triple : tripleTypeSets.sBNodeTriples.get(key)) {
					Node o = triple.getObject();
					if (o.isURI()) {
						String objectFragment = createURI(o.getURI()).getFragment();
						if (objectFragment != null && objectFragment.equals(fragment)) {
							if (fragment.equals("Axiom")) {
								flattenAxiomTripleSets(tripleTypeSets, key, ontologyElementMaps);
							} else if (fragment.equals("Restriction")) {
								flattenRestrictionTripleSets(tripleTypeSets, key);
							}
							break;
						}
					}
				}
			}
			// Ensure no triples have been added or removed
			if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
				System.err.println("Warning: flattenSBNodeTriples: tripleTypeSets.size() != nTriples");
			}
		}
	}

	/**
	 * Collect triples with blank subject and object if the blank subject and object
	 * are contained in the subject blank node triple sets, then remove these
	 * linking blank triples from the both blank triples.
	 *
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void collectLinkingBNodeTriples(TripleTypeSets tripleTypeSets) {
		for (Triple triple : tripleTypeSets.soBNodeTriples.toArray(new Triple[0])) {
			Node s = triple.getSubject();
			Node o = triple.getObject();
			if (tripleTypeSets.sBNodeTriples.containsKey(s) && tripleTypeSets.sBNodeTriples.containsKey(o)) {
				tripleTypeSets.linkingBNodeTriples.add(triple);
			}
		}
		tripleTypeSets.soBNodeTriples.removeAll(tripleTypeSets.linkingBNodeTriples);

		// Ensure no triples have been added or removed
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: collectLinkingBNodeTriples: tripleTypeSets.size() != nTriples");
		}
	}

	/**
	 * Combine subject blank node triple sets linked by triple with blank subject
	 * and object nodes.
	 *
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void linkSBNodeTriples(TripleTypeSets tripleTypeSets) {
		for (Triple triple : tripleTypeSets.linkingBNodeTriples.toArray(new Triple[0])) {
			Node s = triple.getSubject();
			Node o = triple.getObject();
			tripleTypeSets.sBNodeTriples.get(s).addAll(tripleTypeSets.sBNodeTriples.get(o));
			tripleTypeSets.sBNodeTriples.get(o).clear();
		}
		// Ensure no triples have been added or removed
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: linkSBNodeTriples: tripleTypeSets.size() != nTriples");
		}
	}

	/**
	 * Parse ontology files to produce triple sets sorted by the types of nodes the
	 * triples contain.
	 *
	 * @param files Paths to ontology files
	 * @return Map by ontology file name containing triple sets sorted by the types
	 *         of nodes the triples contain
	 */
	public static Map<String, TripleTypeSets> parseOntologyTriples(List<Path> files,
			Map<String, OntologyElementMap> ontologyElementMaps) throws RuntimeException {
		Map<String, TripleTypeSets> ontologyTripleTypeSets = new HashMap<>();
		for (Path file : files) {
			String oboFNm = file.getFileName().toString();
			if (oboFNm.equals("ro.owl"))
				continue;
			System.out.println("Parsing ontology triples in " + oboFNm);
			TripleTypeSets tripleTypeSets = new TripleTypeSets();
			populateTripleTypeSets(file, tripleTypeSets);
			moveOtoSBNodeTriples(tripleTypeSets);
			flattenSBNodeTriples(tripleTypeSets, ontologyElementMaps);
			// TODO: Take a another look at the utility of these
			// collectLinkingBNodeTriples(tripleTypeSets);
			// linkSBNodeTriples(tripleTypeSets);
			ontologyTripleTypeSets.put(oboFNm.substring(0, oboFNm.lastIndexOf(".")), tripleTypeSets);
		}
		System.out.println("Parsed ontology triples from " + files.size() + " files.");
		return ontologyTripleTypeSets;
	}

	public static List<Triple> collectTriplesFromFile(Path owlFile) {

		List<Triple> triples = new ArrayList<>();

		// Read the OWL file
		OntModel ontModel = OntModelFactory.createModel();
		RDFDataMgr.read(ontModel, owlFile.toString());

		// Print a few values describing the OWL file, and identify the root namespace
		Resource ontology = ontModel.listResourcesWithProperty(RDF.type, OWL.Ontology).nextOptional().orElse(null);
		Statement versionIRI = null;
		Statement versionInfo = null;
		Statement comment = null;
		Statement rootTerm = null;
		String rootNS;
		if (ontology != null) {
			versionIRI = ontology.getProperty(OWL.versionIRI);
			if (versionIRI != null) {
				System.out.println("versionIRI = " + versionIRI.getObject());
			}
			versionInfo = ontology.getProperty(OWL.versionInfo);
			if (versionInfo != null) {
				System.out.println("versionInfo = " + versionInfo.getObject());
			}
			comment = ontology.getProperty(RDFS.comment);
			if (comment != null) {
				System.out.println("comment = " + comment.getObject());
			}
			rootTerm = ontology.getProperty(ontModel.createProperty("http://purl.obolibrary.org/obo/IAO_0000700"));
			if (rootTerm != null) {
				System.out.println("rootTerm " + rootTerm.getObject());
				rootNS = rootTerm.getResource().getURI().split("_")[0];
			} else {
				rootNS = null;
			}
		} else {
			rootNS = null;
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
					triples.add(ontModel.createStatement(subject, predicate, object).asTriple());
				}
			});
		});
		return triples;
	}

	/**
	 * Collect unique triples with filled subject and object nodes.
	 *
	 * @param files                  Paths to ontology files
	 * @param ontologyTripleTypeSets Map by ontology file name containing triple
	 *                               sets sorted by the types of nodes the triples
	 *                               contain
	 * @return Set of unique triples with filled subject and object nodes
	 */
	public static HashSet<Triple> collectUniqueSOFNodeTriples(List<Path> files,
			Map<String, TripleTypeSets> ontologyTripleTypeSets) {
		HashSet<Triple> uniqueTriplesSet = new HashSet<>();
		System.out.println("Collecting unique triples from within " + files.size() + " files");
		long startTime = System.nanoTime();
		for (Path file : files) {
			String oboFNm = file.getFileName().toString();
			if (oboFNm.equals("ro.owl"))
				continue;
			List<Triple> triples = ontologyTripleTypeSets
					.get(oboFNm.substring(0, oboFNm.lastIndexOf("."))).soFNodeTriples;
			uniqueTriplesSet.addAll(triples);
		}
		long stopTime = System.nanoTime();
		System.out.println("Collected " + uniqueTriplesSet.size() + " unique triples from within " + files.size()
				+ " in " + (stopTime - startTime) / 1e9 + " s");
		return uniqueTriplesSet;
	}

	/**
	 * Parse each ontology file in the data/obo directory to produce triple sets
	 * sorted by the types of nodes the triples contain.
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

		// Parse ontology triples
		Map<String, TripleTypeSets> ontologyTripleTypeSets = null;
		if (oboFiles.isEmpty()) {
			System.out.println("No files found matching the pattern.");
		} else {
			ontologyTripleTypeSets = parseOntologyTriples(oboFiles, ontologyElementMaps);
		}

		// Collect unique triples
		HashSet<Triple> uniqueTriplesSet = collectUniqueSOFNodeTriples(oboFiles, ontologyTripleTypeSets);
	}
}
