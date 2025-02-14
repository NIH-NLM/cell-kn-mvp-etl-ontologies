package gov.nih.nlm;

import static gov.nih.nlm.OntologyElementParser.createURI;
import static gov.nih.nlm.OntologyElementParser.parseOntologyElements;
import static gov.nih.nlm.OntologyTripleParser.parseOntologyTriples;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoEdgeCollection;
import com.arangodb.ArangoGraph;
import com.arangodb.ArangoVertexCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

/**
 * Loads triples parsed from each ontology file in the data/obo directory into a
 * local ArangoDB server instance.
 */
public class OntologyTripleLoader {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	// Assign vertices to include in the graph
	private static final ArrayList<String> validVertices = new ArrayList<>(Arrays.asList("CHEBI", "CL", "ENSG", "GO",
			"HsapDv", "MONDO", "MmusDv", "NCBITaxon", "PATO", "PCL", "PCLCS", "PR", "SO", "UBERON"));

	// Connect to a local ArangoDB server instance
	private static final ArangoDbUtilities arangoDbUtilities = new ArangoDbUtilities();

	private static final Map<String, OntologyElementMap> ontologyElementMaps = null;
	private static Map<String, TripleTypeSets> ontologyTripleTypeSets = null;
	private static final Map<String, ArangoVertexCollection> vertexCollections = new HashMap<>();
	private static final Map<String, Map<String, BaseDocument>> vertexDocuments = new HashMap<>();
	private static final Map<String, ArangoEdgeCollection> edgeCollections = new HashMap<>();
	private static final Map<String, Map<String, BaseEdgeDocument>> edgeDocuments = new HashMap<>();

	/**
	 * Construct vertices using triples parsed from specified ontology files that
	 * contain a filled subject and object which contain an ontology ID contained in
	 * the valid vertices collection.
	 *
	 * @param files Paths to ontology files
	 * @param graph ArangoDB graph in which to create vertex collections
	 */
	public static void constructVertices(List<Path> files, ArangoGraph graph) {

		// Collect vertex keys to prevent constructing duplicate vertices
		Map<String, Set<String>> vertexKeys = new HashMap<>();

		// Process triples parsed from each file
		for (Path file : files) {
			long startTime = System.nanoTime();
			String oboFNm = file.getFileName().toString();
			List<Triple> triples = ontologyTripleTypeSets.get(oboFNm).soFNodeTriples;
			System.out.println("Constructing vertices using " + triples.size() + " triples from " + oboFNm);
			int nVertices = 0;
			for (Triple triple : triples) {

				// Consider the subject and object nodes
				ArrayList<Node> nodes = new ArrayList<>(Arrays.asList(triple.getSubject(), triple.getObject()));
				for (Node n : nodes) {
					// Construct a vertex from the current node, if it contains a valid id
					if (!n.isURI())
						continue;
					if (createURI(n.getURI()).getPath() == null)
						continue;
					if (Paths.get(createURI(n.getURI()).getPath()).getFileName() == null)
						continue;
					String term = Paths.get(createURI(n.getURI()).getPath()).getFileName().toString();
					String id = term.contains("_") ? term.split("_")[0] : null;
					if (validVertices.contains(id)) {

						// Create a vertex collection, if needed
						if (!vertexCollections.containsKey(id)) {
							vertexCollections.put(id, arangoDbUtilities.createOrGetVertexCollection(graph, id));
							vertexDocuments.put(id, new HashMap<>());
							vertexKeys.put(id, new HashSet<>());
						}

						// Construct the vertex, if needed
						if (!vertexKeys.get(id).contains(term)) {
							nVertices++;
							BaseDocument doc = new BaseDocument(term);
							vertexDocuments.get(id).put(term, doc);
							vertexKeys.get(id).add(term);
						}
					}
				}
			}
			long stopTime = System.nanoTime();
			System.out.println("Constructed " + nVertices + " vertices from " + oboFNm + " in "
					+ (stopTime - startTime) / 1e9 + " s");
		}
	}

	/**
	 * Update vertices using triples parsed from specified ontology files that
	 * contain a filled subject which contains an ontology ID contained in the valid
	 * vertices collection, and a filled object literal.
	 *
	 * @param files Paths to ontology files
	 */
	public static void updateVertices(List<Path> files) {

		// Process triples parsed from each file
		for (Path file : files) {
			long startTime = System.nanoTime();
			String oboFNm = file.getFileName().toString();
			List<Triple> triples = ontologyTripleTypeSets.get(oboFNm).soFNodeTriples;
			System.out.println("Updating vertices using " + triples.size() + " triples from " + oboFNm);
			Set<String> updatedVertices = new HashSet<>(); // For counting
			for (Triple triple : triples) {

				// Ensure the object contains a literal
				Node o = triple.getObject();
				if (!o.isLiteral()) {
					continue;
				}

				// Ensure the subject contains a valid ontology ID
				Node s = triple.getSubject();
				if (createURI(s.getURI()).getPath() == null)
					continue;
				if (Paths.get(createURI(s.getURI()).getPath()).getFileName() == null)
					continue;
				String term = Paths.get(createURI(s.getURI()).getPath()).getFileName().toString();
				String id = term.contains("_") ? term.split("_")[0] : null;
				if (validVertices.contains(id)) {

					// Parse the predicate
					Node p = triple.getPredicate();
					String attribute = createURI(p.getURI()).getFragment();
					if (attribute == null) {
						attribute = createURI(p.getURI()).getPath();
						attribute = attribute.substring(attribute.lastIndexOf('/') + 1);
						// TODO: Translate the predicate using the relationship ontology
					}

					// Parse the object
					String literal = o.getLiteralValue().toString();

					// Update the corresponding vertex
					updatedVertices.add(id + "-" + term);
					BaseDocument doc = vertexDocuments.get(id).get(term);
					doc.addAttribute(attribute, literal);
				}
			}
			long stopTime = System.nanoTime();
			System.out.println("Updated " + updatedVertices.size() + " vertices from " + oboFNm + " in "
					+ (stopTime - startTime) / 1e9 + " s");
		}
	}

	/**
	 * Insert all vertices after they have been constructed and updated to improve
	 * performance.
	 */
	public static void insertVertices() {
		System.out.println("Insert vertices");
		long startTime = System.nanoTime();
		int nVertices = 0;
		for (String id : vertexDocuments.keySet()) {
			ArangoVertexCollection vertexCollection = vertexCollections.get(id);
			for (String term : vertexDocuments.get(id).keySet()) {
				nVertices++;
				BaseDocument doc = vertexDocuments.get(id).get(term);
				vertexCollection.insertVertex(doc);
			}
		}
		long stopTime = System.nanoTime();
		System.out.println("Inserted " + nVertices + " vertices in " + (stopTime - startTime) / 1e9 + " s");
	}

	/**
	 * Construct edges using triples parsed from specified ontology files that
	 * contain a filled subject and object which contain an ontology ID contained in
	 * the valid vertices collection.
	 * 
	 * @param files
	 * @param graph
	 */
	public static void constructEdges(List<Path> files, ArangoGraph graph) {

		// Collect edge keys to prevent constructing duplicate edges
		Map<String, Set<String>> edgeKeys = new HashMap<>();

		// Process triples parsed from each file
		for (Path file : files) {
			long startTime = System.nanoTime();
			String oboFNm = file.getFileName().toString();
			List<Triple> triples = ontologyTripleTypeSets.get(oboFNm).soFNodeTriples;
			System.out.println("Constructing edges using " + triples.size() + " triples from " + oboFNm);
			int nEdges = 0;
			for (Triple triple : triples) {

				// Ensure the subject contains a valid ontology ID
				Node s = triple.getSubject();
				if (createURI(s.getURI()).getPath() == null)
					continue;
				if (Paths.get(createURI(s.getURI()).getPath()).getFileName() == null)
					continue;
				String s_term = Paths.get(createURI(s.getURI()).getPath()).getFileName().toString();
				String s_id = s_term.contains("_") ? s_term.split("_")[0] : null;
				if (!validVertices.contains(s_id))
					continue;

				// Ensure the subject contains a valid ontology ID
				Node o = triple.getObject();
				if (!o.isURI())
					continue;
				if (createURI(o.getURI()).getPath() == null)
					continue;
				if (Paths.get(createURI(o.getURI()).getPath()).getFileName() == null)
					continue;
				String o_term = Paths.get(createURI(o.getURI()).getPath()).getFileName().toString();
				String o_id = o_term.contains("_") ? o_term.split("_")[0] : null;
				if (!validVertices.contains(o_id))
					continue;

				// Parse the predicate
				Node p = triple.getPredicate();
				String label = createURI(p.getURI()).getFragment();
				if (label == null) {
					label = createURI(p.getURI()).getPath();
					label = label.substring(label.lastIndexOf('/') + 1);
				}

				// Create an edge collection, if needed
				String id = s_id + "-" + o_id;
				if (!edgeCollections.containsKey(id)) {
					edgeCollections.put(id, arangoDbUtilities.createOrGetEdgeCollection(graph, s_id, o_id));
					edgeDocuments.put(id, new HashMap<>());
					edgeKeys.put(id, new HashSet<>());
				}

				// Construct the edge, if needed
				String key = s_term + "-" + o_term;
				if (!edgeKeys.get(id).contains(key)) {
					nEdges++;
					BaseEdgeDocument doc = new BaseEdgeDocument(key, s_id + "/" + s_term, o_id + "/" + o_term);
					doc.addAttribute("label", label);
					edgeDocuments.get(id).put(key, doc);
					edgeKeys.get(id).add(key);
				}
			}
			long stopTime = System.nanoTime();
			System.out.println(
					"Constructed " + nEdges + " edges from " + oboFNm + " in " + (stopTime - startTime) / 1e9 + " s");
		}
	}

	/**
	 * Insert all edges after they have been constructed to improve performance.
	 */
	public static void insertEdges() {
		System.out.println("Inserting edges");
		long startTime = System.nanoTime();
		int nEdges = 0;
		for (String id : edgeDocuments.keySet()) {
			ArangoEdgeCollection edgeCollection = edgeCollections.get(id);
			for (String term : edgeDocuments.get(id).keySet()) {
				nEdges++;
				BaseDocument doc = edgeDocuments.get(id).get(term);
				edgeCollection.insertEdge(doc);
			}
		}
		long stopTime = System.nanoTime();
		System.out.println("Inserted " + nEdges + " edges in " + (stopTime - startTime) / 1e9 + " s");
	}

	/**
	 * Loads triples parsed from each ontology file in the data/obo directory into a
	 * local ArangoDB server instance.
	 *
	 * @param args (None expected)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String directoryPath = oboDir.toString();
		String filePattern = ".*\\.owl";
//		String filePattern = "cl.owl";
		List<Path> files = null;
		try {
			files = listFilesMatchingPattern(directoryPath, filePattern);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Map<String, OntologyElementMap> ontologyElementMaps = null;
		if (files.isEmpty()) {
			System.out.println("No files found matching the pattern.");
		} else {
			ontologyElementMaps = parseOntologyElements(files);
			ontologyTripleTypeSets = parseOntologyTriples(files);
		}
		String databaseName = "Sanger";
		arangoDbUtilities.deleteDatabase(databaseName);
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		String graphName = "Combined";
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);
		constructVertices(files, graph);
		updateVertices(files);
		insertVertices();
		constructEdges(files, graph);
		insertEdges();
	}
}
