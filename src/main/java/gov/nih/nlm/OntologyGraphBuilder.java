package gov.nih.nlm;

import static gov.nih.nlm.OntologyElementParser.createURI;
import static gov.nih.nlm.OntologyElementParser.parseOntologyElements;
import static gov.nih.nlm.OntologyTripleParser.collectUniqueSOFNodeTriples;
import static gov.nih.nlm.OntologyTripleParser.parseOntologyTriples;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class OntologyGraphBuilder {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	public static final Path oboDir = usrDir.resolve("data/obo");

	// Assign vertices to include in the graph
	private static final ArrayList<String> validVertices = new ArrayList<>(
			Arrays.asList("BMC", "CHEBI", "CHEMBL", "CL", "CS", "CSD", "GO", "GS", "HP", "HsapDv", "MONDO", "MmusDv",
					"NCBITaxon", "NCT", "Orphanet", "PATO", "PR", "PUB", "RS", "SO", "UBERON"));

	private static final Pattern parenPattern = Pattern.compile("(.*) (\\(.*\\))$");

	// Define a record describing a vertex
	public record VTuple(String term, String id, String number, Boolean isValidVertex) {
	}

	/**
	 * Parse a URI to find an ontology term, ID, and number, and test if the ID is a
	 * valid vertex.
	 * 
	 * @param n Node from which to create VTuple
	 * @return VTuple created from node
	 */
	public static VTuple createVTuple(Node n) {
		VTuple vtuple = new VTuple(null, null, null, false);
		if (!n.isURI())
			return vtuple;
		URI uri;
		try {
			uri = createURI(n.getURI());
		} catch (RuntimeException e) {
			return vtuple;
		}
		String path = uri.getPath();
		if (path == null)
			return vtuple;
		Path fileName = Paths.get(path).getFileName();
		if (fileName == null)
			return vtuple;
		String term = fileName.toString();
		String[] tokens = null;
		if (term.contains("_")) {
			tokens = term.split("_");
		} else if (term.contains(":")) {
			tokens = term.split(":");
		}
		String id = null;
		String number = null;
		if (tokens != null) {
			id = tokens[0];
			number = tokens[1];
		}
		Boolean isValidVertex = validVertices.contains(id);
		return new VTuple(term, id, number, isValidVertex);
	}

	/**
	 * Parse a URI node to obtain the fragment or last element of the path. Useful
	 * only for predicate nodes.
	 *
	 * @param ontologyElementMaps Maps terms and labels
	 * @param p                   Predicate node to parse
	 * @return Label resulting from parsing the node
	 */
	public static String parsePredicate(Map<String, OntologyElementMap> ontologyElementMaps, Node p)
			throws RuntimeException {
		String label;
		if (p.isURI()) {
			label = createURI(p.getURI()).getFragment();
			if (label == null) {
				label = createURI(p.getURI()).getPath();
				if (label != null) {
					label = label.substring(label.lastIndexOf("/") + 1);
					if (ontologyElementMaps.get("ro").terms.containsKey(label)) {
						label = ontologyElementMaps.get("ro").terms.get(label).label;
					}
				}
			}
		} else {
			throw new RuntimeException("Unexpected predicate " + p);
		}
		return label;
	}

	/**
	 * Construct vertices using triples parsed from specified ontology files that
	 * contain a filled subject and object which contain an ontology ID contained in
	 * the valid vertices collection.
	 *
	 * @param uniqueTriples     Unique triples with which to construct vertices
	 * @param arangoDbUtilities Utilities for accessing ArangoDB
	 * @param graph             ArangoDB graph in which to create vertex collections
	 * @param vertexCollections ArangoDB vertex collections
	 * @param vertexDocuments   ArangoDB vertex documents
	 */
	public static void constructVertices(HashSet<Triple> uniqueTriples, ArangoDbUtilities arangoDbUtilities,
			ArangoGraph graph, Map<String, ArangoVertexCollection> vertexCollections,
			Map<String, Map<String, BaseDocument>> vertexDocuments) {

		// Collect vertex keys for each vertex collection to prevent constructing
		// duplicate vertices in the vertex collection
		Map<String, Set<String>> vertexKeys = new HashMap<>();

		// Process triples
		long startTime = System.nanoTime();
		System.out.println("Constructing vertices using " + uniqueTriples.size() + " triples");
		int nVertices = 0;
		for (Triple triple : uniqueTriples) {

			// Consider the subject and object nodes
			ArrayList<Node> nodes = new ArrayList<>(Arrays.asList(triple.getSubject(), triple.getObject()));
			for (Node n : nodes) {

				// Construct a vertex from the current node, if it contains a valid id
				VTuple vtuple = createVTuple(n);
				if (vtuple.isValidVertex) {

					// Create a vertex collection, if needed
					if (!vertexCollections.containsKey(vtuple.id)) {
						vertexCollections.put(vtuple.id,
								arangoDbUtilities.createOrGetVertexCollection(graph, vtuple.id));
						vertexDocuments.put(vtuple.id, new HashMap<>());
						vertexKeys.put(vtuple.id, new HashSet<>());
					}

					// Construct the vertex, if needed
					if (!vertexKeys.get(vtuple.id).contains(vtuple.number)) {
						nVertices++;
						BaseDocument doc = new BaseDocument(vtuple.number);
						vertexDocuments.get(vtuple.id).put(vtuple.number, doc);
						vertexKeys.get(vtuple.id).add(vtuple.number);
					}
				}
			}
		}
		long stopTime = System.nanoTime();
		System.out.println("Constructed " + nVertices + " vertices using " + uniqueTriples.size() + " triples in "
				+ (stopTime - startTime) / 1e9 + " s");
	}

	/**
	 * Update vertices using triples parsed from specified ontology files that
	 * contain a filled subject which contains an ontology ID contained in the valid
	 * vertices collection, and a filled object literal.
	 *
	 * @param uniqueTriples       Unique triples with which to construct vertices
	 * @param ontologyElementMaps Maps terms and labels
	 * @param vertexDocuments     ArangoDB vertex documents
	 */
	public static void updateVertices(HashSet<Triple> uniqueTriples,
			Map<String, OntologyElementMap> ontologyElementMaps, Map<String, Map<String, BaseDocument>> vertexDocuments)
			throws RuntimeException {

		// Process triples
		long startTime = System.nanoTime();
		System.out.println("Updating vertices using " + uniqueTriples.size() + " triples");
		Set<String> updatedVertices = new HashSet<>(); // For counting only
		for (Triple triple : uniqueTriples) {

			// Ensure the object contains a literal
			Node o = triple.getObject();
			if (!o.isLiteral()) {
				continue;
			}

			// Ensure the subject contains a valid ontology ID
			VTuple vtuple = createVTuple(triple.getSubject());
			if (vtuple.isValidVertex) {

				// Parse the predicate
				String attribute = parsePredicate(ontologyElementMaps, triple.getPredicate());

				// Parse the object
				String literal = o.getLiteralValue().toString();

				// Get the vertex to update
				updatedVertices.add(vtuple.id + "-" + vtuple.number);
				BaseDocument doc = vertexDocuments.get(vtuple.id).get(vtuple.number);

				// Handle each attribute as a set of literal values
				Matcher parenMatcher;
				HashSet<String> literals;
				HashSet<String> strippedLiterals = new HashSet<>();
				if (doc.getAttribute(attribute) == null) {
					// Initialize the set of literal values
					literals = new HashSet<>();
					doc.addAttribute(attribute, literals);
				} else {
					// Get the set of literal values
					literals = (HashSet<String>) doc.getAttribute(attribute);

					// Create a set of literal values stripped of ending parentheticals
					for (String l : literals) {
						parenMatcher = parenPattern.matcher(l);
						if (parenMatcher.matches()) {
							strippedLiterals.add(parenMatcher.group(1));
						} else {
							strippedLiterals.add(l);
						}
					}
				}
				// Remove a literal value in the set if it equals the current literal value
				// stripped of an ending parenthetical
				parenMatcher = parenPattern.matcher(literal);
				if (parenMatcher.matches()) {
					literals.remove(parenMatcher.group(1));
				}
				// Only add the current literal value if it is not in the set of literal values
				// stripped of ending parentheticals
				if (!strippedLiterals.contains(literal)) {
					literals.add(literal);
				}
			}
		}
		long stopTime = System.nanoTime();
		System.out.println("Updated " + updatedVertices.size() + " vertices using " + uniqueTriples.size()
				+ " triples in " + (stopTime - startTime) / 1e9 + " s");
	}

	/**
	 * Insert all vertices after they have been constructed and updated to improve
	 * performance.
	 *
	 * @param vertexCollections ArangoDB vertex collections
	 * @param vertexDocuments   ArangoDB vertex documents
	 */
	public static void insertVertices(Map<String, ArangoVertexCollection> vertexCollections,
			Map<String, Map<String, BaseDocument>> vertexDocuments) {
		System.out.println("Inserting vertices");
		long startTime = System.nanoTime();
		int nVertices = 0;
		for (String id : vertexDocuments.keySet()) {
			ArangoVertexCollection vertexCollection = vertexCollections.get(id);
			for (String number : vertexDocuments.get(id).keySet()) {
				nVertices++;
				BaseDocument doc = vertexDocuments.get(id).get(number);
				Map<String, Object> properties = doc.getProperties();
				Set<String> literals;
				for (String attribute : properties.keySet()) {
					if (attribute.equals("_key"))
						continue;
					literals = (HashSet<String>) doc.getAttribute(attribute);
					if (literals.size() == 1) {
						doc.updateAttribute(attribute, literals.toArray()[0]);
					}
				}
				if (vertexCollection.getVertex(doc.getKey(), doc.getClass()) == null) {
					Object deprecated = doc.getAttribute("deprecated");
					Object label = doc.getAttribute("label");
					if ((deprecated != null && deprecated.toString().contains("true"))
							|| (label != null && label.toString().contains("obsolete"))) {
						continue;
					}
					vertexCollection.insertVertex(doc);
				} else {
					vertexCollection.updateVertex(doc.getKey(), doc);
				}
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
	 * @param uniqueTriples       Unique triples with which to construct vertices
	 * @param ontologyElementMaps Maps terms and labels
	 * @param graph               ArangoDB graph in which to create vertex
	 *                            collections
	 * @param edgeCollections     ArangoDB edge collections
	 * @param edgeDocuments       ArangoDB edge documents
	 */
	public static void constructEdges(HashSet<Triple> uniqueTriples,
			Map<String, OntologyElementMap> ontologyElementMaps, ArangoDbUtilities arangoDbUtilities, ArangoGraph graph,
			Map<String, ArangoEdgeCollection> edgeCollections, Map<String, Map<String, BaseEdgeDocument>> edgeDocuments)
			throws RuntimeException {

		// Collect edge keys in each edge collection to prevent constructing duplicate
		// edges in the edge collection
		Map<String, Set<String>> edgeKeys = new HashMap<>();

		// Process triples
		long startTime = System.nanoTime();
		System.out.println("Constructing edges using " + uniqueTriples.size() + " triples");
		int nEdges = 0;
		for (Triple triple : uniqueTriples) {

			// Ensure the subject contains a valid ontology ID
			VTuple subjectVTuple = createVTuple(triple.getSubject());
			if (!subjectVTuple.isValidVertex)
				continue;

			// Ensure the object contains a valid ontology ID
			VTuple objectVTuple = createVTuple(triple.getObject());
			if (!objectVTuple.isValidVertex)
				continue;

			// Parse the predicate
			String label = parsePredicate(ontologyElementMaps, triple.getPredicate());

			// Create an edge collection, if needed
			String idPair = subjectVTuple.id + "-" + objectVTuple.id;
			if (!edgeCollections.containsKey(idPair)) {
				edgeCollections.put(idPair,
						arangoDbUtilities.createOrGetEdgeCollection(graph, subjectVTuple.id, objectVTuple.id));
				edgeDocuments.put(idPair, new HashMap<>());
				edgeKeys.put(idPair, new HashSet<>());
			}

			// Construct the edge, if needed
			String key = subjectVTuple.number + "-" + objectVTuple.number;
			if (!edgeKeys.get(idPair).contains(key)) {
				nEdges++;
				BaseEdgeDocument doc = new BaseEdgeDocument(key, subjectVTuple.id + "/" + subjectVTuple.number,
						objectVTuple.id + "/" + objectVTuple.number);
				doc.addAttribute("label", label);
				edgeDocuments.get(idPair).put(key, doc);
				edgeKeys.get(idPair).add(key);
			}
		}
		long stopTime = System.nanoTime();
		System.out.println("Constructed " + nEdges + " edges from " + uniqueTriples.size() + " triples in "
				+ (stopTime - startTime) / 1e9 + " s");
	}

	/**
	 * Get the document collection name, which is typically an ontology id for a
	 * vertex document, or an ontology id pair for an edge document, from a document
	 * id.
	 *
	 * @param documentId Document id
	 * @return Document collection name
	 */
	public static String getDocumentCollectionName(String documentId) {
		String documentCollectionName = null;
		if (documentId != null && documentId.contains("/")) {
			documentCollectionName = documentId.substring(0, documentId.indexOf("/"));
		}
		return documentCollectionName;
	}

	/**
	 * Get the document key, which is typically an ontology term number for a vertex
	 * document, or an ontology term number pair for an edge document, from a
	 * document id.
	 *
	 * @param documentId Document id
	 * @return Document key
	 */
	public static String getDocumentKey(String documentId) {
		String documentKey = null;
		if (documentId != null && documentId.contains("/")) {
			documentKey = documentId.substring(documentId.indexOf("/") + 1);
		}
		return documentKey;
	}

	/**
	 * Insert all edges after they have been constructed to improve performance.
	 *
	 * @param vertexCollections ArangoDB vertex collections
	 * @param edgeCollections   ArangoDB edge collections
	 * @param edgeDocuments     ArangoDB edge documents
	 */
	public static void insertEdges(Map<String, ArangoVertexCollection> vertexCollections,
			Map<String, ArangoEdgeCollection> edgeCollections,
			Map<String, Map<String, BaseEdgeDocument>> edgeDocuments) {
		System.out.println("Inserting edges");
		long startTime = System.nanoTime();
		int nEdges = 0;
		for (String idPair : edgeDocuments.keySet()) {
			ArangoEdgeCollection edgeCollection = edgeCollections.get(idPair);
			for (String key : edgeDocuments.get(idPair).keySet()) {
				nEdges++;
				BaseEdgeDocument doc = edgeDocuments.get(idPair).get(key);
				String docKey = doc.getKey();
				String fromId = doc.getFrom();
				String fromName = getDocumentCollectionName(fromId);
				String fromKey = getDocumentKey(fromId);
				String toId = doc.getTo();
				String toName = getDocumentCollectionName(toId);
				String toKey = getDocumentKey(toId);
				if (edgeCollection.getEdge(docKey, doc.getClass()) == null) {
					if (!(vertexCollections.get(fromName).getVertex(fromKey, BaseDocument.class) == null)
							&& !(vertexCollections.get(toName).getVertex(toKey, BaseDocument.class) == null)) {
						edgeCollection.insertEdge(doc);
					}
				} else {
					edgeCollection.updateEdge(docKey, doc);
				}
			}
		}
		long stopTime = System.nanoTime();
		System.out.println("Inserted " + nEdges + " edges in " + (stopTime - startTime) / 1e9 + " s");
	}

	/**
	 * Load triples parsed from each ontology file in the data/obo directory into a
	 * local ArangoDB server instance.
	 *
	 * @param args (None expected)
	 */
	public static void main(String[] args) {

		// List ontology files
		String oboPath;
		if (args.length > 0) {
			oboPath = args[0];
		} else {
			oboPath = oboDir.toString();
		}
		String oboPattern = ".*\\.owl";
		List<Path> oboFiles;
		try {
			oboFiles = listFilesMatchingPattern(oboPath, oboPattern);
			if (oboFiles.isEmpty()) {
				System.out.println("No OBO files found matching the pattern " + oboPattern);
				System.exit(1);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// Parse ontology elements and triples, and connect unique triples
		Map<String, OntologyElementMap> ontologyElementMaps = parseOntologyElements(oboFiles);
		Map<String, TripleTypeSets> ontologyTripleTypeSets = parseOntologyTriples(oboFiles, ontologyElementMaps);
		HashSet<Triple> uniqueTriples = collectUniqueSOFNodeTriples(oboFiles, ontologyTripleTypeSets);

		// Connect to a local ArangoDB server instance
		ArangoDbUtilities arangoDbUtilities = new ArangoDbUtilities();
		String databaseName;
		if (args.length > 1) {
			databaseName = args[1];
		} else {
			databaseName = "Cell-KN-Ontologies";
		}

		// Always recreate the database
		arangoDbUtilities.deleteDatabase(databaseName);
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);

		// Always recreate the graph
		String graphName;
		if (args.length > 2) {
			graphName = args[2];
		} else {
			graphName = "KN-Ontologies-v2.0";
		}
		arangoDbUtilities.deleteGraph(db, graphName);
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);

		// Create, update, and insert the vertices
		Map<String, ArangoVertexCollection> vertexCollections = new HashMap<>();
		Map<String, Map<String, BaseDocument>> vertexDocuments = new HashMap<>();
		constructVertices(uniqueTriples, arangoDbUtilities, graph, vertexCollections, vertexDocuments);
		try {
			updateVertices(uniqueTriples, ontologyElementMaps, vertexDocuments);
		} catch (RuntimeException e) {
			throw new RuntimeException(e);
		}
		insertVertices(vertexCollections, vertexDocuments);

		// Create, and insert the edges
		Map<String, ArangoEdgeCollection> edgeCollections = new HashMap<>();
		Map<String, Map<String, BaseEdgeDocument>> edgeDocuments = new HashMap<>();
		try {
			constructEdges(uniqueTriples, ontologyElementMaps, arangoDbUtilities, graph, edgeCollections,
					edgeDocuments);
		} catch (RuntimeException e) {
			throw new RuntimeException(e);
		}
		insertEdges(vertexCollections, edgeCollections, edgeDocuments);

		// Disconnect from a local ArangoDB server instance
		arangoDbUtilities.arangoDB.shutdown();
	}
}
