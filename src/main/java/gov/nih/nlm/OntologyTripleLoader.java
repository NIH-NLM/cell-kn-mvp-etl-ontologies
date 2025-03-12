package gov.nih.nlm;

import static gov.nih.nlm.OntologyElementParser.createURI;
import static gov.nih.nlm.OntologyElementParser.parseOntologyElements;
import static gov.nih.nlm.OntologyTripleParser.parseOntologyTriples;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
	public static final Path oboDir = usrDir.resolve("data/obo");

	// Assign vertices to include in the graph
	private static final ArrayList<String> validVertices = new ArrayList<>(
			Arrays.asList("CHEBI", "CL", "ENSG", "GO", "HsapDv", "MONDO", "MmusDv", "NCBITaxon", "PATO", "PCL", "PCLCS",
					"PR", "UBERON", "BMC", "CHEMBL", "CS", "CSD", "DS", "GS", "PUB", "SO"));

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
		} catch (URISyntaxException e) {
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
			throws URISyntaxException {
		String label = null;
		if (p.isURI()) { // Always true for predicates
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
		}
		return label;
	}

	/**
	 * Construct vertices using triples parsed from specified ontology files that
	 * contain a filled subject and object which contain an ontology ID contained in
	 * the valid vertices collection.
	 *
	 * @param files                  Paths to ontology files
	 * @param ontologyTripleTypeSets Triple sets sorted by the types of nodes the
	 *                               triples contain
	 * @param arangoDbUtilities      Utilities for accessing ArangoDB
	 * @param graph                  ArangoDB graph in which to create vertex
	 *                               collections
	 * @param vertexCollections      ArangoDB vertex collections
	 * @param vertexDocuments        ArangoDB vertex documents
	 */
	public static void constructVertices(List<Path> files, Map<String, TripleTypeSets> ontologyTripleTypeSets,
			ArangoDbUtilities arangoDbUtilities, ArangoGraph graph,
			Map<String, ArangoVertexCollection> vertexCollections,
			Map<String, Map<String, BaseDocument>> vertexDocuments) {

		// Collect vertex keys for each vertex collection to prevent constructing
		// duplicate vertices in the vertex collection
		Map<String, Set<String>> vertexKeys = new HashMap<>();

		// Process triples parsed from each file
		for (Path file : files) {
			long startTime = System.nanoTime();
			String oboFNm = file.getFileName().toString();
			if (oboFNm.equals("ro.owl"))
				continue;
			List<Triple> triples = ontologyTripleTypeSets
					.get(oboFNm.substring(0, oboFNm.lastIndexOf("."))).soFNodeTriples;
			System.out.println("Constructing vertices using " + triples.size() + " triples from " + oboFNm);
			int nVertices = 0;
			for (Triple triple : triples) {

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
			System.out.println("Constructed " + nVertices + " vertices from " + oboFNm + " in "
					+ (stopTime - startTime) / 1e9 + " s");
		}
	}

	/**
	 * Update vertices using triples parsed from specified ontology files that
	 * contain a filled subject which contains an ontology ID contained in the valid
	 * vertices collection, and a filled object literal.
	 *
	 * @param files                  Paths to ontology files
	 * @param ontologyElementMaps    Maps terms and labels
	 * @param ontologyTripleTypeSets Triple sets sorted by the types of nodes the
	 *                               triples contain
	 * @param vertexDocuments        ArangoDB vertex documents
	 */
	public static void updateVertices(List<Path> files, Map<String, OntologyElementMap> ontologyElementMaps,
			Map<String, TripleTypeSets> ontologyTripleTypeSets, Map<String, Map<String, BaseDocument>> vertexDocuments)
			throws URISyntaxException {

		// Process triples parsed from each file
		for (Path file : files) {
			long startTime = System.nanoTime();
			String oboFNm = file.getFileName().toString();
			if (oboFNm.equals("ro.owl"))
				continue;
			List<Triple> triples = ontologyTripleTypeSets
					.get(oboFNm.substring(0, oboFNm.lastIndexOf("."))).soFNodeTriples;
			System.out.println("Updating vertices using " + triples.size() + " triples from " + oboFNm);
			Set<String> updatedVertices = new HashSet<>(); // For counting only
			for (Triple triple : triples) {

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

					// Update the corresponding vertex
					updatedVertices.add(vtuple.id + "-" + vtuple.number);
					BaseDocument doc = vertexDocuments.get(vtuple.id).get(vtuple.number);
					Set<String> literals;
					if (doc.getAttribute(attribute) == null) {
						literals = new HashSet<>();
						doc.addAttribute(attribute, literals);
					} else {
						literals = (HashSet<String>) doc.getAttribute(attribute);
					}
					literals.add(literal);
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
	 *
	 * @param vertexCollections ArangoDB vertex collections
	 * @param vertexDocuments   ArangoDB vertex documents
	 */
	public static void insertVertices(Map<String, ArangoVertexCollection> vertexCollections,
			Map<String, Map<String, BaseDocument>> vertexDocuments) {
		System.out.println("Insert vertices");
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
				try {
					vertexCollection.insertVertex(doc);
				} catch (Exception e) {
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
	 * @param files Paths to ontology files
	 * @param graph ArangoDB graph in which to create vertex collections
	 */
	public static void constructEdges(List<Path> files, Map<String, OntologyElementMap> ontologyElementMaps,
			Map<String, TripleTypeSets> ontologyTripleTypeSets, ArangoDbUtilities arangoDbUtilities, ArangoGraph graph,
			Map<String, ArangoEdgeCollection> edgeCollections, Map<String, Map<String, BaseEdgeDocument>> edgeDocuments)
			throws URISyntaxException {

		// Collect edge keys in each edge collection to prevent constructing duplicate
		// edges in the edge collection
		Map<String, Set<String>> edgeKeys = new HashMap<>();

		// Process triples parsed from each file
		for (Path file : files) {
			long startTime = System.nanoTime();
			String oboFNm = file.getFileName().toString();
			if (oboFNm.equals("ro.owl"))
				continue;
			List<Triple> triples = ontologyTripleTypeSets
					.get(oboFNm.substring(0, oboFNm.lastIndexOf("."))).soFNodeTriples;
			System.out.println("Constructing edges using " + triples.size() + " triples from " + oboFNm);
			int nEdges = 0;
			for (Triple triple : triples) {

				// Ensure the subject contains a valid ontology ID
				VTuple s_vtuple = createVTuple(triple.getSubject());
				if (!s_vtuple.isValidVertex)
					continue;

				// Ensure the object contains a valid ontology ID
				VTuple o_vtuple = createVTuple(triple.getObject());
				if (!o_vtuple.isValidVertex)
					continue;

				// Parse the predicate
				String label = parsePredicate(ontologyElementMaps, triple.getPredicate());

				// Create an edge collection, if needed
				String idPair = s_vtuple.id + "-" + o_vtuple.id;
				if (!edgeCollections.containsKey(idPair)) {
					edgeCollections.put(idPair,
							arangoDbUtilities.createOrGetEdgeCollection(graph, s_vtuple.id, o_vtuple.id));
					edgeDocuments.put(idPair, new HashMap<>());
					edgeKeys.put(idPair, new HashSet<>());
				}

				// Construct the edge, if needed
				String key = s_vtuple.number + "-" + o_vtuple.number;
				if (!edgeKeys.get(idPair).contains(key)) {
					nEdges++;
					BaseEdgeDocument doc = new BaseEdgeDocument(key, s_vtuple.id + "/" + s_vtuple.number,
							o_vtuple.id + "/" + o_vtuple.number);
					doc.addAttribute("label", label);
					edgeDocuments.get(idPair).put(key, doc);
					edgeKeys.get(idPair).add(key);
				}
			}
			long stopTime = System.nanoTime();
			System.out.println(
					"Constructed " + nEdges + " edges from " + oboFNm + " in " + (stopTime - startTime) / 1e9 + " s");
		}
	}

	/**
	 * Insert all edges after they have been constructed to improve performance.
	 *
	 * @param edgeCollections ArangoDB edge collections
	 * @param edgeDocuments   ArangoDB edge documents
	 */
	public static void insertEdges(Map<String, ArangoEdgeCollection> edgeCollections,
			Map<String, Map<String, BaseEdgeDocument>> edgeDocuments) {
		System.out.println("Inserting edges");
		long startTime = System.nanoTime();
		int nEdges = 0;
		for (String idPair : edgeDocuments.keySet()) {
			ArangoEdgeCollection edgeCollection = edgeCollections.get(idPair);
			for (String key : edgeDocuments.get(idPair).keySet()) {
				nEdges++;
				BaseDocument doc = edgeDocuments.get(idPair).get(key);
				try {
					edgeCollection.insertEdge(doc);
				} catch (Exception e) {
					edgeCollection.updateEdge(doc.getKey(), doc);
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
		String directoryPath;
		if (args.length > 0) {
			directoryPath = args[0];
		} else {
			directoryPath = oboDir.toString();
		}
		String filePattern = ".*\\.owl";
		List<Path> files;
		try {
			files = listFilesMatchingPattern(directoryPath, filePattern);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Map<String, OntologyElementMap> ontologyElementMaps = null;
		Map<String, TripleTypeSets> ontologyTripleTypeSets = null;
		if (files.isEmpty()) {
			System.out.println("No files found matching the pattern.");
		} else {
			try {
				ontologyElementMaps = parseOntologyElements(files);
				ontologyTripleTypeSets = parseOntologyTriples(files);
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
		}

		// Connect to a local ArangoDB server instance
		ArangoDbUtilities arangoDbUtilities = new ArangoDbUtilities();
		String databaseName;
		if (args.length > 1) {
			databaseName = args[1];
		} else {
			databaseName = "Cell-KN-v2.0";
		}

		// Always recreate the database
		arangoDbUtilities.deleteDatabase(databaseName);
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);

		// Always recreate the graph
		String graphName;
		if (args.length > 2) {
			graphName = args[2];
		} else {
			graphName = "Combined";
		}
		arangoDbUtilities.deleteGraph(db, graphName);
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);

		// Create, update, and insert the vertices
		Map<String, ArangoVertexCollection> vertexCollections = new HashMap<>();
		Map<String, Map<String, BaseDocument>> vertexDocuments = new HashMap<>();
		constructVertices(files, ontologyTripleTypeSets, arangoDbUtilities, graph, vertexCollections, vertexDocuments);
		try {
			updateVertices(files, ontologyElementMaps, ontologyTripleTypeSets, vertexDocuments);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		insertVertices(vertexCollections, vertexDocuments);

		// Create, and insert the edges
		Map<String, ArangoEdgeCollection> edgeCollections = new HashMap<>();
		Map<String, Map<String, BaseEdgeDocument>> edgeDocuments = new HashMap<>();
		try {
			constructEdges(files, ontologyElementMaps, ontologyTripleTypeSets, arangoDbUtilities, graph,
					edgeCollections, edgeDocuments);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		insertEdges(edgeCollections, edgeDocuments);
	}
}
