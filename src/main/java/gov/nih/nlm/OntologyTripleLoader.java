package gov.nih.nlm;

import static gov.nih.nlm.OntologyElementParser.parseOntologyElements;
import static gov.nih.nlm.OntologyTripleParser.parseOntologyTriples;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.IOException;
import java.net.URI;
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

public class OntologyTripleLoader {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	private static final ArangoDbUtilities arangoDbUtilities = new ArangoDbUtilities();

	private static final ArrayList<String> validVertices = new ArrayList<>(
			Arrays.asList("UBERON", "CL", "GO", "NCBITaxon", "PR", "PATO", "CHEBI", "CLM"));

	private static Map<String, OntologyElementMap> ontologyElementMaps = null;
	private static Map<String, TripleTypeSets> ontologyTripleTypeSets = null;
	private static final Map<String, ArangoVertexCollection> vertexCollections = new HashMap<>();
	private static final Map<String, Map<String, BaseDocument>> vertexDocuments = new HashMap<>();
	private static final Map<String, ArangoEdgeCollection> edgeCollections = new HashMap<>();
	private static final Map<String, Map<String, BaseEdgeDocument>> edgeDocuments = new HashMap<>();

	public static void constructVertices(List<Path> files, ArangoGraph graph) {
		Map<String, Set<String>> vertexKeys = new HashMap<>();

		for (Path file : files) {
			long startTime = System.nanoTime();

			String oboFNm = file.getFileName().toString();
			List<Triple> triples = ontologyTripleTypeSets.get(oboFNm).soFNodeTriples;
			System.out.println("Constructing vertices using " + triples.size() + " triples from " + oboFNm);

			int nVertices = 0;
			for (Triple triple : triples) {

				ArrayList<Node> nodes = new ArrayList<>(Arrays.asList(triple.getSubject(), triple.getObject()));
				for (Node n : nodes) {
					if (!n.isURI())
						continue;
					if (URI.create(n.getURI()).getPath() == null)
						continue;
					if (Paths.get(URI.create(n.getURI()).getPath()).getFileName() == null)
						continue;
					String term = Paths.get(URI.create(n.getURI()).getPath()).getFileName().toString();
					String id = term.contains("_") ? term.split("_")[0] : null;
					if (validVertices.contains(id)) {

						if (!vertexCollections.containsKey(id)) {
							vertexCollections.put(id, arangoDbUtilities.createOrGetVertexCollection(graph, id));
							vertexDocuments.put(id, new HashMap<>());
							vertexKeys.put(id, new HashSet<>());
						}

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

	public static void updateVertices(List<Path> files) {
		for (Path file : files) {
			long startTime = System.nanoTime();

			String oboFNm = file.getFileName().toString();
			List<Triple> triples = ontologyTripleTypeSets.get(oboFNm).soFNodeTriples;
			System.out.println("Updating vertices using " + triples.size() + " triples from " + oboFNm);

			Set<String> updatedVertices = new HashSet<>();
			for (Triple triple : triples) {
				Node o = triple.getObject();
				if (!o.isLiteral()) {
					continue;
				}

				Node s = triple.getSubject();
				if (URI.create(s.getURI()).getPath() == null)
					continue;
				if (Paths.get(URI.create(s.getURI()).getPath()).getFileName() == null)
					continue;
				String term = Paths.get(URI.create(s.getURI()).getPath()).getFileName().toString();
				String id = term.contains("_") ? term.split("_")[0] : null;
				if (validVertices.contains(id)) {

					Node p = triple.getPredicate();
					String attribute = URI.create(p.getURI()).getFragment();
					if (attribute == null) {
						attribute = URI.create(p.getURI()).getPath();
						attribute = attribute.substring(attribute.lastIndexOf('/') + 1);
					}

					String literal = o.getLiteralValue().toString();

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

	public static void constructEdges(List<Path> files, ArangoGraph graph) {
		Map<String, Set<String>> edgeKeys = new HashMap<>();

		for (Path file : files) {
			long startTime = System.nanoTime();

			String oboFNm = file.getFileName().toString();
			List<Triple> triples = ontologyTripleTypeSets.get(oboFNm).soFNodeTriples;
			System.out.println("Constructing edges using " + triples.size() + " triples from " + oboFNm);

			int nEdges = 0;
			for (Triple triple : triples) {

				Node s = triple.getSubject();
				if (URI.create(s.getURI()).getPath() == null)
					continue;
				if (Paths.get(URI.create(s.getURI()).getPath()).getFileName() == null)
					continue;
				String s_term = Paths.get(URI.create(s.getURI()).getPath()).getFileName().toString();
				String s_id = s_term.contains("_") ? s_term.split("_")[0] : null;
				if (!validVertices.contains(s_id))
					continue;

				Node o = triple.getObject();
				if (!o.isURI())
					continue;
				if (URI.create(o.getURI()).getPath() == null)
					continue;
				if (Paths.get(URI.create(o.getURI()).getPath()).getFileName() == null)
					continue;
				String o_term = Paths.get(URI.create(o.getURI()).getPath()).getFileName().toString();
				String o_id = o_term.contains("_") ? o_term.split("_")[0] : null;
				if (!validVertices.contains(o_id))
					continue;

				Node p = triple.getPredicate();
				String label = URI.create(p.getURI()).getFragment();
				if (label == null) {
					label = URI.create(p.getURI()).getPath();
					label = label.substring(label.lastIndexOf('/') + 1);
				}

				String id = s_id + "-" + o_id;
				if (!edgeCollections.containsKey(id)) {
					edgeCollections.put(id, arangoDbUtilities.createOrGetEdgeCollection(graph, s_id, o_id));
					edgeDocuments.put(id, new HashMap<>());
					edgeKeys.put(id, new HashSet<>());
				}

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
