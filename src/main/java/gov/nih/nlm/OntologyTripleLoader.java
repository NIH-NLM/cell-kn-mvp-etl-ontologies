package gov.nih.nlm;

import static gov.nih.nlm.OntologyElementParser.parseOntologyElements;
import static gov.nih.nlm.OntologyTripleParser.parseOntologyTriples;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;
import static org.apache.jena.vocabulary.SchemaDO.collection;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.ArangoVertexCollection;

public class OntologyTripleLoader {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	private static final ArrayList<String> validVertices = new ArrayList<>(
			Arrays.asList("UBERON", "CL", "GO", "NCBITaxon", "PR", "PATO", "CHEBI", "CLM"));

	public static void main(String[] args) throws Exception {

		String directoryPath = oboDir.toString();
		String filePattern = "cl.owl";
		List<Path> files = null;
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
			ontologyElementMaps = parseOntologyElements(files);
			ontologyTripleTypeSets = parseOntologyTriples(files);
		}

		ArangoDbUtilities arangoDbUtilities = new ArangoDbUtilities();

		String databaseName = "Sanger";
		arangoDbUtilities.deleteDatabase(databaseName);
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);

		for (Path file : files) {
			String oboFNm = file.getFileName().toString();

			System.out.println("Loading ontology triples in " + oboFNm);
			List<Triple> triples = ontologyTripleTypeSets.get(oboFNm).soFNodeTriples;

			String graphName = oboFNm.substring(0, oboFNm.lastIndexOf('.')).toUpperCase();
			ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);

			Map<String, ArangoVertexCollection> vertexCollections = new HashMap<>();
			Map<String, Set<String>> collections = new HashMap<>();

			int nTriples = 0;
			for (Triple triple : triples) {
				ArrayList<Node> nodes = new ArrayList<>(Arrays.asList(triple.getSubject(), triple.getObject()));
				for (Node n : nodes) {
					if (!n.isURI()) {
						continue;
					}
					String uri = n.getURI();

					String term = Paths.get(URI.create(uri).getPath()).getFileName().toString();
					if (term.contains("_")) {
						String[] tokens = term.split("_");
						String id = tokens[0];
						if (validVertices.contains(id)) {
							nTriples++;
							if (!vertexCollections.containsKey(id)) {
								ArangoVertexCollection vertexCollection = arangoDbUtilities.createOrGetVertexCollection(graph, id);
								vertexCollections.put(id, vertexCollection);
								collections.put(id, new HashSet<>());
							}
							String key = term;
							if (!collections.get(id).contains(key)) {
								BaseDocument doc = new BaseDocument(key);
								vertexCollections.get(id).insertVertex(doc);
								collections.get(id).add(key);
							}
						}
					}
				}
			}
			System.out.println("Loaded " + nTriples + " triples in " + oboFNm);
		}
	}
}
