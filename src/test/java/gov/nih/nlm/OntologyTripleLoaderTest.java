package gov.nih.nlm;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoEdgeCollection;
import com.arangodb.ArangoGraph;
import com.arangodb.ArangoVertexCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

class OntologyTripleLoaderTest {

	static String arangoDbHost = "localhost";
	static String arangoDbPort = "8529";
	static String arangoDbUser = "root";
	static String arangoDbPassword = System.getenv("ARANGO_DB_PASSWORD");
	static ArangoDbUtilities arangoDbUtilities;

	static Path arangoDbHome = Paths.get("").toAbsolutePath().resolve("src/test/java/data/arangodb");
	static File shellDir = Paths.get("").toAbsolutePath().resolve("src/main/shell").toFile();
	static String[] stopArangoDB = new String[] { "./stop-arangodb.sh" };
	static String[] startArangoDB = new String[] { "./start-arangodb.sh" };
	static String[] envp = new String[] { "ARANGO_DB_HOME=" + arangoDbHome, "ARANGO_DB_PASSWORD=" + arangoDbPassword };

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("src/test/data/obo");

	@BeforeEach
	void setUp() {
		try {
			// Stop any ArangoDB instance
			if (Runtime.getRuntime().exec(stopArangoDB, envp, shellDir).waitFor() != 0) {
				throw new RuntimeException("Could not stop ArangoDB");
			}
			// Start an ArangoDB instance using the test data directory
			if (Runtime.getRuntime().exec(startArangoDB, envp, shellDir).waitFor() != 0) {
				throw new RuntimeException("Could not start ArangoDB");
			}
		} catch (java.io.IOException | InterruptedException e) {
			e.printStackTrace();
		}
		// Connect to ArangoDB
		Map<String, String> env = new HashMap<>();
		env.put("ARANGO_DB_HOST", arangoDbHost);
		env.put("ARANGO_DB_PORT", arangoDbPort);
		env.put("ARANGO_DB_USER", arangoDbUser);
		env.put("ARANGO_DB_PASSWORD", arangoDbPassword);
		arangoDbUtilities = new ArangoDbUtilities(env);
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@AfterEach
	void tearDown() {
		try {
			// Stop the ArangoDB instance using the test data directory
			if (Runtime.getRuntime().exec(stopArangoDB, envp, shellDir).waitFor() != 0) {
				throw new RuntimeException("Could not stop ArangoDB");
			}
		} catch (java.io.IOException | InterruptedException e) {
			e.printStackTrace();
		}
		// Remove ArangoDB test data directory
		try {
			FileUtils.deleteDirectory(arangoDbHome.toFile());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	@Disabled("To be completed")
	void createVTuple() {
	}

	@Test
	@Disabled("To be completed")
	void parsePredicate() {
	}

	@Test
	@Disabled("To be completed")
	void constructVertices() {
	}

	@Test
	@Disabled("To be completed")
	void updateVertices() {
	}

	@Test
	@Disabled("To be completed")
	void insertVertices() {
	}

	@Test
	@Disabled("To be completed")
	void constructEdges() {
	}

	@Test
	@Disabled("To be completed")
	void insertEdges() {
	}

	/*
	 * Compare actual and expected macrophage vertex and edges, obtaining expected
	 * values by manual inspection of the macrophage OWL file.
	 */
	@Test
	void main() {

		// Parse macrophage OWL file and load the result into ArangoDB
		try {
			String[] args = new String[] { oboDir.toString(), "cl-test", "test" };
			OntologyTripleLoader.main(args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// Connect to ArangoDB
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase("cl-test");
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, "test");

		// Get the macrophage vertex
		String vertexName = "CL";
		ArangoVertexCollection vertexCollection = graph.vertexCollection(vertexName);
		String number = "0000235";
		assertTrue(graph.db().collection(vertexName).documentExists(number));
		BaseDocument vertexDoc = vertexCollection.getVertex(number, BaseDocument.class);

		// Assert vertex attributes have expected values
		assertArrayEquals(((ArrayList<String>) vertexDoc.getAttribute("hasDbXref")).toArray(),
				new ArrayList<>(
						Arrays.asList("ZFA:0009141", "PMID:16213494", "GO_REF:0000031", "PMID:1919437", "GOC:add",
								"CALOHA:TS-0587", "GOC:tfm", "FMA:83585", "MESH:D008264", "BTO:0000801", "FMA:63261"))
						.toArray());
		assertEquals(vertexDoc.getAttribute("hasExactSynonym"), "histiocyte");
		assertEquals(vertexDoc.getAttribute("mapping_justification"),
				"https://w3id.org/semapv/vocab/UnspecifiedMatching");
		assertEquals(vertexDoc.getAttribute("definition"),
				"A mononuclear phagocyte present in variety of tissues, typically differentiated from monocytes, capable of phagocytosing a variety of extracellular particulate material, including immune complexes, microorganisms, and dead cells.");
		assertEquals(vertexDoc.getAttribute("comment"),
				"Morphology: Diameter 30_M-80 _M, abundant cytoplasm, low N/C ratio, eccentric nucleus. Irregular shape with pseudopods, highly adhesive. Contain vacuoles and phagosomes, may contain azurophilic granules; markers: Mouse & Human: CD68, in most cases CD11b. Mouse: in most cases F4/80+; role or process: immune, antigen presentation, & tissue remodelling; lineage: hematopoietic, myeloid.");
		assertEquals(vertexDoc.getAttribute("id"), "CL:0000235");
		assertEquals(vertexDoc.getAttribute("label"), "macrophage");

		// Get macrophage edges to CL terms, then assert equal labels
		String edgeName = "CL-CL";
		ArangoEdgeCollection edgeCollection = graph.edgeCollection(edgeName);
		String[] keys = new String[] { "0000235-0000113", "0000235-0000145", "0000235-0000766" };
		for (String key : keys) {
			assertTrue(graph.db().collection(edgeName).documentExists(key));
			BaseDocument edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
			assertEquals("subClassOf", edgeDoc.getAttribute("label"));
		}
		String key = "0000235-0000576";
		assertTrue(graph.db().collection(edgeName).documentExists(key));
		BaseDocument edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
		assertEquals("develops from", edgeDoc.getAttribute("label"));

		// Get macrophage edges to GO terms, then assert equal labels
		edgeName = "CL-GO";
		edgeCollection = graph.edgeCollection(edgeName);
		key = "0000235-0031268";
		assertTrue(graph.db().collection(edgeName).documentExists(key));
		edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
		assertEquals("capable of", edgeDoc.getAttribute("label"));

		// Get macrophage edges to NCBITaxon terms, then assert equal labels
		edgeName = "CL-NCBITaxon";
		edgeCollection = graph.edgeCollection(edgeName);
		key = "0000235-9606";
		assertTrue(graph.db().collection(edgeName).documentExists(key));
		edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
		assertEquals("present in taxon", edgeDoc.getAttribute("label"));
	}
}
