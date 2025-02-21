package gov.nih.nlm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;

class ArangoDbUtilitiesTest {

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

	static String databaseName = "database";
	static String graphName = "graph";
	static String fromVertexName = "from_vertex";
	static String toVertexName = "to_vertex";
	static String edgeName = fromVertexName + "-" + toVertexName;

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
	void createOrGetDatabase() {
		assertFalse(arangoDbUtilities.arangoDB.db(databaseName).exists());
		arangoDbUtilities.createOrGetDatabase(databaseName);
		assertTrue(arangoDbUtilities.arangoDB.db(databaseName).exists());
	}

	@Test
	void deleteDatabase() {
		arangoDbUtilities.createOrGetDatabase(databaseName);
		assertTrue(arangoDbUtilities.arangoDB.db(databaseName).exists());
		arangoDbUtilities.deleteDatabase(databaseName);
		assertFalse(arangoDbUtilities.arangoDB.db(databaseName).exists());
	}

	@Test
	void createOrGetGraph() {
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		assertFalse(db.graph(graphName).exists());
		arangoDbUtilities.createOrGetGraph(db, graphName);
		assertTrue(db.graph(graphName).exists());
	}

	@Test
	void deleteGraph() {
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		arangoDbUtilities.createOrGetGraph(db, graphName);
		assertTrue(db.graph(graphName).exists());
		arangoDbUtilities.deleteGraph(db, graphName);
		assertFalse(db.graph(graphName).exists());
	}

	@Test
	void createOrGetVertexCollection() {
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);
		assertFalse(graph.db().collection(fromVertexName).exists());
		arangoDbUtilities.createOrGetVertexCollection(graph, fromVertexName);
		assertTrue(graph.db().collection(fromVertexName).exists());
	}

	@Test
	void deleteVertexCollection() {
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);
		arangoDbUtilities.createOrGetVertexCollection(graph, fromVertexName);
		assertTrue(graph.db().collection(fromVertexName).exists());
		arangoDbUtilities.deleteVertexCollection(graph, fromVertexName);
		assertFalse(graph.db().collection(fromVertexName).exists());
	}

	@Test
	void createOrGetEdgeCollection() {
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);
		arangoDbUtilities.createOrGetVertexCollection(graph, fromVertexName);
		arangoDbUtilities.createOrGetVertexCollection(graph, toVertexName);
		assertFalse(graph.db().collection(edgeName).exists());
		arangoDbUtilities.createOrGetEdgeCollection(graph, fromVertexName, toVertexName);
		assertTrue(graph.db().collection(edgeName).exists());
	}

	@Test
	void deleteEdgeCollection() {
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);
		arangoDbUtilities.createOrGetVertexCollection(graph, fromVertexName);
		arangoDbUtilities.createOrGetVertexCollection(graph, toVertexName);
		arangoDbUtilities.createOrGetEdgeCollection(graph, fromVertexName, toVertexName);
		assertTrue(graph.db().collection(edgeName).exists());
		arangoDbUtilities.deleteEdgeCollection(graph, edgeName);
		assertFalse(graph.db().collection(edgeName).exists());
	}
}