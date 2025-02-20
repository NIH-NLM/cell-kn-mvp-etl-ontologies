package gov.nih.nlm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.arangodb.*;
import com.arangodb.entity.EdgeDefinition;

/**
 * Provides utilities for managing named ArangoDB databases, graphs, vertex
 * collections, and edge collections.
 *
 * @author Raymond LeClair
 */
public class ArangoDbUtilities {

	/**
	 * An ArangoDB instance
	 */
	public final ArangoDB arangoDB;

	/**
	 * Build the ArangoDB instance specified in the system environment.
	 */
	public ArangoDbUtilities() {
		Map<String, String> env = System.getenv();
		arangoDB = new ArangoDB.Builder().host(env.get("ARANGO_DB_HOST"), Integer.parseInt(env.get("ARANGO_DB_PORT")))
				.user(env.get("ARANGO_DB_USER")).password(env.get("ARANGO_DB_PASSWORD")).build();
	}

	/**
	 * Create or get a named database.
	 *
	 * @param databaseName Name of the database to create or get
	 * @return Named database
	 */
	public ArangoDatabase createOrGetDatabase(String databaseName) {
		// Create the database, if needed
		if (!arangoDB.db(databaseName).exists()) {
			System.out.println("Creating database: " + databaseName);
			if (!arangoDB.createDatabase(databaseName)) {
				throw new RuntimeException("Could not create database: " + databaseName);
			}
		}
		// Get the database
		System.out.println("Getting database: " + databaseName);
		return arangoDB.db(databaseName);
	}

	/**
	 * Delete a named database.
	 *
	 * @param databaseName Name of the database to delete
	 */
	public void deleteDatabase(String databaseName) {
		// Delete the database, if needed
		if (arangoDB.db(databaseName).exists()) {
			System.out.println("Deleting database: " + databaseName);
			if (!arangoDB.db(databaseName).drop()) {
				throw new RuntimeException("Could not delete database: " + databaseName);
			}
		}
	}

	/**
	 * Create or get a named graph.
	 *
	 * @param db        Database in which to create or get the graph
	 * @param graphName Name of the graph to create or get
	 * @return Named graph
	 */
	public ArangoGraph createOrGetGraph(ArangoDatabase db, String graphName) {
		// Create the graph, if needed
		if (!db.graph(graphName).exists()) {
			System.out.println("Creating graph: " + graphName);
			Collection<EdgeDefinition> edgeDefinitions = new ArrayList<>();
			db.createGraph(graphName, edgeDefinitions);
		}
		// Get the graph
		System.out.println("Getting graph: " + graphName);
		return db.graph(graphName);
	}

	/**
	 * Delete a named graph.
	 *
	 * @param db        Database in which to delete the graph
	 * @param graphName Name of the graph to delete
	 */
	public void deleteGraph(ArangoDatabase db, String graphName) {
		// Delete the graph, if needed
		if (db.graph(graphName).exists()) {
			System.out.println("Deleting graph: " + graphName);
			db.graph(graphName).drop();
		}
	}

	/**
	 * Create or get a named vertex collection.
	 *
	 * @param graph      Graph in which to create or get the vertex collection
	 * @param vertexName Name of the vertex collection to create or get
	 * @return Named vertex collection
	 */
	public ArangoVertexCollection createOrGetVertexCollection(ArangoGraph graph, String vertexName) {
		// Create the vertex collection, if needed
		if (!graph.db().collection(vertexName).exists()) {
			System.out.println("Creating vertex collection: " + vertexName);
			graph.addVertexCollection(vertexName);
		}
		// Get the vertex collection
		System.out.println("Getting vertex collection: " + vertexName);
		return graph.vertexCollection(vertexName);
	}

	/**
	 * Delete a named vertex collection.
	 *
	 * @param graph      Graph in which to delete the vertex collection
	 * @param vertexName Name of the vertex collection to delete
	 */
	public void deleteVertexCollection(ArangoGraph graph, String vertexName) {
		// Delete the vertex collection, if needed
		if (graph.db().collection(vertexName).exists()) {
			System.out.println("Deleting vertex collection: " + vertexName);
			graph.vertexCollection(vertexName).drop();
		}
	}

	/**
	 * Create, or get a named edge collection from and to the named vertices.
	 *
	 * @param graph          Graph in which to create, or get the edge collection
	 * @param fromVertexName Name of the vertex collection from which the edge
	 *                       originates
	 * @param toVertexName   Name of the vertex collection to which the edge
	 *                       terminates
	 * @return Named edge collection
	 */
	public ArangoEdgeCollection createOrGetEdgeCollection(ArangoGraph graph, String fromVertexName,
			String toVertexName) {
		// Create edge collection, if needed
		String collectionName = fromVertexName + "-" + toVertexName;
		if (!graph.db().collection(collectionName).exists()) {
			System.out.println("Creating edge collection: " + collectionName);
			EdgeDefinition edgeDefinition = new EdgeDefinition().collection(collectionName).from(fromVertexName)
					.to(toVertexName);
			graph.addEdgeDefinition(edgeDefinition);
		}
		// Get the edge collection
		System.out.println("Getting edge collection: " + collectionName);
		return graph.edgeCollection(collectionName);
	}

	/**
	 * Delete a named edge collection.
	 *
	 * @param graph    Graph in which to create, or get the edge collection
	 * @param edgeName Name of the edge collection to delete
	 */
	public void deleteEdgeCollection(ArangoGraph graph, String edgeName) {
		// Delete the edge collection, if needed
		if (graph.db().collection(edgeName).exists()) {
			System.out.println("Deleting edge collection: " + edgeName);
			graph.edgeCollection(edgeName).drop();
		}
	}

	/**
	 * Exercise the utilities.
	 *
	 * @param args (None expected)
	 */
	public static void main(String[] args) {
		ArangoDbUtilities arangoDbUtilities = new ArangoDbUtilities();
		String databaseName = "myDb";
		ArangoDatabase db = arangoDbUtilities.createOrGetDatabase(databaseName);
		String graphName = "myGraph";
		ArangoGraph graph = arangoDbUtilities.createOrGetGraph(db, graphName);
		String vertexOneName = "myVertexOne";
		arangoDbUtilities.createOrGetVertexCollection(graph, vertexOneName);
		String vertexTwoName = "myVertexTwo";
		arangoDbUtilities.createOrGetVertexCollection(graph, vertexTwoName);
		ArangoEdgeCollection edgeCollection = arangoDbUtilities.createOrGetEdgeCollection(graph, vertexOneName,
				vertexTwoName);
		arangoDbUtilities.deleteEdgeCollection(graph, edgeCollection.name());
		arangoDbUtilities.deleteVertexCollection(graph, vertexTwoName);
		arangoDbUtilities.deleteVertexCollection(graph, vertexOneName);
		arangoDbUtilities.deleteGraph(db, graphName);
		arangoDbUtilities.deleteDatabase(databaseName);
	}
}
