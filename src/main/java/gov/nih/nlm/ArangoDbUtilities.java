package gov.nih.nlm;

import com.arangodb.*;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.arangosearch.AnalyzerFeature;
import com.arangodb.entity.arangosearch.CollectionLink;
import com.arangodb.entity.arangosearch.FieldLink;
import com.arangodb.entity.arangosearch.StoreValuesType;
import com.arangodb.entity.arangosearch.analyzer.*;
import com.arangodb.model.EdgeCollectionRemoveOptions;
import com.arangodb.model.VertexCollectionRemoveOptions;
import com.arangodb.model.arangosearch.ArangoSearchCreateOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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
        arangoDB = new ArangoDB.Builder().host(env.get("ARANGO_DB_HOST"), Integer.parseInt(env.get("ARANGO_DB_PORT"))).user(env.get("ARANGO_DB_USER")).password(env.get("ARANGO_DB_PASSWORD")).build();
    }

    /**
     * Build the ArangoDB instance specified in the provided environment.
     *
     * @param env Environment map
     */
    public ArangoDbUtilities(Map<String, String> env) {
        arangoDB = new ArangoDB.Builder().host(env.get("ARANGO_DB_HOST"), Integer.parseInt(env.get("ARANGO_DB_PORT"))).user(env.get("ARANGO_DB_USER")).password(env.get("ARANGO_DB_PASSWORD")).build();
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
        ArangoEdgeCollection edgeCollection = arangoDbUtilities.createOrGetEdgeCollection(graph, vertexOneName, vertexTwoName);
        arangoDbUtilities.deleteEdgeCollection(graph, edgeCollection.name());
        arangoDbUtilities.deleteVertexCollection(graph, vertexTwoName);
        arangoDbUtilities.deleteVertexCollection(graph, vertexOneName);
        arangoDbUtilities.deleteGraph(db, graphName);
        arangoDbUtilities.deleteDatabase(databaseName);
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
        if (!graph.getVertexCollections().contains(vertexName)) {
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
        if (graph.getVertexCollections().contains(vertexName)) {
            System.out.println("Deleting vertex collection: " + vertexName);
            VertexCollectionRemoveOptions options = new VertexCollectionRemoveOptions();
            options.dropCollection(true);
            graph.vertexCollection(vertexName).remove(options);
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
    public ArangoEdgeCollection createOrGetEdgeCollection(ArangoGraph graph, String fromVertexName, String toVertexName) {
        // Create edge collection, if needed
        String collectionName = fromVertexName + "-" + toVertexName;
        if (!graph.getEdgeDefinitions().contains(collectionName)) {
            System.out.println("Creating edge collection: " + collectionName);
            EdgeDefinition edgeDefinition = new EdgeDefinition().collection(collectionName).from(fromVertexName).to(toVertexName);
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
        if (graph.getEdgeDefinitions().contains(edgeName)) {
            System.out.println("Deleting edge collection: " + edgeName);
            EdgeCollectionRemoveOptions options = new EdgeCollectionRemoveOptions();
            options.dropCollections(true);
            graph.edgeCollection(edgeName).remove(options);
        }
    }

    /**
     * Create n-gram and text analyzers in the specified database.
     *
     * @param db Database in which to create the analyzers
     */
    public void createAnalyzers(ArangoDatabase db) {
        // Create n-gram analyzer
        NGramAnalyzer ngramAnalyzer = new NGramAnalyzer();
        ngramAnalyzer.setName("n-gram");
        ngramAnalyzer.setFeatures(Set.of(AnalyzerFeature.frequency, AnalyzerFeature.position, AnalyzerFeature.norm));
        NGramAnalyzerProperties ngramProps = new NGramAnalyzerProperties();
        ngramProps.setMin(3);
        ngramProps.setMax(4);
        ngramProps.setPreserveOriginal(true);
        ngramProps.setStreamType(StreamType.utf8);
        ngramAnalyzer.setProperties(ngramProps);
        System.out.println("Creating analyzer: n-gram");
        db.createSearchAnalyzer(ngramAnalyzer);

        // Create text analyzer without stemming
        TextAnalyzer textAnalyzer = new TextAnalyzer();
        textAnalyzer.setName("text_en_no_stem");
        textAnalyzer.setFeatures(Set.of(AnalyzerFeature.frequency, AnalyzerFeature.position, AnalyzerFeature.norm));
        TextAnalyzerProperties textProps = new TextAnalyzerProperties();
        textProps.setLocale("en");
        textProps.setAnalyzerCase(SearchAnalyzerCase.lower);
        textProps.setAccent(false);
        textProps.setStemming(false);
        EdgeNgram edgeNgram = new EdgeNgram();
        edgeNgram.setMin(3);
        edgeNgram.setMax(12);
        edgeNgram.setPreserveOriginal(true);
        textProps.setEdgeNgram(edgeNgram);
        textAnalyzer.setProperties(textProps);
        System.out.println("Creating analyzer: text_en_no_stem");
        db.createSearchAnalyzer(textAnalyzer);
    }

    /**
     * Delete n-gram and text analyzers in the specified database.
     *
     * @param db Database in which to delete the analyzers
     */
    public void deleteAnalyzers(ArangoDatabase db) {
        try {
            System.out.println("Deleting analyzer: n-gram");
            db.deleteSearchAnalyzer("n-gram");
        } catch (ArangoDBException e) {
            System.out.println("Analyzer n-gram not found, skipping");
        }
        try {
            System.out.println("Deleting analyzer: text_en_no_stem");
            db.deleteSearchAnalyzer("text_en_no_stem");
        } catch (ArangoDBException e) {
            System.out.println("Analyzer text_en_no_stem not found, skipping");
        }
    }

    /**
     * Create an arangosearch view named "indexed" in the specified database,
     * using the collection maps JSON file to configure per-collection links.
     *
     * @param db                 Database in which to create the view
     * @param collectionMapsPath Path to the JSON file containing collection maps
     * @throws IOException if the JSON file cannot be read
     */
    public void createView(ArangoDatabase db, Path collectionMapsPath) throws IOException {
        // Read collection maps JSON
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(Files.readString(collectionMapsPath));
        JsonNode maps = root.get("maps");

        // Build view options with per-collection links
        ArangoSearchCreateOptions options = new ArangoSearchCreateOptions().commitIntervalMsec(1000L).consolidationIntervalMsec(1000L).cleanupIntervalStep(2L);

        for (JsonNode collectionMap : maps) {
            String vertexName = collectionMap.get(0).asText();
            // Skip non-vertex entries
            if (vertexName.equals("edges") || vertexName.equals("TEST_DOCUMENT_COLLECTION") || vertexName.equals("TEST_EDGE_COLLECTION")) {
                continue;
            }

            // Collect field names
            JsonNode individualFields = collectionMap.get(1).get("individual_fields");
            List<FieldLink> fieldLinks = new ArrayList<>();
            for (JsonNode field : individualFields) {
                String fieldName = field.get("field_to_display").asText();
                fieldLinks.add(FieldLink.on(fieldName).analyzers("text_en", "text_en_no_stem", "n-gram", "identity"));
            }

            // Create collection link
            CollectionLink link = CollectionLink.on(vertexName).analyzers("identity").includeAllFields(false).storeValues(StoreValuesType.NONE).trackListPositions(false).fields(fieldLinks.toArray(new FieldLink[0]));

            options.link(link);
        }

        System.out.println("Creating view: indexed");
        db.createArangoSearch("indexed", options);
    }

    /**
     * Delete the arangosearch view named "indexed" in the specified database.
     *
     * @param db Database in which to delete the view
     */
    public void deleteView(ArangoDatabase db) {
        try {
            System.out.println("Deleting view: indexed");
            db.arangoSearch("indexed").drop();
        } catch (ArangoDBException e) {
            System.out.println("View indexed not found, skipping");
        }
    }
}
