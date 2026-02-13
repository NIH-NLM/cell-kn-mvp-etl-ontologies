package gov.nih.nlm;

import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoEdgeCollection;
import com.arangodb.ArangoGraph;
import com.arangodb.ArangoVertexCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class OntologyGraphBuilderTest {

    // Assign location of ontology files
    private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
    private static final Path oboDir = usrDir.resolve("src/test/data/obo");
    static String arangoDbHost = "localhost";
    static String arangoDbPort = "8529";
    static String arangoDbUser = "root";
    static String arangoDbPassword = System.getenv("ARANGO_DB_PASSWORD");
    static ArangoDbUtilities arangoDbUtilities;
    static Path arangoDbHome = Paths.get("").toAbsolutePath().resolve("src/test/java/data/arangodb");
    static File shellDir = Paths.get("").toAbsolutePath().resolve("src/main/shell").toFile();
    static String[] stopArangoDB = new String[]{"./stop-arangodb.sh"};
    static String[] startArangoDB = new String[]{"./start-arangodb.sh"};
    static String[] envp = new String[]{"ARANGO_DB_HOME=" + arangoDbHome, "ARANGO_DB_PASSWORD=" + arangoDbPassword};

    // --- createVTuple tests (no ArangoDB needed) ---

    @Test
    void createVTuple_validCLTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/CL_0000235");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("CL_0000235", vtuple.term());
        assertEquals("CL", vtuple.id());
        assertEquals("0000235", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validGOTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/GO_0031268");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("GO_0031268", vtuple.term());
        assertEquals("GO", vtuple.id());
        assertEquals("0031268", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validUBERONTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/UBERON_0000061");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("UBERON_0000061", vtuple.term());
        assertEquals("UBERON", vtuple.id());
        assertEquals("0000061", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_invalidPrefix() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/BFO_0000002");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("BFO_0000002", vtuple.term());
        assertEquals("BFO", vtuple.id());
        assertEquals("0000002", vtuple.number());
        assertFalse(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_nonUriNode() {
        var node = NodeFactory.createLiteralString("not a URI");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertNull(vtuple.term());
        assertNull(vtuple.id());
        assertNull(vtuple.number());
        assertFalse(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_uriWithFragment() {
        var node = NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        // "subClassOf" has no underscore or colon separator, so tokens will be null
        assertNull(vtuple.term());
        assertFalse(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_ncbiTaxon() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/NCBITaxon_9606");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("NCBITaxon_9606", vtuple.term());
        assertEquals("NCBITaxon", vtuple.id());
        assertEquals("9606", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validHPTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/HP_0000001");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("HP_0000001", vtuple.term());
        assertEquals("HP", vtuple.id());
        assertEquals("0000001", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    @Test
    void createVTuple_validMONDOTerm() {
        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/MONDO_0000001");
        OntologyGraphBuilder.VTuple vtuple = OntologyGraphBuilder.createVTuple(node);

        assertEquals("MONDO_0000001", vtuple.term());
        assertEquals("MONDO", vtuple.id());
        assertEquals("0000001", vtuple.number());
        assertTrue(vtuple.isValidVertex());
    }

    // --- parsePredicate tests (no ArangoDB needed) ---

    @Test
    void parsePredicate_fragmentUri() {
        // A URI with a fragment should return the fragment
        var node = NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf");
        Map<String, OntologyElementMap> maps = new HashMap<>();
        maps.put("ro", new OntologyElementMap());

        String label = OntologyGraphBuilder.parsePredicate(maps, node);
        assertEquals("subClassOf", label);
    }

    @Test
    void parsePredicate_oboTermWithDevelopsFrom() {
        // A URI without fragment, where the term is in the ro map
        List<Path> roFile = List.of(oboDir.resolve("ro.owl"));
        Map<String, OntologyElementMap> maps = OntologyElementParser.parseOntologyElements(roFile);

        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/RO_0002202");
        String label = OntologyGraphBuilder.parsePredicate(maps, node);
        assertEquals("develops from", label);
    }

    @Test
    void parsePredicate_oboTermWithCapableOf() {
        List<Path> roFile = List.of(oboDir.resolve("ro.owl"));
        Map<String, OntologyElementMap> maps = OntologyElementParser.parseOntologyElements(roFile);

        var node = NodeFactory.createURI("http://purl.obolibrary.org/obo/RO_0002215");
        String label = OntologyGraphBuilder.parsePredicate(maps, node);
        assertEquals("capable of", label);
    }

    @Test
    void parsePredicate_nonUriThrows() {
        var node = NodeFactory.createLiteralString("not a URI");
        Map<String, OntologyElementMap> maps = new HashMap<>();
        maps.put("ro", new OntologyElementMap());

        assertThrows(RuntimeException.class, () -> OntologyGraphBuilder.parsePredicate(maps, node));
    }

    // --- normalizeEdgeSource tests ---

    @Test
    void normalizeEdgeSource_mondoSimple() {
        assertEquals("MONDO", OntologyGraphBuilder.normalizeEdgeSource("mondo-simple"));
    }

    @Test
    void normalizeEdgeSource_taxslim() {
        assertEquals("NCBITAXON", OntologyGraphBuilder.normalizeEdgeSource("taxslim"));
    }

    @Test
    void normalizeEdgeSource_goPlus() {
        assertEquals("GO", OntologyGraphBuilder.normalizeEdgeSource("go-plus"));
    }

    @Test
    void normalizeEdgeSource_uberonBase() {
        assertEquals("UBERON", OntologyGraphBuilder.normalizeEdgeSource("uberon-base"));
    }

    @Test
    void normalizeEdgeSource_defaultUpperCase() {
        assertEquals("CL", OntologyGraphBuilder.normalizeEdgeSource("cl"));
        assertEquals("HP", OntologyGraphBuilder.normalizeEdgeSource("hp"));
        assertEquals("PATO", OntologyGraphBuilder.normalizeEdgeSource("pato"));
    }

    // --- normalizeEdgeLabel tests ---

    @Test
    void normalizeEdgeLabel_subClassOf() {
        assertEquals("SUB_CLASS_OF", OntologyGraphBuilder.normalizeEdgeLabel("subClassOf"));
    }

    @Test
    void normalizeEdgeLabel_disjointWith() {
        assertEquals("DISJOINT_WITH", OntologyGraphBuilder.normalizeEdgeLabel("disjointWith"));
    }

    @Test
    void normalizeEdgeLabel_crossSpeciesExactMatch() {
        assertEquals("CROSS_SPECIES_EXACT_MATCH", OntologyGraphBuilder.normalizeEdgeLabel("crossSpeciesExactMatch"));
    }

    @Test
    void normalizeEdgeLabel_exactMatch() {
        assertEquals("EXACT_MATCH", OntologyGraphBuilder.normalizeEdgeLabel("exactMatch"));
    }

    @Test
    void normalizeEdgeLabel_equivalentClass() {
        assertEquals("EQUIVALENT_CLASS", OntologyGraphBuilder.normalizeEdgeLabel("equivalentClass"));
    }

    @Test
    void normalizeEdgeLabel_seeAlso() {
        assertEquals("SEE_ALSO", OntologyGraphBuilder.normalizeEdgeLabel("seeAlso"));
    }

    @Test
    void normalizeEdgeLabel_defaultWithSpaces() {
        assertEquals("DEVELOPS_FROM", OntologyGraphBuilder.normalizeEdgeLabel("develops from"));
        assertEquals("CAPABLE_OF", OntologyGraphBuilder.normalizeEdgeLabel("capable of"));
        assertEquals("PART_OF", OntologyGraphBuilder.normalizeEdgeLabel("part of"));
    }

    @Test
    void normalizeEdgeLabel_defaultUpperCase() {
        assertEquals("LABEL", OntologyGraphBuilder.normalizeEdgeLabel("label"));
    }

    // --- getDocumentCollectionName tests ---

    @Test
    void getDocumentCollectionName_vertexId() {
        assertEquals("CL", OntologyGraphBuilder.getDocumentCollectionName("CL/0000235"));
    }

    @Test
    void getDocumentCollectionName_edgeId() {
        assertEquals("CL-GO", OntologyGraphBuilder.getDocumentCollectionName("CL-GO/0000235-0031268"));
    }

    @Test
    void getDocumentCollectionName_nullInput() {
        assertNull(OntologyGraphBuilder.getDocumentCollectionName(null));
    }

    @Test
    void getDocumentCollectionName_noSlash() {
        assertNull(OntologyGraphBuilder.getDocumentCollectionName("CL0000235"));
    }

    // --- getDocumentKey tests ---

    @Test
    void getDocumentKey_vertexId() {
        assertEquals("0000235", OntologyGraphBuilder.getDocumentKey("CL/0000235"));
    }

    @Test
    void getDocumentKey_edgeId() {
        assertEquals("0000235-0031268", OntologyGraphBuilder.getDocumentKey("CL-GO/0000235-0031268"));
    }

    @Test
    void getDocumentKey_nullInput() {
        assertNull(OntologyGraphBuilder.getDocumentKey(null));
    }

    @Test
    void getDocumentKey_noSlash() {
        assertNull(OntologyGraphBuilder.getDocumentKey("CL0000235"));
    }

    // --- Integration test (requires ArangoDB) ---

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    /*
     * Compare actual and expected macrophage vertex and edges, obtaining expected
     * values by manual inspection of the macrophage OWL file.
     *
     * This test requires a running ArangoDB instance.
     */
    @Tag("integration")
    @Test
    void main() {
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

        try {
            // Parse macrophage OWL file and load the result into ArangoDB
            try {
                String[] args = new String[]{oboDir.toString(), "cl-test", "test"};
                OntologyGraphBuilder.main(args);
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
            assertArrayEquals(((ArrayList<String>) vertexDoc.getAttribute("hasDbXref")).toArray(), new ArrayList<>(Arrays.asList("ZFA:0009141", "CALOHA:TS-0587", "MESH:D008264", "FMA:83585", "BTO:0000801", "FMA:63261")).toArray());
            assertEquals(vertexDoc.getAttribute("hasExactSynonym"), "histiocyte");
            assertEquals(vertexDoc.getAttribute("comment"), "Morphology: Diameter 30_M-80 _M, abundant cytoplasm, low N/C ratio, eccentric nucleus. Irregular shape with pseudopods, highly adhesive. Contain vacuoles and phagosomes, may contain azurophilic granules; markers: Mouse & Human: CD68, in most cases CD11b. Mouse: in most cases F4/80+; role or process: immune, antigen presentation, & tissue remodelling; lineage: hematopoietic, myeloid.");
            assertEquals(vertexDoc.getAttribute("definition"), "A mononuclear phagocyte present in variety of tissues, typically differentiated from monocytes, capable of phagocytosing a variety of extracellular particulate material, including immune complexes, microorganisms, and dead cells.");
            assertEquals(vertexDoc.getAttribute("label"), "macrophage");
            assertEquals(vertexDoc.getAttribute("id"), "CL:0000235");

            // Get macrophage edges to CL terms, then assert equal labels
            String edgeName = "CL-CL";
            ArangoEdgeCollection edgeCollection = graph.edgeCollection(edgeName);
            String[] keys = new String[]{"0000235-0000113", "0000235-0000145", "0000235-0000766"};
            for (String key : keys) {
                assertTrue(graph.db().collection(edgeName).documentExists(key));
                BaseDocument edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
                assertEquals("SUB_CLASS_OF", ((ArrayList<String>) edgeDoc.getAttribute("Label")).get(0));
            }
            String key = "0000235-0000576";
            assertTrue(graph.db().collection(edgeName).documentExists(key));
            BaseDocument edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
            assertEquals("DEVELOPS_FROM", ((ArrayList<String>) edgeDoc.getAttribute("Label")).get(0));

            // Get macrophage edges to GO terms, then assert equal labels
            edgeName = "CL-GO";
            edgeCollection = graph.edgeCollection(edgeName);
            key = "0000235-0031268";
            assertTrue(graph.db().collection(edgeName).documentExists(key));
            edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
            assertEquals("CAPABLE_OF", ((ArrayList<String>) edgeDoc.getAttribute("Label")).get(0));

            // Get macrophage edges to NCBITaxon terms, then assert equal labels
            edgeName = "CL-NCBITaxon";
            edgeCollection = graph.edgeCollection(edgeName);
            key = "0000235-9606";
            assertTrue(graph.db().collection(edgeName).documentExists(key));
            edgeDoc = edgeCollection.getEdge(key, BaseEdgeDocument.class);
            assertEquals("PRESENT_IN_TAXON", ((ArrayList<String>) edgeDoc.getAttribute("Label")).get(0));

        } finally {
            try {
                // Stop the ArangoDB instance
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
    }
}
