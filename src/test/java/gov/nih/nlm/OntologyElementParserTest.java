package gov.nih.nlm;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OntologyElementParserTest {

    private static final Path testOboDir = Paths.get(System.getProperty("user.dir")).resolve("src/test/data/obo");

    // --- parseXmlFile tests ---

    @Test
    void parseXmlFile_macrophage() {
        File xmlFile = testOboDir.resolve("macrophage.owl").toFile();
        Document doc = OntologyElementParser.parseXmlFile(xmlFile);
        assertNotNull(doc);
        assertEquals("rdf:RDF", doc.getDocumentElement().getTagName());
    }

    @Test
    void parseXmlFile_ro() {
        File xmlFile = testOboDir.resolve("ro.owl").toFile();
        Document doc = OntologyElementParser.parseXmlFile(xmlFile);
        assertNotNull(doc);
        assertEquals("rdf:RDF", doc.getDocumentElement().getTagName());
    }

    @Test
    void parseXmlFile_invalidFile() {
        File xmlFile = new File("/nonexistent/file.owl");
        assertThrows(RuntimeException.class, () -> OntologyElementParser.parseXmlFile(xmlFile));
    }

    // --- createURI tests ---

    @Test
    void createURI_normalUri() {
        URI uri = OntologyElementParser.createURI("http://purl.obolibrary.org/obo/CL_0000235");
        assertEquals(URI.create("http://purl.obolibrary.org/obo/CL_0000235"), uri);
    }

    @Test
    void createURI_pclCsTerm() {
        URI uri = OntologyElementParser.createURI("http://purl.obolibrary.org/obo/pcl/CS12345");
        assertEquals(URI.create("http://purl.obolibrary.org/obo/PCLCS_12345"), uri);
    }

    @Test
    void createURI_ensemblTerm() {
        URI uri = OntologyElementParser.createURI("http://purl.obolibrary.org/obo/ensembl/ENSG00000123456");
        assertEquals(URI.create("http://purl.obolibrary.org/obo/ENSG_00000123456"), uri);
    }

    @Test
    void createURI_noSpecialHandling() {
        URI uri = OntologyElementParser.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf");
        assertEquals(URI.create("http://www.w3.org/2000/01/rdf-schema#subClassOf"), uri);
    }

    // --- parseOntologyNode tests ---

    @Test
    void parseOntologyNode_macrophage() {
        File xmlFile = testOboDir.resolve("macrophage.owl").toFile();
        Document doc = OntologyElementParser.parseXmlFile(xmlFile);
        OntologyElementMap map = new OntologyElementMap();
        OntologyElementParser.parseOntologyNode(doc.getDocumentElement(), map);

        // Should contain CL ids from macrophage.owl
        assertTrue(map.ids.contains("CL"));

        // Should contain the macrophage term with its label
        assertTrue(map.terms.containsKey("CL_0000235"));
        assertEquals("macrophage", map.terms.get("CL_0000235").label);
        assertEquals(URI.create("http://purl.obolibrary.org/obo/CL_0000235"), map.terms.get("CL_0000235").purl);

        // Should contain other CL terms
        assertTrue(map.terms.containsKey("CL_0000000"));
        assertEquals("cell", map.terms.get("CL_0000000").label);

        assertTrue(map.terms.containsKey("CL_0000576"));
        assertEquals("monocyte", map.terms.get("CL_0000576").label);
    }

    @Test
    void parseOntologyNode_ro() {
        File xmlFile = testOboDir.resolve("ro.owl").toFile();
        Document doc = OntologyElementParser.parseXmlFile(xmlFile);
        OntologyElementMap map = new OntologyElementMap();
        OntologyElementParser.parseOntologyNode(doc.getDocumentElement(), map);

        // Should contain RO and IAO ids
        assertTrue(map.ids.contains("RO"));
        assertTrue(map.ids.contains("IAO"));

        // The "develops from" term (RO_0002202) should be present
        assertTrue(map.terms.containsKey("RO_0002202"));
        assertEquals("develops from", map.terms.get("RO_0002202").label);

        // The "capable of" term (RO_0002215) should be present
        assertTrue(map.terms.containsKey("RO_0002215"));
        assertEquals("capable of", map.terms.get("RO_0002215").label);
    }

    @Test
    void parseOntologyNode_filtersOutValidPrefix() {
        File xmlFile = testOboDir.resolve("macrophage.owl").toFile();
        Document doc = OntologyElementParser.parseXmlFile(xmlFile);
        OntologyElementMap map = new OntologyElementMap();
        OntologyElementParser.parseOntologyNode(doc.getDocumentElement(), map);

        // Should not contain "valid" as an id
        assertFalse(map.ids.contains("valid"));
    }

    // --- parseOntologyElements tests ---

    @Test
    void parseOntologyElements_macrophage() {
        List<Path> files = List.of(testOboDir.resolve("macrophage.owl"));
        Map<String, OntologyElementMap> maps = OntologyElementParser.parseOntologyElements(files);

        assertEquals(1, maps.size());
        assertTrue(maps.containsKey("macrophage"));

        OntologyElementMap map = maps.get("macrophage");

        // macrophage.owl uses owl:Ontology with rdf:about for PURL
        assertEquals(URI.create("http://purl.obolibrary.org/obo/cl.owl"), map.purl);

        // macrophage.owl has owl:versionIRI
        assertEquals(URI.create("http://purl.obolibrary.org/obo/cl/releases/2024-09-26/cl.owl"), map.versionIRI);

        // macrophage.owl does not have dc:title or dc:description
        assertNull(map.title);
        assertNull(map.description);

        // macrophage.owl has obo:IAO_0000700 pointing to CL_0000000
        assertEquals(URI.create("http://purl.obolibrary.org/obo/CL_0000000"), map.root);

        // Should contain CL terms
        assertTrue(map.ids.contains("CL"));
        assertTrue(map.terms.containsKey("CL_0000235"));
        assertEquals("macrophage", map.terms.get("CL_0000235").label);
    }

    @Test
    void parseOntologyElements_ro() {
        List<Path> files = List.of(testOboDir.resolve("ro.owl"));
        Map<String, OntologyElementMap> maps = OntologyElementParser.parseOntologyElements(files);

        assertEquals(1, maps.size());
        assertTrue(maps.containsKey("ro"));

        OntologyElementMap map = maps.get("ro");

        // ro.owl uses owl:Ontology with rdf:about for PURL
        assertEquals(URI.create("http://purl.obolibrary.org/obo/ro.owl"), map.purl);

        // ro.owl has owl:versionIRI
        assertEquals(URI.create("http://purl.obolibrary.org/obo/ro/releases/2024-04-24/ro.owl"), map.versionIRI);

        // Should contain RO terms
        assertTrue(map.ids.contains("RO"));
        assertTrue(map.terms.containsKey("RO_0002202"));
        assertEquals("develops from", map.terms.get("RO_0002202").label);
    }

    @Test
    void parseOntologyElements_multipleFiles() {
        List<Path> files = List.of(testOboDir.resolve("macrophage.owl"), testOboDir.resolve("ro.owl"));
        Map<String, OntologyElementMap> maps = OntologyElementParser.parseOntologyElements(files);

        assertEquals(2, maps.size());
        assertTrue(maps.containsKey("macrophage"));
        assertTrue(maps.containsKey("ro"));
    }
}
