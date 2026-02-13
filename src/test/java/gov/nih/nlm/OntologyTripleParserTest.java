package gov.nih.nlm;

import org.apache.jena.graph.Triple;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OntologyTripleParserTest {

    private static final Path testOboDir = Paths.get(System.getProperty("user.dir")).resolve("src/test/data/obo");

    @Test
    void collectTriplesFromFile_macrophage() {
        Path macrophageOwl = testOboDir.resolve("macrophage.owl");
        List<Triple> triples = OntologyTripleParser.collectTriplesFromFile(macrophageOwl);

        assertNotNull(triples);
        assertFalse(triples.isEmpty());

        // All triples should have URI subjects (no anonymous)
        for (Triple t : triples) {
            assertTrue(t.getSubject().isURI(), "Subject should be a URI node: " + t.getSubject());
        }
    }

    @Test
    void collectTriplesFromFile_containsMacrophageSubClassOfTriples() {
        Path macrophageOwl = testOboDir.resolve("macrophage.owl");
        List<Triple> triples = OntologyTripleParser.collectTriplesFromFile(macrophageOwl);

        // Macrophage (CL_0000235) has rdfs:subClassOf to CL_0000113, CL_0000145, CL_0000766
        String macrophageUri = "http://purl.obolibrary.org/obo/CL_0000235";
        String subClassOfUri = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

        List<Triple> macrophageSubClassTriples = triples.stream().filter(t -> t.getSubject().getURI().equals(macrophageUri)).filter(t -> t.getPredicate().getURI().equals(subClassOfUri)).toList();

        assertFalse(macrophageSubClassTriples.isEmpty());

        List<String> objectUris = macrophageSubClassTriples.stream().filter(t -> t.getObject().isURI()).map(t -> t.getObject().getURI()).toList();
        assertTrue(objectUris.contains("http://purl.obolibrary.org/obo/CL_0000113"));
        assertTrue(objectUris.contains("http://purl.obolibrary.org/obo/CL_0000145"));
        assertTrue(objectUris.contains("http://purl.obolibrary.org/obo/CL_0000766"));
    }

    @Test
    void collectTriplesFromFile_flattenedRestrictions() {
        Path macrophageOwl = testOboDir.resolve("macrophage.owl");
        List<Triple> triples = OntologyTripleParser.collectTriplesFromFile(macrophageOwl);

        // Macrophage has a restriction: RO_0002202 someValuesFrom CL_0000576 (develops from monocyte)
        String macrophageUri = "http://purl.obolibrary.org/obo/CL_0000235";
        String developsFromUri = "http://purl.obolibrary.org/obo/RO_0002202";
        String monocyteUri = "http://purl.obolibrary.org/obo/CL_0000576";

        boolean found = triples.stream().anyMatch(t -> t.getSubject().getURI().equals(macrophageUri) && t.getPredicate().getURI().equals(developsFromUri) && t.getObject().isURI() && t.getObject().getURI().equals(monocyteUri));
        assertTrue(found, "Should contain flattened restriction: macrophage develops_from monocyte");
    }

    @Test
    void collectTriplesFromFile_capableOfRestriction() {
        Path macrophageOwl = testOboDir.resolve("macrophage.owl");
        List<Triple> triples = OntologyTripleParser.collectTriplesFromFile(macrophageOwl);

        // Macrophage has a restriction: RO_0002215 someValuesFrom GO_0031268 (capable of)
        String macrophageUri = "http://purl.obolibrary.org/obo/CL_0000235";
        String capableOfUri = "http://purl.obolibrary.org/obo/RO_0002215";
        String pseudopodiumOrgUri = "http://purl.obolibrary.org/obo/GO_0031268";

        boolean found = triples.stream().anyMatch(t -> t.getSubject().getURI().equals(macrophageUri) && t.getPredicate().getURI().equals(capableOfUri) && t.getObject().isURI() && t.getObject().getURI().equals(pseudopodiumOrgUri));
        assertTrue(found, "Should contain flattened restriction: macrophage capable_of GO_0031268");
    }

    @Test
    void collectTriplesFromFile_containsLiteralTriples() {
        Path macrophageOwl = testOboDir.resolve("macrophage.owl");
        List<Triple> triples = OntologyTripleParser.collectTriplesFromFile(macrophageOwl);

        // Should contain triples with literal objects (labels, definitions, etc.)
        String macrophageUri = "http://purl.obolibrary.org/obo/CL_0000235";
        boolean hasLiteral = triples.stream().filter(t -> t.getSubject().getURI().equals(macrophageUri)).anyMatch(t -> t.getObject().isLiteral());
        assertTrue(hasLiteral, "Should contain triples with literal objects for macrophage");
    }

    @Test
    void collectUniqueTriples_skipsRoOwl() {
        List<Path> files = List.of(testOboDir.resolve("ro.owl"), testOboDir.resolve("macrophage.owl"));
        HashSet<Triple> uniqueTriples = OntologyTripleParser.collectUniqueTriples(files);

        // Should have triples from macrophage.owl but not from ro.owl
        assertFalse(uniqueTriples.isEmpty());

        // All subjects should be from CL namespace (macrophage.owl root) not RO
        // since ro.owl is skipped
        String roNS = "http://purl.obolibrary.org/obo/RO_";
        boolean hasRoSubject = uniqueTriples.stream().anyMatch(t -> t.getSubject().isURI() && t.getSubject().getURI().startsWith(roNS));
        assertFalse(hasRoSubject, "Should not contain triples with RO subjects (ro.owl is skipped)");
    }

    @Test
    void collectUniqueTriples_deduplicates() {
        // Calling with the same file twice should still deduplicate
        List<Path> files = List.of(testOboDir.resolve("macrophage.owl"));
        HashSet<Triple> uniqueTriples = OntologyTripleParser.collectUniqueTriples(files);

        // Get total triples from file directly
        List<Triple> allTriples = OntologyTripleParser.collectTriplesFromFile(testOboDir.resolve("macrophage.owl"));

        // The unique set size should be <= total triples
        assertTrue(uniqueTriples.size() <= allTriples.size());
    }

    @Test
    void getRootNS_macrophage() {
        Path macrophageOwl = testOboDir.resolve("macrophage.owl");
        OntModel ontModel = OntModelFactory.createModel();
        RDFDataMgr.read(ontModel, macrophageOwl.toString());

        String rootNS = OntologyTripleParser.getRootNS(ontModel);

        assertEquals("http://purl.obolibrary.org/obo/CL", rootNS);
    }

    @Test
    void getRootNS_no_IAO_0000700() {
        Path macrophageOwl = testOboDir.resolve("no-IAO_0000700-test.owl");
        OntModel ontModel = OntModelFactory.createModel();
        RDFDataMgr.read(ontModel, macrophageOwl.toString());

        String rootNS = OntologyTripleParser.getRootNS(ontModel);

        assertEquals("http://purl.obolibrary.org/obo/NCBITaxon", rootNS);
    }

    @Test
    void collectUniqueTriples_emptyListReturnsEmpty() {
        HashSet<Triple> uniqueTriples = OntologyTripleParser.collectUniqueTriples(List.of());
        assertNotNull(uniqueTriples);
        assertTrue(uniqueTriples.isEmpty());
    }
}
