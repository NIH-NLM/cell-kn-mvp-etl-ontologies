package gov.nih.nlm;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class OntologyDownloaderTest {

    private static final Path testOboDir = Paths.get(System.getProperty("user.dir")).resolve("src/test/data/obo");

    // --- findOboVersion tests ---

    @Test
    void findOboVersion_fromVersionInfo() {
        // version-info-test.owl has owl:versionInfo with text "2024-01-15"
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("version-info-test.owl"));
        assertEquals("2024-01-15", version);
    }

    @Test
    void findOboVersion_fromVersionIRI() {
        // macrophage.owl has owl:versionIRI but no owl:versionInfo
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("macrophage.owl"));
        assertEquals("2024-09-26", version);
    }

    @Test
    void findOboVersion_prefersVersionInfo() {
        // ro.owl has both owl:versionInfo and owl:versionIRI with the same date
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("ro.owl"));
        assertEquals("2024-04-24", version);
    }

    @Test
    void findOboVersion_noVersion() {
        // no-version-test.owl has neither owl:versionInfo nor owl:versionIRI
        String version = OntologyDownloader.findOboVersion(testOboDir.resolve("no-version-test.owl"));
        assertNull(version);
    }

    // --- OBO_PURLS tests ---

    @Test
    void oboPurls_containsExpectedUrls() {
        assertEquals(9, OntologyDownloader.OBO_PURLS.size());
        assertTrue(OntologyDownloader.OBO_PURLS.contains("http://purl.obolibrary.org/obo/cl.owl"));
        assertTrue(OntologyDownloader.OBO_PURLS.contains("http://purl.obolibrary.org/obo/ro.owl"));
    }
}
