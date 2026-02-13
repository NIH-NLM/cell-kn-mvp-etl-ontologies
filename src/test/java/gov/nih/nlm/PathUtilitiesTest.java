package gov.nih.nlm;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

class PathUtilitiesTest {

	private static final Path testOboDir = Paths.get(System.getProperty("user.dir"))
			.resolve("src/test/data/obo");

	@Test
	void listFilesMatchingPattern_owlFiles() throws IOException {
		List<Path> files = PathUtilities.listFilesMatchingPattern(testOboDir.toString(), ".*\\.owl");
		assertNotNull(files);
		assertEquals(5, files.size());
		List<String> names = files.stream().map(p -> p.getFileName().toString()).sorted().toList();
		assertTrue(names.contains("macrophage.owl"));
		assertTrue(names.contains("no-IAO_0000700-test.owl"));
		assertTrue(names.contains("ro.owl"));
		assertTrue(names.contains("version-info-test.owl"));
		assertTrue(names.contains("no-version-test.owl"));
	}

	@Test
	void listFilesMatchingPattern_specificFile() throws IOException {
		List<Path> files = PathUtilities.listFilesMatchingPattern(testOboDir.toString(), "ro\\.owl");
		assertEquals(1, files.size());
		assertEquals("ro.owl", files.get(0).getFileName().toString());
	}

	@Test
	void listFilesMatchingPattern_noMatch() throws IOException {
		List<Path> files = PathUtilities.listFilesMatchingPattern(testOboDir.toString(), ".*\\.txt");
		assertNotNull(files);
		assertTrue(files.isEmpty());
	}

	@Test
	void listFilesMatchingPattern_invalidDirectory() {
		assertThrows(IOException.class, () -> {
			PathUtilities.listFilesMatchingPattern("/nonexistent/directory", ".*\\.owl");
		});
	}

	@Test
	void listFilesMatchingPattern_returnsRegularFilesOnly() throws IOException {
		List<Path> files = PathUtilities.listFilesMatchingPattern(testOboDir.toString(), ".*");
		for (Path file : files) {
			assertTrue(file.toFile().isFile());
		}
	}
}
