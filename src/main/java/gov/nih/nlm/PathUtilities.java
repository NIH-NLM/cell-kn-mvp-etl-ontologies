package gov.nih.nlm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Collects common methods for handling paths.
 */
public class PathUtilities {

    /**
     * List files in a directory matching a pattern.
     *
     * @param directoryPath Directory containing the files
     * @param filePattern   Pattern for matching to files
     * @return List of matching files
     * @throws IOException On read
     */
    public static List<Path> listFilesMatchingPattern(String directoryPath, String filePattern) throws IOException {
        Pattern pattern = Pattern.compile(filePattern);
        try (var filesStream = Files.list(Paths.get(directoryPath))) {
            return filesStream.filter(Files::isRegularFile).filter(path -> pattern.matcher(path.getFileName().toString()).matches()).collect(Collectors.toList());
        }
    }
}
