package gov.nih.nlm;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Downloads ontology files from the OBO Foundry, comparing versions to manage
 * updates.
 */
public class OntologyDownloader {

    // Assign OBO Foundry PURLs
    static final List<String> OBO_PURLS = List.of("http://purl.obolibrary.org/obo/cl.owl", "http://purl.obolibrary.org/obo/ro.owl", "http://purl.obolibrary.org/obo/go.owl", "http://purl.obolibrary.org/obo/uberon/uberon-base.owl", "http://purl.obolibrary.org/obo/ncbitaxon/subsets/taxslim.owl", "http://purl.obolibrary.org/obo/mondo/mondo-simple.owl", "http://purl.obolibrary.org/obo/hp.owl", "http://purl.obolibrary.org/obo/pato.owl", "http://purl.obolibrary.org/obo/hsapdv.owl");
    // Assign location of ontology files
    private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
    private static final Path oboDir = usrDir.resolve("data/obo");
    // Assign pattern for extracting YYYY-MM-DD dates
    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");

    /**
     * Parse the ontology XML file to find its version as a YYYY-MM-DD date
     * string. First tries owl:versionInfo, then falls back to extracting a date
     * from owl:versionIRI.
     *
     * @param oboFilePath Path to ontology XML file
     * @return Version string in YYYY-MM-DD format, or null if not found
     */
    public static String findOboVersion(Path oboFilePath) {
        System.out.println("Parsing " + oboFilePath);
        Document doc = OntologyElementParser.parseXmlFile(oboFilePath.toFile());

        // Try owl:versionInfo first
        Element versionInfoElement = (Element) doc.getElementsByTagName("owl:versionInfo").item(0);
        if (versionInfoElement != null) {
            String text = versionInfoElement.getTextContent().trim();
            Matcher matcher = DATE_PATTERN.matcher(text);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }

        // Fall back to owl:versionIRI
        Element versionIRIElement = (Element) doc.getElementsByTagName("owl:versionIRI").item(0);
        if (versionIRIElement != null) {
            String resource = versionIRIElement.getAttribute("rdf:resource");
            Matcher matcher = DATE_PATTERN.matcher(resource);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }

        System.out.println("Could not get version for " + oboFilePath);
        return null;
    }

    /**
     * Download each specified URL, parse version information from new and
     * current download, and replace current with new if new is newer than
     * current.
     *
     * @param urls        List of URLs to download
     * @param downloadDir Path to directory containing downloaded files
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the download is interrupted
     */
    public static void updateDownloads(List<String> urls, Path downloadDir) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();

        for (String url : urls) {
            System.out.println("Getting " + url);
            URI uri = URI.create(url);
            String path = uri.getPath();
            String fileName = path.substring(path.lastIndexOf('/') + 1);
            String stem = fileName.substring(0, fileName.lastIndexOf('.'));
            String suffix = fileName.substring(fileName.lastIndexOf('.'));

            // Download to a temporary file
            HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

            Path newFile = downloadDir.resolve(stem + "-new" + suffix);
            System.out.println("Writing " + newFile);
            Files.write(newFile, response.body());

            String versionNew = findOboVersion(newFile);
            System.out.println("Found new version " + versionNew);

            Path curFile = downloadDir.resolve(stem + suffix);
            if (Files.exists(curFile)) {
                String versionCur = findOboVersion(curFile);
                System.out.println("Found current version " + versionCur);

                if (versionNew != null && versionCur != null && versionNew.compareTo(versionCur) > 0) {
                    Path archiveDir = downloadDir.resolve(".archive");
                    Files.createDirectories(archiveDir);
                    Path oldFile = archiveDir.resolve(stem + "-" + versionCur + suffix);

                    System.out.println("Renaming " + curFile + " to " + oldFile);
                    Files.move(curFile, oldFile);

                    System.out.println("Renaming " + newFile + " to " + curFile);
                    Files.move(newFile, curFile);
                } else {
                    System.out.println("New version is not newer than current version");
                    System.out.println("Removing " + newFile);
                    Files.delete(newFile);
                }
            } else {
                System.out.println("Renaming " + newFile + " to " + curFile);
                Files.move(newFile, curFile);
            }
        }
    }

    /**
     * Download ontology files from the OBO Foundry.
     *
     * @param args (None expected)
     */
    public static void main(String[] args) {
        try {
            updateDownloads(OBO_PURLS, oboDir);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
