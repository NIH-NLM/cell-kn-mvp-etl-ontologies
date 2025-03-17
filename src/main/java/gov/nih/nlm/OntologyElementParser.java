package gov.nih.nlm;

import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Identifies ontology files in the data/obo directory, parses each file to
 * produce unique term ids, and a mapping of ontology term to ontology term
 * PURLs and labels for all elements with a non-empty "about" attribute and at
 * least one "label" element.
 */
public class OntologyElementParser {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	// Assign pattern for matching to required elements
	private static final Pattern owlPattern = Pattern.compile("^owl:");

	// Assign pattern for matching to pcl/CS terms
	private static final Pattern pclPattern = Pattern.compile("/pcl/CS");

	// Assign pattern for matching to ensembl/ENSG terms
	private static final Pattern ensemblPattern = Pattern.compile("/ensembl/ENSG");

	/**
	 * Parse the specified file, and normalize.
	 * 
	 * @param xmlFile File containing XML to parse
	 * @return Document resulting after parsing, and normalization
	 */
	public static Document parseXmlFile(File xmlFile) {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		}
		Document doc;
		try {
			doc = dBuilder.parse(xmlFile);
		} catch (SAXException | IOException e) {
			throw new RuntimeException(e);
		}
		doc.getDocumentElement().normalize();
		return doc;
	}

	/**
	 * Create a URI from a string, handling the provisional cell ontology as special
	 * cases.
	 * 
	 * @param uri String from which to create URI
	 * @return URI created
	 */
	public static URI createURI(String uri) throws RuntimeException {
		Matcher pclMatcher = pclPattern.matcher(uri);
		if (pclMatcher.find()) {
			return URI.create(pclMatcher.replaceFirst("/PCLCS_"));
		}
		Matcher ensembleMatcher = ensemblPattern.matcher(uri);
		if (ensembleMatcher.find()) {
			return URI.create(ensembleMatcher.replaceFirst("/ENSG_"));
		}
		return URI.create(uri);
	}

	/**
	 * Parse a node recursively to find all elements in the "owl" namespace which
	 * contain a non-empty "about" attribute, and at least one "label" element. Also
	 * collect resulting unique ontology term ids.
	 * 
	 * @param node               A triple node
	 * @param ontologyElementMap Maps terms and labels
	 */
	public static void parseOntologyNode(Node node, OntologyElementMap ontologyElementMap) throws RuntimeException {
		// Consider element nodes
		if (node.getNodeType() == Node.ELEMENT_NODE) {
			Element element = (Element) node;

			// Consider elements with tags in the "owl" namespace
			if (owlPattern.matcher(element.getTagName()).find()) {

				// Consider elements with a non-empty "about" attribute
				String about = element.getAttribute("rdf:about");
				if (!about.isEmpty()) {

					// Consider terms containing an underscore
					URI uri = createURI(about);
					String term = Paths.get(uri.getPath()).getFileName().toString();
					if (term.contains("_")) {
						String id = term.split("_")[0];
						// TODO: Add other unneeded terms here
						if (!id.equals("valid")) {
							ontologyElementMap.ids.add(id);
						}
					}

					// Consider terms with at least one "label" element
					NodeList nodeList = element.getElementsByTagName("rdfs:label");
					if (nodeList.getLength() > 0) {
						Element labelElement = (Element) nodeList.item(0);
						String label = labelElement.getTextContent();
						ontologyElementMap.terms.put(term, new OntologyElementMap.OntologyTerm(uri, label));
					}
				}
			}
			// Parse every child node
			NodeList nodeList = node.getChildNodes();
			for (int i = 0; i < nodeList.getLength(); i++) {
				parseOntologyNode(nodeList.item(i), ontologyElementMap);
			}
		}
	}

	/**
	 * Parse ontology files to produce ontology terms for all elements with a
	 * non-empty "about" attribute and at least one "label" element.
	 *
	 * @param files Paths to ontology files
	 * @return Map by ontology term containing ontology term PURLs and labels for
	 *         all elements with a non-empty "about" attribute and at least one
	 *         "label" element, and corresponding unique term ids.
	 */
	public static Map<String, OntologyElementMap> parseOntologyElements(List<Path> files) throws RuntimeException {
		Map<String, OntologyElementMap> ontologyElementMaps = new HashMap<>();
		for (Path file : files) {
			String oboFNm = file.getFileName().toString();
			System.out.println("Parsing ontology element in " + oboFNm);
			Document doc = parseXmlFile(file.toFile());
			OntologyElementMap ontologyElementMap = new OntologyElementMap();
			// Get title
			Element titleElement = (Element) doc.getElementsByTagName("dc:title").item(0);
			if (titleElement != null) {
				ontologyElementMap.title = titleElement.getTextContent();
			}
			// Get description
			Element descriptionElement = (Element) doc.getElementsByTagName("dc:description").item(0);
			if (descriptionElement != null) {
				ontologyElementMap.description = descriptionElement.getTextContent();
			}
			// Get PURL
			Element purlElement = (Element) doc.getElementsByTagName("owl:Ontology").item(0);
			if (purlElement != null) {
				ontologyElementMap.purl = URI.create(purlElement.getAttribute("rdf:about"));
				// Get version
				Element versionElement = (Element) purlElement.getElementsByTagName("owl:versionIRI").item(0);
				if (versionElement != null) {
					ontologyElementMap.versionIRI = URI.create(versionElement.getAttribute("rdf:resource"));
				}
			}
			// Get root
			Element rootElement = (Element) doc.getElementsByTagName("obo:IAO_0000700").item(0);
			if (rootElement != null) {
				ontologyElementMap.root = URI.create(rootElement.getAttribute("rdf:resource"));
			}
			// Parse the first node
			parseOntologyNode(doc.getDocumentElement(), ontologyElementMap);
			// Map maps by filename
			ontologyElementMaps.put(oboFNm.substring(0, oboFNm.lastIndexOf(".")), ontologyElementMap);
		}
		return ontologyElementMaps;
	}

	/**
	 * Identify ontology files in the data/obo directory, parse each file to produce
	 * unique term ids, and a mapping of ontology term to ontology term PURLs and
	 * labels for all elements with a non-empty "about" attribute and at least one
	 * "label" element.
	 *
	 * @param args (None expected)
	 */
	public static void main(String[] args) {
		String directoryPath = oboDir.toString();
		String filePattern = ".*\\.owl";
		List<Path> files;
		try {
			files = listFilesMatchingPattern(directoryPath, filePattern);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (files.isEmpty()) {
			System.out.println("No files found matching the pattern.");
		} else {
			parseOntologyElements(files);
		}
		System.out.println("Parsed ontology elements from " + files.size() + " files.");
	}
}
