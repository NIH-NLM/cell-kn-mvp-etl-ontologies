package gov.nih.nlm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.CollectorStreamTriples;

public class NodeTypeCounter {

	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	public static List<Path> listFilesMatchingPattern(String directoryPath, String filePattern) throws IOException {
		Pattern pattern = Pattern.compile(filePattern);
		try (var filesStream = Files.list(Paths.get(directoryPath))) {
			return filesStream.filter(Files::isRegularFile)
					.filter(path -> pattern.matcher(path.getFileName().toString()).matches())
					.collect(Collectors.toList());
		}
	}

	public static void countNodeTypesForOntology(Path oboPth, BufferedWriter writer, Boolean doPrintHeader)
			throws IOException {
		System.out.println("Counting: " + oboPth);
		if (doPrintHeader) {
			writer.write("Ontology,Triples,Entity,Blank,Concrete,Ext,Literal,NodeGraph,NodeTriple,URI,Variable\n");
		}
		CollectorStreamTriples inputStream = new CollectorStreamTriples();
		RDFParser.source(oboPth).parse(inputStream);
		record Counts(int nIsBlank, int nIsConcrete, int nIsExt, int nIsLiteral, int nIsNodeGraph, int nIsNodeTriple,
				int nIsURI, int nIsVariable) {
		}
		int nTriples = 0;
		NodeTypeCounts subjectCounts = new NodeTypeCounts();
		NodeTypeCounts predicateCounts = new NodeTypeCounts();
		NodeTypeCounts objectCounts = new NodeTypeCounts();
		for (Triple triple : inputStream.getCollected()) {
			nTriples++;
			subjectCounts.countNodeTypes(triple.getSubject());
			predicateCounts.countNodeTypes(triple.getPredicate());
			objectCounts.countNodeTypes(triple.getObject());
		}
		writer.write(String.format("%s,%d,%s,%s", oboPth.getFileName(), nTriples, "Subject", subjectCounts));
		writer.write(String.format("%s,%d,%s,%s", oboPth.getFileName(), nTriples, "Predicate", predicateCounts));
		writer.write(String.format("%s,%d,%s,%s", oboPth.getFileName(), nTriples, "Object", objectCounts));
	}

	public static void main(String[] args) {
		String directoryPath = oboDir.toString();
		String filePattern = ".*\\.owl";
		List<Path> files = null;
		try {
			files = listFilesMatchingPattern(directoryPath, filePattern);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (files.isEmpty()) {
			System.out.println("No files found matching the pattern.");
		} else {
			String outFNm = oboDir.resolve("../../results/countNodeTypes.csv").normalize().toString();
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(outFNm));
				boolean doPrintHeader = true;
				for (Path file : files) {
					countNodeTypesForOntology(file, writer, doPrintHeader);
					if (doPrintHeader) {
						doPrintHeader = false;
					}
				}
				writer.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
