package gov.nih.nlm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.CollectorStreamTriples;

import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

public class NodeTypeCounter {

	// Specify location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	/**
	 * Parse the ontology file, count types of the subject, predicate, and object node of every triple, then write the result.
	 *
	 * @param oboPth Path to ontology file
	 * @param writer Write with which to write results
	 * @param doPrintHeader Flag to print header, or not
	 * @throws IOException
	 */
	public static void countNodeTypesForOntology(Path oboPth, BufferedWriter writer, Boolean doPrintHeader)
			throws IOException {
		System.out.println("Counting: " + oboPth);
		if (doPrintHeader) {
			writer.write("Ontology,Triples,Entity,Blank,Concrete,Ext,Literal,NodeGraph,NodeTriple,URI,Variable\n");
		}
		// Parse triples
		CollectorStreamTriples inputStream = new CollectorStreamTriples();
		RDFParser.source(oboPth).parse(inputStream);

		// Count each node of each triple
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

		// Write results
		writer.write(String.format("%s,%d,%s,%s", oboPth.getFileName(), nTriples, "Subject", subjectCounts));
		writer.write(String.format("%s,%d,%s,%s", oboPth.getFileName(), nTriples, "Predicate", predicateCounts));
		writer.write(String.format("%s,%d,%s,%s", oboPth.getFileName(), nTriples, "Object", objectCounts));
	}

	// TODO: Complete documentation
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
