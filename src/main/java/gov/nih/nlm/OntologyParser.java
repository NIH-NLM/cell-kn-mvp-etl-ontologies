package gov.nih.nlm;

import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.CollectorStreamTriples;

public class OntologyParser {

	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	private static final Node axiom = NodeFactory.createURI("http://www.w3.org/2002/07/owl#Axiom");
	private static final Node restriction = NodeFactory.createURI("http://www.w3.org/2002/07/owl#Restriction");

	/**
	 * Collect all triples into sets containing triples with:
	 * <ul>
	 * <li>Filled subject and object</li>
	 * <li>Blank subject and filled object</li>
	 * <li>Filled subject and blank object</li>
	 * <li>Blank subject and object</li>
	 * </ul>
	 * 
	 * @param oboPth
	 * @param tripleTypeSets
	 */
	public static void populateTripleTypeSets(Path oboPth, TripleTypeSets tripleTypeSets) {
		if (tripleTypeSets.size() != 0) {
			throw new RuntimeException("TripleTypeSets have already been populated");
		}
		CollectorStreamTriples inputStream = new CollectorStreamTriples();
		RDFParser.source(oboPth).parse(inputStream);
		int nTriples = 0;
		for (Triple triple : inputStream.getCollected()) {
			nTriples++;
			Node s = triple.getSubject();
			Node p = triple.getPredicate();
			Node o = triple.getObject();
			if (!s.isBlank() && !o.isBlank()) {
				tripleTypeSets.soFNodeTriples.add(triple);
			} else if (s.isBlank() && !o.isBlank()) {
				if (!tripleTypeSets.sBNodeTriples.containsKey(s)) {
					tripleTypeSets.sBNodeTriples.put(s, new ArrayList<>());
				}
				tripleTypeSets.sBNodeTriples.get(s).add(triple);
			} else if (!s.isBlank() && o.isBlank()) {
				if (!tripleTypeSets.oBNodeTriples.containsKey(o)) {
					tripleTypeSets.oBNodeTriples.put(o, new ArrayList<>());
				}
				tripleTypeSets.oBNodeTriples.get(o).add(triple);
			} else {
				tripleTypeSets.soBNodeTriples.add(triple);
			}
		}
		if (tripleTypeSets.size() != nTriples) {
			System.err.println("Warning: tripleTypeSets.size() != nTriples");
		} else {
			tripleTypeSets.nTriples = nTriples;
		}
	}

	/**
	 * Move blank object triple sets into blank subject triple sets. If this method
	 * is called after populating the triple type sets, the blank subject triple
	 * sets will each use a single blank node throughout the set.
	 * 
	 * @param tripleTypeSets
	 */
	public static void moveOtoSBNodeTriples(TripleTypeSets tripleTypeSets) {
		for (Node key : tripleTypeSets.oBNodeTriples.keySet().toArray(new Node[0])) {
			if (tripleTypeSets.sBNodeTriples.containsKey(key)) {
				tripleTypeSets.sBNodeTriples.get(key).addAll(tripleTypeSets.oBNodeTriples.get(key));
				tripleTypeSets.oBNodeTriples.remove(key);
			}
		}
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: tripleTypeSets.size() != nTriples");
		}
	}

	public static Node getFNode(Triple triple) {
		if (triple.getSubject().isBlank() && triple.getObject().isBlank()) {
			throw new IllegalArgumentException("Triple subject and object are both blank");
		} else if (!triple.getSubject().isBlank() && !triple.getObject().isBlank()) {
			throw new IllegalArgumentException("Triple subject and object are both filled");
		} else if (triple.getSubject().isBlank()) {
			return triple.getObject();
		} else {
			return triple.getSubject();
		}
	}

	public static void flattenAxiomTripleSets(TripleTypeSets tripleTypeSets, Node key) {

		Node flattened_s = null;
		Node flattened_p = null;
		Node flattened_o = null;
		ArrayList<Triple> flattenedTriples = new ArrayList<>();
		ArrayList<Triple> remainingTriples = new ArrayList<>();
		for (Triple triple : tripleTypeSets.sBNodeTriples.get(key)) {
			String p_fragment = URI.create(triple.getPredicate().getURI()).getFragment();
			if (p_fragment != null && p_fragment.equals("annotatedSource")) {
				flattenedTriples.add(triple);
				flattened_s = getFNode(triple);
			} else if (p_fragment != null && p_fragment.equals("annotatedProperty")) {
				flattenedTriples.add(triple);
				flattened_p = getFNode(triple);
			} else if (p_fragment != null && p_fragment.equals("annotatedTarget")) {
				flattenedTriples.add(triple);
				flattened_o = getFNode(triple);
			} else if (p_fragment != null && !p_fragment.equals("type")) {
				remainingTriples.add(triple);
			}
		}
		if (flattened_s != null && flattened_p != null && flattened_o != null) {
			tripleTypeSets.nTriples++;
			tripleTypeSets.soFNodeTriples.add(Triple.create(flattened_s, flattened_p, flattened_o));
			tripleTypeSets.flattenedBNodeTriples.addAll(flattenedTriples);
			tripleTypeSets.sBNodeTriples.get(key).removeAll(flattenedTriples);

			flattenedTriples.clear();
			for (Triple triple : remainingTriples) {
				flattened_o = triple.getObject();
				if (flattened_o.isLiteral()) {
					flattenedTriples.add(triple);
					tripleTypeSets.nTriples++;
					flattened_p = triple.getPredicate();
					tripleTypeSets.soFNodeTriples.add(Triple.create(flattened_s, flattened_p, flattened_o));
					tripleTypeSets.flattenedBNodeTriples.add(triple);
				}
			}
			tripleTypeSets.sBNodeTriples.get(key).removeAll(flattenedTriples);
		}
	}

	public static void flattenRestrictionTripleSets(TripleTypeSets tripleTypeSets, Node key) {

		Node flattened_s = null;
		Node flattened_p = null;
		Node flattened_o = null;
		ArrayList<Triple> flattenedTriples = new ArrayList<>();
		ArrayList<Triple> remainingTriples = new ArrayList<>();
		for (Triple triple : tripleTypeSets.sBNodeTriples.get(key)) {
			String p_fragment = URI.create(triple.getPredicate().getURI()).getFragment();
			if (p_fragment.equals("subClassOf")) {
				flattenedTriples.add(triple);
				flattened_s = getFNode(triple);
			} else if (p_fragment.equals("onProperty")) {
				flattenedTriples.add(triple);
				flattened_p = getFNode(triple);
			} else if (p_fragment.equals("someValuesFrom")) {
				flattenedTriples.add(triple);
				flattened_o = getFNode(triple);
			} else if (!p_fragment.equals("type")) {
				remainingTriples.add(triple);
			}
		}
		if (flattened_s != null && flattened_p != null && flattened_o != null) {
			tripleTypeSets.nTriples++;
			tripleTypeSets.soFNodeTriples.add(Triple.create(flattened_s, flattened_p, flattened_o));
			tripleTypeSets.flattenedBNodeTriples.addAll(flattenedTriples);
			tripleTypeSets.sBNodeTriples.get(key).removeAll(flattenedTriples);
		}
	}

	public static void flattenSBNodeTriples(TripleTypeSets tripleTypeSets) {
		String[] fragments = { "Axiom", "Restriction" };
		for (String fragment : fragments) {
			for (Node key : tripleTypeSets.sBNodeTriples.keySet().toArray(new Node[0])) {
				for (Triple triple : tripleTypeSets.sBNodeTriples.get(key).toArray(new Triple[0])) {
					Node o = triple.getObject();
					if (o.isURI()) {
						String o_fragment = URI.create(o.getURI()).getFragment();
						if (o_fragment != null && o_fragment.equals(fragment)) {
							if (fragment.equals("Axiom")) {
								flattenAxiomTripleSets(tripleTypeSets, key);
							} else if (fragment.equals("Restriction")) {
								flattenRestrictionTripleSets(tripleTypeSets, key);
							}
							break;
						}
					}
				}
			}
			if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
				System.err.println("Warning: tripleTypeSets.size() != nTriples");
			}
		}
	}

	/**
	 * Collect triples with blank subject and object if the blank subject and object
	 * are contained in the subject blank node triple sets, then remove these
	 * linking blank triples from the both blank triples.
	 *
	 * @param tripleTypeSets
	 */
	public static void collectLinkingBnodeTriples(TripleTypeSets tripleTypeSets) {
		for (Triple triple : tripleTypeSets.soBNodeTriples.toArray(new Triple[0])) {
			Node s = triple.getSubject();
			Node o = triple.getObject();
			if (tripleTypeSets.sBNodeTriples.containsKey(s) && tripleTypeSets.sBNodeTriples.containsKey(o)) {
				tripleTypeSets.linkingBNodeTriples.add(triple);
			}
		}
		tripleTypeSets.soBNodeTriples.removeAll(tripleTypeSets.linkingBNodeTriples);
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: tripleTypeSets.size() != nTriples");
		}
	}

	public static void linkSBNodeTriples(TripleTypeSets tripleTypeSets) {
		for (Triple triple : tripleTypeSets.linkingBNodeTriples.toArray(new Triple[0])) {
			Node s = triple.getSubject();
			Node o = triple.getObject();
			tripleTypeSets.sBNodeTriples.get(s).addAll(tripleTypeSets.sBNodeTriples.get(o));
			tripleTypeSets.sBNodeTriples.get(o).clear();
		}
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: tripleTypeSets.size() != nTriples");
		}
	}

	public static Map<String, TripleTypeSets> parseOntologies(List<Path> files) {
		Map<String, TripleTypeSets> ontologyTripleTypeSets = new HashMap<String, TripleTypeSets>();
		for (Path file : files) {
			System.out.println("Processing " + file.getFileName());
			TripleTypeSets tripleTypeSets = new TripleTypeSets();
			populateTripleTypeSets(file, tripleTypeSets);
			moveOtoSBNodeTriples(tripleTypeSets);
			flattenSBNodeTriples(tripleTypeSets);
			// TODO: Take a another look at the utility of these
			// collectLinkingBnodeTriples(tripleTypeSets);
			// linkSBNodeTriples(tripleTypeSets);
			ontologyTripleTypeSets.put(file.getFileName().toString(), tripleTypeSets);
		}
		return ontologyTripleTypeSets;
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
			parseOntologies(files);
		}
	}
}
