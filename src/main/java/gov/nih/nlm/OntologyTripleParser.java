package gov.nih.nlm;

import static gov.nih.nlm.OntologyElementParser.createURI;
import static gov.nih.nlm.PathUtilities.listFilesMatchingPattern;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.CollectorStreamTriples;

/**
 * Parses each ontology file in the data/obo directory, parses each file to
 * produce triple sets sorted by the types of nodes the triples contain.
 */
public class OntologyTripleParser {

	// Assign location of ontology files
	private static final Path usrDir = Paths.get(System.getProperty("user.dir"));
	private static final Path oboDir = usrDir.resolve("data/obo");

	/**
	 * Collect all triples into sets containing triples with:
	 * <ul>
	 * <li>Filled subject and object</li>
	 * <li>Blank subject and filled object</li>
	 * <li>Filled subject and blank object</li>
	 * <li>Blank subject and object</li>
	 * </ul>
	 * 
	 * @param oboPth         Path to ontology file
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void populateTripleTypeSets(Path oboPth, TripleTypeSets tripleTypeSets) {
		// Only populate empty triple type sets
		if (tripleTypeSets.size() != 0) {
			throw new RuntimeException("TripleTypeSets have already been populated");
		}
		// Parse ontology file, and process each triple
		CollectorStreamTriples inputStream = new CollectorStreamTriples();
		RDFParser.source(oboPth).parse(inputStream);
		int nTriples = 0;
		for (Triple triple : inputStream.getCollected()) {
			nTriples++;
			Node s = triple.getSubject();
			Node p = triple.getPredicate();
			Node o = triple.getObject();
			if (!s.isBlank() && !o.isBlank()) {
				// Collect filled node triples
				tripleTypeSets.soFNodeTriples.add(triple);
			} else if (s.isBlank() && !o.isBlank()) {
				// Collect blank subject node triples in sets identified by the blank subject
				// node
				if (!tripleTypeSets.sBNodeTriples.containsKey(s)) {
					tripleTypeSets.sBNodeTriples.put(s, new ArrayList<>());
				}
				tripleTypeSets.sBNodeTriples.get(s).add(triple);
			} else if (!s.isBlank() && o.isBlank()) {
				// Collect blank object node triples in sets identified by the blank object node
				if (!tripleTypeSets.oBNodeTriples.containsKey(o)) {
					tripleTypeSets.oBNodeTriples.put(o, new ArrayList<>());
				}
				tripleTypeSets.oBNodeTriples.get(o).add(triple);
			} else {
				// Collect blank subject and object node triples
				tripleTypeSets.soBNodeTriples.add(triple);
			}
		}
		// Ensure all triples typed, and assign number of triples
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
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void moveOtoSBNodeTriples(TripleTypeSets tripleTypeSets) {
		for (Node key : tripleTypeSets.oBNodeTriples.keySet().toArray(new Node[0])) {
			if (tripleTypeSets.sBNodeTriples.containsKey(key)) {
				tripleTypeSets.sBNodeTriples.get(key).addAll(tripleTypeSets.oBNodeTriples.get(key));
				tripleTypeSets.oBNodeTriples.remove(key);
			}
		}
		// Ensure no triples have been added or removed
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: tripleTypeSets.size() != nTriples");
		}
	}

	/**
	 * Get the filled node of a triple containing either a blank subject or object
	 * node.
	 *
	 * @param triple Triple containing a blank subject or object node
	 * @return Filled node
	 */
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

	/**
	 * Flatten Axiom triple sets by identifying the components of the Axiom triple
	 * contained in the triples of the set, creating a corresponding triple, and
	 * adding it to the list of triples with filled subject and object nodes.
	 * <p>
	 * Use triples remaining after flattening to add annotations corresponding to
	 * the identified subject node.
	 * </p>
	 *
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 * @param key            Node identifying triple lists containing the same blank
	 *                       node
	 */
	public static void flattenAxiomTripleSets(TripleTypeSets tripleTypeSets, Node key) {
		Node flattened_s = null;
		Node flattened_p = null;
		Node flattened_o = null;
		ArrayList<Triple> flattenedTriples = new ArrayList<>();
		ArrayList<Triple> remainingTriples = new ArrayList<>();
		for (Triple triple : tripleTypeSets.sBNodeTriples.get(key)) {
			String p_fragment = createURI(triple.getPredicate().getURI()).getFragment();
			// Identify components of the Axiom triple
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
			// Create triple, and add it to the list of triples with filled subject and
			// object nodes
			tripleTypeSets.nTriples++;
			tripleTypeSets.soFNodeTriples.add(Triple.create(flattened_s, flattened_p, flattened_o));

			// Keep track of flattened triples
			tripleTypeSets.flattenedBNodeTriples.addAll(flattenedTriples);
			tripleTypeSets.sBNodeTriples.get(key).removeAll(flattenedTriples);

			// Use remaining triples to add annotations corresponding to the
			// identified subject node
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

	/**
	 * Flatten Restriction triple sets by identifying the components of the
	 * Restriction triple contained in the triples of the set, creating a
	 * corresponding triple, and adding it to the list of triples with filled
	 * subject and object nodes.
	 *
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 * @param key            Node identifying triple lists containing the same blank
	 *                       node
	 */
	public static void flattenRestrictionTripleSets(TripleTypeSets tripleTypeSets, Node key) {
		Node flattened_s = null;
		Node flattened_p = null;
		Node flattened_o = null;
		ArrayList<Triple> flattenedTriples = new ArrayList<>();
		ArrayList<Triple> remainingTriples = new ArrayList<>();
		for (Triple triple : tripleTypeSets.sBNodeTriples.get(key)) {
			String p_fragment = createURI(triple.getPredicate().getURI()).getFragment();
			// Identify components of the Restriction triple
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
			// Create triple, and add it to the list of triples with filled subject and
			// object nodes
			tripleTypeSets.nTriples++;
			tripleTypeSets.soFNodeTriples.add(Triple.create(flattened_s, flattened_p, flattened_o));

			// Keep track of flattened triples
			tripleTypeSets.flattenedBNodeTriples.addAll(flattenedTriples);
			tripleTypeSets.sBNodeTriples.get(key).removeAll(flattenedTriples);
		}
	}

	/**
	 * Flatten Axiom and Restriction triple sets.
	 * 
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
	 */
	public static void flattenSBNodeTriples(TripleTypeSets tripleTypeSets) {
		// Process each triple list containing the same blank node successively without
		// assuming Axiom and Restriction triple sets are not contained in the same list
		String[] fragments = { "Axiom", "Restriction" };
		for (String fragment : fragments) {
			for (Node key : tripleTypeSets.sBNodeTriples.keySet().toArray(new Node[0])) {
				for (Triple triple : tripleTypeSets.sBNodeTriples.get(key).toArray(new Triple[0])) {
					Node o = triple.getObject();
					if (o.isURI()) {
						String o_fragment = createURI(o.getURI()).getFragment();
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
			// Ensure no triples have been added or removed
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
	 * @param tripleTypeSets Triple sets sorted by the types of nodes the triples
	 *                       contain
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

		// Ensure no triples have been added or removed
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: tripleTypeSets.size() != nTriples");
		}
	}

	/**
	 * Combine subject blank node triple sets linked by triple with blank subject
	 * and object nodes.
	 * 
	 * @param tripleTypeSets
	 */
	public static void linkSBNodeTriples(TripleTypeSets tripleTypeSets) {
		for (Triple triple : tripleTypeSets.linkingBNodeTriples.toArray(new Triple[0])) {
			Node s = triple.getSubject();
			Node o = triple.getObject();
			tripleTypeSets.sBNodeTriples.get(s).addAll(tripleTypeSets.sBNodeTriples.get(o));
			tripleTypeSets.sBNodeTriples.get(o).clear();
		}
		// Ensure no triples have been added or removed
		if (tripleTypeSets.size() != tripleTypeSets.nTriples) {
			System.err.println("Warning: tripleTypeSets.size() != nTriples");
		}
	}

	/**
	 * Parse ontology files to produce triple sets sorted by the types of nodes the
	 * triples contain.
	 *
	 * @param files Paths to ontology files
	 * @return Map by ontology file name containing triple sets sorted by the types
	 *         of nodes the triples contain
	 */
	public static Map<String, TripleTypeSets> parseOntologyTriples(List<Path> files) {
		Map<String, TripleTypeSets> ontologyTripleTypeSets = new HashMap<>();
		for (Path file : files) {
			String oboFNm = file.getFileName().toString();
			System.out.println("Parsing ontology triples in " + oboFNm);
			TripleTypeSets tripleTypeSets = new TripleTypeSets();
			populateTripleTypeSets(file, tripleTypeSets);
			moveOtoSBNodeTriples(tripleTypeSets);
			flattenSBNodeTriples(tripleTypeSets);
			// TODO: Take a another look at the utility of these
			// collectLinkingBnodeTriples(tripleTypeSets);
			// linkSBNodeTriples(tripleTypeSets);
			ontologyTripleTypeSets.put(oboFNm, tripleTypeSets);
		}
		return ontologyTripleTypeSets;
	}

	/**
	 * Parses each ontology file in the data/obo directory, parses each file to
	 * produce triple sets sorted by the types of nodes the triples contain.
	 * 
	 * @param args (None expected)
	 */
	public static void main(String[] args) {
		String directoryPath = oboDir.toString();
		String filePattern = ".*\\.owl";
		List<Path> files = null;
		try {
			files = listFilesMatchingPattern(directoryPath, filePattern);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Map<String, TripleTypeSets> ontologyTripleTypeSets = null;
		if (files.isEmpty()) {
			System.out.println("No files found matching the pattern.");
		} else {
			ontologyTripleTypeSets = parseOntologyTriples(files);
		}
		System.out.println("Parsed ontology triples from " + files.size() + " files.");
	}
}
