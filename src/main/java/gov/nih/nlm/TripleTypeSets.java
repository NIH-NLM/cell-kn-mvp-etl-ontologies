package gov.nih.nlm;

import java.util.*;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * Triple sets sorted by the types of nodes the triples contain.
 */
public class TripleTypeSets {

	/** Total number of triples */
	public int nTriples;

	/** Triples with filled subject and object nodes */
	public List<Triple> soFNodeTriples;
	/** Triple lists containing the same blank subject node */
	public Map<Node, ArrayList<Triple>> sBNodeTriples;
	/** Triple lists containing the same blank object node */
	public Map<Node, ArrayList<Triple>> oBNodeTriples;
	/** Triples with blank subject and object nodes */
	public List<Triple> soBNodeTriples;
	/**
	 * Triples with blank subject and object nodes that link triple lists containing
	 * the same blank subject or object node
	 */
	public Set<Triple> linkingBNodeTriples;
	/** Triples of Axiom and Restriction triple lists which have been flattened */
	public Set<Triple> flattenedBNodeTriples;

	/**
	 * No argument constructor initializes triple sets.
	 */
	public TripleTypeSets() {
		nTriples = 0;
		soFNodeTriples = new ArrayList<>();
		sBNodeTriples = new HashMap<>();
		oBNodeTriples = new HashMap<>();
		soBNodeTriples = new ArrayList<>();
		linkingBNodeTriples = new HashSet<>();
		flattenedBNodeTriples = new HashSet<>();
	}

	/**
	 * Count the total number of triples in all triple sets.
	 *
	 * @return Total number triples
	 */
	public int size() {
		int totalSize = soFNodeTriples.size();
		for (Map.Entry<Node, ArrayList<Triple>> entry : sBNodeTriples.entrySet()) {
			totalSize += entry.getValue().size();
		}
		for (Map.Entry<Node, ArrayList<Triple>> entry : oBNodeTriples.entrySet()) {
			totalSize += entry.getValue().size();
		}
		totalSize += soBNodeTriples.size() + linkingBNodeTriples.size() + flattenedBNodeTriples.size();
		return totalSize;
	}
}