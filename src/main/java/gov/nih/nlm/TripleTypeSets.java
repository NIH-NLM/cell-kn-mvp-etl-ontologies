package gov.nih.nlm;

import java.util.*;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TripleTypeSets {

	public List<Triple> fNodeTriples;
	public Map<Node, ArrayList<Triple>> sBNodeTriples;
	public Map<Node, ArrayList<Triple>> oBNodeTriples;
	public List<Triple> bBNodeTriples;
	public Set<Triple> lBNodeTriples;

	public TripleTypeSets() {
		List<Triple> filledNodeTriples = new ArrayList<>();
		Map<Node, ArrayList<Triple>> sBlankNodeTriples = new HashMap<>();
		Map<Node, ArrayList<Triple>> oBlankNodeTriples = new HashMap<>();
		List<Triple> bBlankTriples = new ArrayList<>();
		Set<Triple> lBlankTriples = new HashSet<>();
	}
}
