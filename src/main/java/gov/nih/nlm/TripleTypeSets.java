package gov.nih.nlm;

import java.util.*;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TripleTypeSets {

	public int nTriples;
	public List<Triple> soFNodeTriples;
	public Map<Node, ArrayList<Triple>> sBNodeTriples;
	public Map<Node, ArrayList<Triple>> oBNodeTriples;
	public List<Triple> soBNodeTriples;
	public Set<Triple> linkingBNodeTriples;
	public Set<Triple> flattenedBNodeTriples;

	public TripleTypeSets() {
		nTriples = 0;
		soFNodeTriples = new ArrayList<>();
		sBNodeTriples = new HashMap<>();
		oBNodeTriples = new HashMap<>();
		soBNodeTriples = new ArrayList<>();
		linkingBNodeTriples = new HashSet<>();
		flattenedBNodeTriples = new HashSet<>();
	}

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