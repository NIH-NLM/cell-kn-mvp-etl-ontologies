package gov.nih.nlm;

import org.apache.jena.graph.Node;

public class NodeTypeCounts {

	public int nIsBlank;
	public int nIsConcrete;
	public int nIsExt;
	public int nIsLiteral;
	public int nIsNodeGraph;
	public int nIsNodeTriple;
	public int nIsURI;
	public int nIsVariable;

	public NodeTypeCounts() {
		nIsBlank = 0;
		nIsConcrete = 0;
		nIsExt = 0;
		nIsLiteral = 0;
		nIsNodeGraph = 0;
		nIsNodeTriple = 0;
		nIsURI = 0;
		nIsVariable = 0;
	}

	public void countNodeTypes(Node node) {
		if (node.isBlank()) {
			nIsBlank++;
		}
		if (node.isConcrete()) {
			nIsConcrete++;
		}
		if (node.isExt()) {
			nIsExt++;
		}
		if (node.isLiteral()) {
			nIsLiteral++;
		}
		if (node.isNodeGraph()) {
			nIsNodeGraph++;
		}
		if (node.isNodeTriple()) {
			nIsNodeTriple++;
		}
		if (node.isURI()) {
			nIsURI++;
		}
		if (node.isVariable()) {
			nIsVariable++;
		}
	}

	public String toString() {
		return String.format("%d,%d,%d,%d,%d,%d,%d,%d\n", nIsBlank, nIsConcrete, nIsExt, nIsLiteral, nIsNodeGraph,
				nIsNodeTriple, nIsURI, nIsVariable);
	}

}
