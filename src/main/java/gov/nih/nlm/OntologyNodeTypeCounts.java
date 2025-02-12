package gov.nih.nlm;

import org.apache.jena.graph.Node;

/**
 * Counts of specified ontology node types.
 */
public class OntologyNodeTypeCounts {

	/** Number of blank nodes */
	public int nIsBlank;
	/** Number of concrete nodes (nodes that have data in the RDF Graph) */
	public int nIsConcrete;
	/** Number of extension nodes */
	public int nIsExt;
	/** Number of literal nodes */
	public int nIsLiteral;
	/** Number of "graph nodes" (N3 formula) */
	public int nIsNodeGraph;
	/** Number of "triple nodes" (RDF-star) */
	public int nIsNodeTriple;
	/** Number of URI nodes */
	public int nIsURI;
	/** Number of variable nodes */
	public int nIsVariable;

	/**
	 * No argument constructor initializes counts.
	 */
	public OntologyNodeTypeCounts() {
		nIsBlank = 0;
		nIsConcrete = 0;
		nIsExt = 0;
		nIsLiteral = 0;
		nIsNodeGraph = 0;
		nIsNodeTriple = 0;
		nIsURI = 0;
		nIsVariable = 0;
	}

	/**
	 * Count the types of the specified node.
	 *
	 * @param node Node for which types are counted
	 */
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

	/**
	 * Format a string suitable for printing to a table.
	 *
	 * @return Formatted string suitable for printing to a table.
	 */
	public String toString() {
		return String.format("%d,%d,%d,%d,%d,%d,%d,%d\n", nIsBlank, nIsConcrete, nIsExt, nIsLiteral, nIsNodeGraph,
				nIsNodeTriple, nIsURI, nIsVariable);
	}
}
