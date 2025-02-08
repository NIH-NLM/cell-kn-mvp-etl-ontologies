package gov.nih.nlm;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.CollectorStreamTriples;

public class OntologyParser {

	public static Path usrDir = Paths.get(System.getProperty("user.dir"));
	public static Path oboDir = usrDir.resolve("data/obo");

	/**
	 * Collect all triples into sets containing triples with:
	 * <ul>
	 * <li>Filled subject and object</li>
	 * <li>Blank subject and filled object</li>
	 * <li>Filled subject and blank object</li>
	 * <li>Blank subject and object</li>
	 * </ul>
	 * 
	 * @param oboFNm
	 * @param tripleTypeSets
	 */
	public static void populateTripleTypeSets(String oboFNm, TripleTypeSets tripleTypeSets) {
		Path oboPth = oboDir.resolve(oboFNm);
		CollectorStreamTriples inputStream = new CollectorStreamTriples();
		RDFParser.source(oboPth).parse(inputStream);
		int nTriples = 0;
		for (Triple triple : inputStream.getCollected()) {
			nTriples++;
			Node s = triple.getSubject();
			Node p = triple.getPredicate();
			Node o = triple.getObject();
			if (!s.isBlank() && !o.isBlank()) {
				tripleTypeSets.fNodeTriples.add(triple);
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
				tripleTypeSets.bBNodeTriples.add(triple);
			}
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
	}

	/**
	 * Both blank subject and object triples are added to the subject blank node
	 * triple sets if the blank subject and object are contained in the subject
	 * blank node triple sets.
	 * 
	 * @param tripleTypeSets
	 */
	public static void addLtoSBnodeTriples(TripleTypeSets tripleTypeSets) {
		for (Triple triple : tripleTypeSets.bBNodeTriples.toArray(new Triple[0])) {
			Node s = triple.getSubject();
			Node o = triple.getObject();
			if (tripleTypeSets.sBNodeTriples.containsKey(s) && tripleTypeSets.sBNodeTriples.containsKey(o)) {
				tripleTypeSets.lBNodeTriples.add(triple);
				tripleTypeSets.sBNodeTriples.get(s).add(triple);
				tripleTypeSets.sBNodeTriples.get(o).add(triple);
			}
		}
		tripleTypeSets.bBNodeTriples.removeAll(tripleTypeSets.lBNodeTriples);
	}

	public static void flattenAxiomTriples(TripleTypeSets tripleTypeSets, Node key) {
		// TODO: Flatten axiom triples then add to filledTriples
		tripleTypeSets.sBNodeTriples.remove(key);
	}

	public static void flattenRestrictionTriples(TripleTypeSets tripleTypeSets, Node key) {
		// TODO: Flatten restriction triples then add to filledTriples
		tripleTypeSets.sBNodeTriples.remove(key);
	}

	public static void main(String[] args) {

		TripleTypeSets tripleTypeSets = new TripleTypeSets();
		populateTripleTypeSets("cl.owl", tripleTypeSets);
		moveOtoSBNodeTriples(tripleTypeSets);
		addLtoSBnodeTriples(tripleTypeSets);

		Node axiom = NodeFactory.createURI("http://www.w3.org/2002/07/owl#Axiom");
		Node restriction = NodeFactory.createURI("http://www.w3.org/2002/07/owl#Restriction");
		for (Node key : tripleTypeSets.sBNodeTriples.keySet().toArray(new Node[0])) {
			Node o = tripleTypeSets.sBNodeTriples.get(key).getFirst().getObject();
			if (o.equals(axiom)) {
				flattenAxiomTriples(tripleTypeSets, key);
			} else if (o.equals(restriction)) {
				flattenRestrictionTriples(tripleTypeSets, key);
			}
		}

//		for (Triple triple : lBlankTriples.toArray(new Triple[0])) {
//			Node s = triple.getSubject();
//			Node o = triple.getObject();
//			ArrayList<Triple> sTriples = sBlankNodeTriples.get(s);
//			ArrayList<Triple> oTriples = sBlankNodeTriples.get(o);
//		}
	}
}
