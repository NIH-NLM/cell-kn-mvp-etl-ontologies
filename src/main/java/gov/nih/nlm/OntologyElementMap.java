package gov.nih.nlm;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains ontology terms for all elements with a non-empty "about" attribute
 * and at least one "label" element.
 */
public class OntologyElementMap {

    /**
     * Ontology title
     */
    public String title;
    /**
     * Ontology description
     */
    public String description;
    /**
     * Ontology PURL
     */
    public URI purl;
    /**
     * Ontology version
     */
    public URI versionIRI;
    /**
     * Ontology root term
     */
    public URI root;
    /**
     * Unique ontology term ids
     */
    public Set<String> ids;
    /**
     * Mapping from ontology term to PURLs and labels
     */
    public Map<String, OntologyTerm> terms;

    public OntologyElementMap() {
        ids = new HashSet<>();
        terms = new HashMap<>();
    }

    /**
     * Contains ontology term PURL and label
     */
    public static class OntologyTerm {

        /**
         * Ontology term PURL
         */
        public URI purl;
        /**
         * Ontology term label
         */
        public String label;

        /**
         * Construct an OntologyTerm instance.
         *
         * @param purl  Ontology term PURL
         * @param label Ontology term label
         */
        public OntologyTerm(URI purl, String label) {
            this.purl = purl;
            this.label = label;
        }
    }
}
