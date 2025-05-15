package com.ebremer.beakgraph.core.lib;

import java.util.Comparator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.NodeCmp;

/**
 * A comparator for RDF nodes that extends comparison to include graph nodes (URIs or null for the default graph).
 * * The sorting order is as follows:
 * 1. Graph (Default Graph < Named Graphs)
 * 2. Subject (delegated to NodeCmp.compareRDFTerms)
 * 3. Predicate (delegated to NodeCmp.compareRDFTerms)
 * 4. Object (delegated to NodeCmp.compareRDFTerms)
 */
public class NodeComparator implements Comparator<Node> {

    // Singleton instance to avoid creating unnecessary objects during large sorts
    public static final NodeComparator INSTANCE = new NodeComparator();

    // Private constructor to enforce singleton usage
    private NodeComparator() {}

    @Override
    public int compare(Node n1, Node n2) {
        // Handle comparison involving the default graph.
        boolean n1isDefaultGraph = isDefaultGraph(n1);
        boolean n2isDefaultGraph = isDefaultGraph(n2);

        if (n1isDefaultGraph && n2isDefaultGraph) {
            return 0; // Both are default graphs, hence equal.
        } else if (n1isDefaultGraph) {
            return -1; // The default graph (n1) comes before any named graph (n2).
        } else if (n2isDefaultGraph) {
            return 1; // A named graph (n1) comes after the default graph (n2).
        }

        // For all other nodes delegate to Jena's standard RDF term comparison.
        return NodeCmp.compareRDFTerms(n1, n2);
    }

    /**
     * Checks if a given node represents the default graph.
     * Handles null, Quad.defaultGraphNodeGenerated, and Quad.defaultGraphIRI.
     */
    private boolean isDefaultGraph(Node n) {
        return n == null 
            || n.equals(Quad.defaultGraphNodeGenerated) 
            || n.equals(Quad.defaultGraphIRI);
    }
}