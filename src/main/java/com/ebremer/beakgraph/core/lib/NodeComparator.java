package com.ebremer.beakgraph.core.lib;

import java.util.Comparator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.NodeCmp;

/**
 * Enforces a strict Total Ordering of RDF Nodes.
 * CRITICAL for HDF5 Binary Search: 
 * Ensures the sorting order perfectly matches the Monolithic Dictionary ID assignments:
 * 1. Default Graph
 * 2. Blank Nodes
 * 3. URIs
 * 4. Literals (Sorted numerically/temporally/lexicographically by value)
 */
public class NodeComparator implements Comparator<Node> {

    public static final NodeComparator INSTANCE = new NodeComparator();

    private NodeComparator() {}

    @Override
    public int compare(Node n1, Node n2) {
        // 1. Handle the Default Graph first (Always ID 0 or implicitly lowest)
        boolean n1isDefault = isDefaultGraph(n1);
        boolean n2isDefault = isDefaultGraph(n2);
        
        if (n1isDefault && n2isDefault) return 0;
        if (n1isDefault) return -1;
        if (n2isDefault) return 1;

        // 2. Enforce RDF Term Macro-Ordering (BNode < URI < Literal)
        // This ensures the sorted array perfectly aligns with how IDs are chunked
        int type1 = getMacroType(n1);
        int type2 = getMacroType(n2);
        
        if (type1 != type2) {
            return Integer.compare(type1, type2);
        }

        // 3. Both nodes are the SAME RDF Term Type.
        // If they are both Literals, we must sort by actual Value (e.g. 2 < 10), not String ("10" < "2")
        if (n1.isLiteral() && n2.isLiteral()) {
            try {
                NodeValue nv1 = NodeValue.makeNode(n1);
                NodeValue nv2 = NodeValue.makeNode(n2);
                
                // compareAlways provides a strict SPARQL "ORDER BY" total ordering, 
                // handling mixed datatypes safely without throwing exceptions.
                return NodeValue.compareAlways(nv1, nv2);
            } catch (Exception e) {
                // Absolute fallback if Jena fails to parse a highly malformed literal
                return NodeCmp.compareRDFTerms(n1, n2);
            }
        }

        // 4. If they are both URIs or both BNodes, fallback to Jena's standard lexicographical sort
        return NodeCmp.compareRDFTerms(n1, n2);
    }

    /**
     * Maps a Node to an integer rank to enforce BNode < URI < Literal.
     */
    private int getMacroType(Node n) {
        if (n.isBlank()) return 1;
        if (n.isURI()) return 2;
        if (n.isLiteral()) return 3;
        // Should never happen in valid RDF, but safe fallback
        return 4; 
    }

    private boolean isDefaultGraph(Node n) {
        if (n == null) return true;
        return n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated);
    }
}
