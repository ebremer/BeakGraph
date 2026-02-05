package com.ebremer.beakgraph.core.lib;

import java.util.Comparator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.NodeCmp;

public class NodeComparator implements Comparator<Node> {

    public static final NodeComparator INSTANCE = new NodeComparator();

    private NodeComparator() {}

    @Override
    public int compare(Node n1, Node n2) {
        // 1. Handle comparison involving the default graph first
        boolean n1isDefaultGraph = isDefaultGraph(n1);
        boolean n2isDefaultGraph = isDefaultGraph(n2);
        
        if (n1isDefaultGraph && n2isDefaultGraph) return 0;
        if (n1isDefaultGraph) return -1;
        if (n2isDefaultGraph) return 1;

        // 2. Value-based Numeric Comparison
        if (n1.isLiteral() && n2.isLiteral()) {
            NodeValue nv1 = NodeValue.makeNode(n1);
            NodeValue nv2 = NodeValue.makeNode(n2);

            // We only use NodeValue comparison if BOTH are numeric.
            // This prevents mixing numeric logic with strings or dates.
            if (nv1.isNumber() && nv2.isNumber()) {
                try {
                    return NodeValue.compare(nv1, nv2);
                } catch (Exception e) {
                    // Fall through if Jena fails to compare them for any reason
                }
            }
            
            // If one is numeric and the other isn't, we need to be stable.
            // Let's decide Numerics always come BEFORE other literals.
            if (nv1.isNumber() && !nv2.isNumber()) return -1;
            if (!nv1.isNumber() && nv2.isNumber()) return 1;
        }

        // 3. Fallback to Jena's standard stable RDF term comparison
        return NodeCmp.compareRDFTerms(n1, n2);
    }

    private boolean isDefaultGraph(Node n) {
        if (n == null) return true;
        return n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated);
    }
}