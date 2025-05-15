package com.ebremer.beakgraph.hdf5.util;

import com.ebremer.beakgraph.core.lib.NodeComparator;
import java.util.Comparator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class QuadComparator implements Comparator<Quad> {

    @Override
    public int compare(Quad q1, Quad q2) {
        // Step 1: Compare graph nodes
        Node g1 = q1.getGraph();
        Node g2 = q2.getGraph();
        int cmp = NodeComparator.INSTANCE.compare(g1, g2);
        if (cmp != 0) {
            return cmp;
        }

        // Step 2: Compare subject nodes
        Node s1 = q1.getSubject();
        Node s2 = q2.getSubject();
        cmp = NodeComparator.INSTANCE.compare(s1, s2);
        if (cmp != 0) {
            return cmp;
        }

        // Step 3: Compare predicate nodes
        Node p1 = q1.getPredicate();
        Node p2 = q2.getPredicate();
        cmp = NodeComparator.INSTANCE.compare(p1, p2);
        if (cmp != 0) {
            return cmp;
        }

        // Step 4: Compare object nodes
        Node o1 = q1.getObject();
        Node o2 = q2.getObject();
        return NodeComparator.INSTANCE.compare(o1, o2);
    }
}
