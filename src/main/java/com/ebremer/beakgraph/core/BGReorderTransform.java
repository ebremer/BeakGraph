package com.ebremer.beakgraph.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;

/**
 * Selectivity-based join reordering tuned to BeakGraph's two indexes (GSPO + GPOS).
 *
 * <p>The query engine substitutes the incoming row's bindings into the BGP before calling this, so a
 * slot is "bound" when it is a concrete term or a variable already produced by an earlier triple in
 * the chosen order. Triples are greedily placed most-selective-first using the VoID per-predicate
 * counts. The BeakGraph-specific rule: only a bound <em>subject</em> (GSPO prefix) or bound
 * <em>predicate</em> (GPOS prefix) gives an index lookup - a pattern bound on the object alone has no
 * index and must scan, so it is penalised and deferred until chaining binds its subject or predicate.
 */
final class BGReorderTransform implements ReorderTransformation {

    private final VoidStats stats;

    BGReorderTransform(VoidStats stats) {
        this.stats = stats;
    }

    @Override
    public BasicPattern reorder(BasicPattern pattern) {
        return reorderIndexes(pattern).reorder(pattern);
    }

    @Override
    public ReorderProc reorderIndexes(BasicPattern pattern) {
        List<Triple> triples = pattern.getList();
        int n = triples.size();
        boolean[] used = new boolean[n];
        Set<Node> bound = new HashSet<>();      // variables bound by already-chosen triples
        List<Integer> order = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            int best = -1;
            double bestCost = Double.POSITIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                if (used[i]) {
                    continue;
                }
                double c = cost(triples.get(i), bound);
                if (c < bestCost) {
                    bestCost = c;
                    best = i;
                }
            }
            used[best] = true;
            order.add(best);
            bindVars(bound, triples.get(best));
        }
        return original -> {
            BasicPattern out = new BasicPattern();
            for (int idx : order) {
                out.add(original.get(idx));
            }
            return out;
        };
    }

    /** Estimated result/scan cost of evaluating {@code t} given the currently bound terms/variables. */
    private double cost(Triple t, Set<Node> bound) {
        boolean s = isBound(t.getSubject(), bound);
        boolean p = isBound(t.getPredicate(), bound);
        boolean o = isBound(t.getObject(), bound);
        double total = Math.max(1, stats.totalTriples());

        // Estimated matching cardinality via independent selectivity factors.
        double est = total;
        if (s) {
            est /= Math.max(1, stats.distinctSubjects());
        }
        if (o) {
            est /= Math.max(1, stats.distinctObjects());
        }
        if (p) {
            long cp = t.getPredicate().isConcrete() ? stats.predicateCount(t.getPredicate()) : -1L;
            if (cp >= 0) {
                est *= (cp / total);                                // exact fraction of triples with this predicate
            } else {
                est /= Math.max(1, stats.distinctPredicates());     // unknown/variable predicate: assume uniform
            }
        }
        est = Math.max(est, 1.0);

        // GSPO + GPOS only: a bound subject or predicate yields an index prefix. With neither, the
        // pattern (object-only, or fully unbound) cannot use an index and must scan the store, so add
        // that cost - this defers such triples until chaining binds their subject or predicate.
        if (!s && !p) {
            est += total;
        }
        return est;
    }

    private static boolean isBound(Node n, Set<Node> bound) {
        return n.isConcrete() || (n.isVariable() && bound.contains(n));
    }

    private static void bindVars(Set<Node> bound, Triple t) {
        if (t.getSubject().isVariable())   bound.add(t.getSubject());
        if (t.getPredicate().isVariable()) bound.add(t.getPredicate());
        addObjectVars(bound, t.getObject());
    }

    /**
     * Records the object position's produced variables: the object itself, or -
     * for a var-containing triple-term pattern - every variable embedded in it
     * (all depths). Without this a later triple joining on an embedded variable
     * was costed as if the join were free-floating (wrong order, not wrong
     * answers).
     */
    private static void addObjectVars(Set<Node> bound, Node o) {
        if (o.isVariable()) {
            bound.add(o);
            return;
        }
        if (o.isTripleTerm() && !o.isConcrete()) {
            Triple t = o.getTriple();
            addObjectVars(bound, t.getSubject());
            addObjectVars(bound, t.getPredicate());
            addObjectVars(bound, t.getObject());
        }
    }
}
