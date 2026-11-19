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
 *
 * <p>Exact counts, where the graphs a query reads can give them ({@link Counts}): a pattern whose
 * predicate is a term and whose subject is not yet bound - {@code ?s <p> <o>} or {@code ?s <p> ?o} -
 * is costed at its exact number of matches in those graphs, read from GPOS. The estimate above
 * divides by the store's distinct objects, which in a large store puts nearly every
 * {@code ?s <p> <o>} below one match; clamped to 1, they all tied, and the order they were written
 * in decided: {@code ?fac rdf:type fio:Facility} (4.86 M matches) ran before
 * {@code ?fac fio:ofIndustry naics:NAICS-562212} (14,306), and the query timed out.
 */
final class BGReorderTransform implements ReorderTransformation {

    /**
     * Exact matches of {@code ?s <predicate> <object>} - or {@code ?s <predicate> ?o} when
     * {@code object} is null - in the graphs a query reads; -1 when they cannot be counted.
     */
    @FunctionalInterface
    interface Counts {
        long matches(Node predicate, Node object);
    }

    private final VoidStats stats;
    private final Counts counts;

    BGReorderTransform(VoidStats stats) {
        this(stats, null);
    }

    BGReorderTransform(VoidStats stats, Counts counts) {
        this.stats = stats;
        this.counts = counts;
    }

    /** The dataset statistics this transform costs by (shared by every view of a store). */
    VoidStats stats() {
        return stats;
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
        long[] exact = exactCounts(triples);
        for (int k = 0; k < n; k++) {
            int best = -1;
            double bestCost = Double.POSITIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                if (used[i]) {
                    continue;
                }
                double c = cost(triples.get(i), bound, exact[i]);
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

    /**
     * Each triple's exact matches as {@link Counts} gives them - for a predicate that is a term,
     * with the object when it is one too - or -1: once per triple and reorder, not per candidate
     * order.
     */
    private long[] exactCounts(List<Triple> triples) {
        long[] exact = new long[triples.size()];
        for (int i = 0; i < exact.length; i++) {
            Triple t = triples.get(i);
            exact[i] = -1;
            // BeakGraph.BOUND is a variable bound earlier, value unknown: no term to count.
            if (counts != null && t.getPredicate().isConcrete() && !t.getSubject().isConcrete()
                    && !BeakGraph.BOUND.equals(t.getPredicate())) {
                Node o = t.getObject();
                if ((o.isConcrete() && !BeakGraph.BOUND.equals(o)) || o.isVariable()) {
                    exact[i] = counts.matches(t.getPredicate(), o.isConcrete() ? o : null);
                }
            }
        }
        return exact;
    }

    /**
     * Estimated result/scan cost of evaluating {@code t} given the currently bound terms/variables,
     * and {@code exact}, its exact matches when its subject is unbound (or -1).
     */
    private double cost(Triple t, Set<Node> bound, long exact) {
        boolean s = isBound(t.getSubject(), bound);
        boolean p = isBound(t.getPredicate(), bound);
        boolean o = isBound(t.getObject(), bound);
        // The exact count is the pattern's own matches: it stands for the cost only while nothing
        // chosen earlier binds its subject or (a variable) object, which would make it a lookup
        // per incoming row instead.
        if (exact >= 0 && !s && (t.getObject().isConcrete() || !o)) {
            return exact;
        }
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
