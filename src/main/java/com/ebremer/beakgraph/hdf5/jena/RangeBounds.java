package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.ExprList;

/**
 * The id ranges a pattern's FILTER hints narrow each position to: inclusive
 * signed [min, max] for the subject, predicate and object, resolved ONCE per
 * (filter, pattern positions) against a dictionary and shared by every
 * iterator that answers the pattern.
 * <p>
 * Each of the four index iterators used to carry its own copy of the
 * analyzeFilters / applyBound logic, and the copies had drifted: three
 * compared with {@code Long.compareUnsigned}, the fourth signed, so a store
 * without a literals section (where the object dictionary answers -1) took
 * different paths per route (BG-301). Every copy also ran the dictionary
 * probes ({@link ValueCluster#of}: a search plus at least two term extracts)
 * in the iterator constructor - once per input binding of a join step, and
 * once per chunk of a parallel scan (BG-62, BG-451). The resolution now lives
 * here, in signed arithmetic with every lower edge clamped to the position's
 * floor, and is memoised per dictionary under a structural key: the same
 * FILTER over the same pattern shape resolves once per store.
 */
public final class RangeBounds {

    /** No narrowing: the iterators' own initial ranges. */
    public static final RangeBounds NONE = new RangeBounds(1, Long.MAX_VALUE, 0, Long.MAX_VALUE, 0, Long.MAX_VALUE, 0);

    public final long minS, maxS, minP, maxP, minO, maxO;
    /** How many FILTER bounds narrowed these positions (0 for NONE and for filters with no usable comparison). */
    final int hints;

    private RangeBounds(long minS, long maxS, long minP, long maxP, long minO, long maxO, int hints) {
        this.minS = minS; this.maxS = maxS;
        this.minP = minP; this.maxP = maxP;
        this.minO = minO; this.maxO = maxO;
        this.hints = hints;
    }

    /** Memo key: the filter (ExprList equality is structural) and which variable sits in each position. */
    record Key(ExprList filter, String subject, String predicate, String object) {}

    private static String varName(Node n) {
        return n != null && n.isVariable() ? n.getName() : null;
    }

    /** The bounds for {@code quad}'s positions under {@code filter}, memoised on {@code dict}. */
    public static RangeBounds of(ExprList filter, Quad quad, PositionalDictionaryReader dict) {
        if (filter == null || filter.isEmpty()) {
            return NONE;
        }
        Key key = new Key(filter, varName(quad.getSubject()), varName(quad.getPredicate()), varName(quad.getObject()));
        RangeBounds bounds = (RangeBounds) dict.queryMemo().get(key, k -> resolve(filter, quad, dict));
        if (bounds.hints > 0) {
            FilterBounds.HITS.addAndGet(bounds.hints); // every iterator that receives a bound counts, memo hit or not
        }
        return bounds;
    }

    /** Resolves without the memo (tests, and the memo's loader). */
    static RangeBounds resolve(ExprList filter, Quad quad, PositionalDictionaryReader dict) {
        long[] b = {1, Long.MAX_VALUE, 0, Long.MAX_VALUE, 0, Long.MAX_VALUE};
        int[] hints = {0};
        FilterBounds.scan(filter, (var, op, value) -> {
            int pos;
            Dictionary d;
            if (var.equals(quad.getSubject())) {
                pos = 0; d = dict.getSubjects();
            } else if (var.equals(quad.getPredicate())) {
                pos = 2; d = dict.getPredicates();
            } else if (var.equals(quad.getObject())) {
                pos = 4; d = dict.getObjects();
            } else {
                return;
            }
            // Snap the bound to the edges of the whole value-equal cluster:
            // value-equal but term-distinct literals ("5"^^xsd:int vs
            // "5"^^xsd:integer) occupy adjacent distinct ids, and the raw
            // exact-term insertion point can land inside that cluster, silently
            // dropping qualifying boundary rows. A lower edge below the
            // position's floor (an absent section answers -1) clamps to it.
            hints[0]++;
            ValueCluster.Bounds c = ValueCluster.of(d, value);
            long floor = b[pos];
            switch (op) {
                case ">" -> b[pos] = Math.max(floor, c.firstGT());
                case ">=" -> b[pos] = Math.max(floor, c.firstGE());
                case "<" -> b[pos + 1] = Math.min(b[pos + 1], c.lastLT());
                case "<=" -> b[pos + 1] = Math.min(b[pos + 1], c.lastLE());
                default -> { }
            }
            if (pos == 4 && value.isLiteral()) {
                // A literal constant never compares with an IRI or blank node (the
                // FILTER fails with a type error), so only the literal ids - those
                // past the entity block - can qualify at the object position: a
                // "<" bound no longer walks every entity object, and an all-IRI
                // store answers empty for either direction (BG-351).
                b[4] = Math.max(b[4], dict.getSubjects().getNumberOfNodes() + 1);
            }
        });
        return new RangeBounds(b[0], b[1], b[2], b[3], b[4], b[5], hints[0]);
    }

    @Override
    public String toString() {
        return "RangeBounds[S " + minS + ".." + maxS + ", P " + minP + ".." + maxP + ", O " + minO + ".." + maxO + "]";
    }
}
