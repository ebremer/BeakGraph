package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.lib.NumericOrder;
import java.math.BigDecimal;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * Turns a range-filter constant into id bounds for the index iterators.
 * <p>
 * The dictionaries order literals by value first and break value ties on the
 * exact RDF term, so value-equal but term-distinct literals ("5"^^xsd:int,
 * "5"^^xsd:integer, "5.0"^^xsd:double) occupy a run of adjacent ids - and an
 * exact-term binary search can land anywhere inside that run. Range-filter
 * pushdown must therefore snap its bound to the edges of the whole cluster:
 * {@link Bounds} gives, per comparison, the first id that may satisfy
 * {@code >} / {@code >=} and the last that may satisfy {@code <} / {@code <=}.
 * <p>
 * For a non-numeric constant the cluster is the run of ARQ-value-equal
 * entries around it (empty for a constant with no value-equal entries,
 * including all non-literals, where the bounds reduce to the insertion
 * point). For a finite NUMERIC constant the dictionary orders by exact value
 * ({@link NumericOrder}) while ARQ compares mixed datatypes by lossy
 * promotion, so the candidate interval is widened to the promotion
 * neighbourhood: every stored value ARQ accepts for {@code ?x >= c} has an
 * exact value of at least {@code NumericOrder.lowerEdge(c)}, and every one it
 * accepts for {@code ?x > c} too (the cluster itself is scanned rather than
 * skipped). The enclosing FILTER removes the extra candidates; recall is
 * never lost.
 * <p>
 * Growing over ADJACENT ids presumes value-equal entries are adjacent, which
 * holds for the value-ordered spaces but not for composite (cdt) or
 * language-tagged literals, whose dictionary order is lexical / exact-tag.
 * {@link FilterBounds#orderAgreesWithArq} keeps such constants from reaching
 * the pushdown at all.
 */
final class ValueCluster {

    private ValueCluster() {}

    /** Id bounds for the four ordering comparisons against one constant. */
    record Bounds(long firstGE, long firstGT, long lastLT, long lastLE) {}

    /** Bounds for {@code value} against {@code dict}. */
    static Bounds of(Dictionary dict, Node value) {
        if (value.isLiteral()) {
            NodeValue nv = nodeValueOrNull(value);
            if (nv != null && nv.isNumber() && NumericOrder.isFinite(nv)) {
                return numericBounds(dict, nv);
            }
        }
        long[] c = cluster(dict, value);
        return new Bounds(c[0], c[1] + 1, c[0] - 1, c[1]);
    }

    /**
     * Returns {@code {lo, hi}}: the inclusive 1-based id range of entries in
     * {@code dict} that are value-equal to {@code value}. Empty when {@code hi < lo}.
     */
    static long[] cluster(Dictionary dict, Node value) {
        long raw = dict.search(value);
        boolean found = raw >= 0;
        long pos = found ? raw : (-raw - 1);
        long lo = pos;
        long hi = found ? pos : pos - 1;
        if (!value.isLiteral()) {
            // URIs/bnodes are ordered by term, never by value: no cluster to grow.
            return new long[]{lo, hi};
        }
        // The ids are provably valid (1..n), so extract() can only fail for a
        // real reason - an HTTP range read, a corrupt block, a decompression
        // error. Such a failure must surface: swallowing it (the old
        // extractOrNull) stopped the walk early and snapped the bound INSIDE
        // the value-equal cluster, silently dropping rows at the boundary -
        // the very loss this class exists to prevent (BG-68).
        long n = dict.getNumberOfNodes();
        // Ids are 1-based: an insertion point of 0 (an absent section answers -1)
        // must not probe id 0 - extract() now fails loudly for it (BG-68).
        while (hi + 1 >= 1 && hi + 1 <= n && valueEqual(dict.extract(hi + 1), value)) hi++;
        while (lo - 1 >= 1 && valueEqual(dict.extract(lo - 1), value)) lo--;
        return new long[]{lo, hi};
    }

    private static Bounds numericBounds(Dictionary dict, NodeValue c) {
        boolean floats = dict.hasFloatLiterals();
        boolean doubles = dict.hasDoubleLiterals();
        long first = firstIdAtLeast(dict, NumericOrder.lowerEdge(c, floats, doubles));
        long last = lastIdAtMost(dict, NumericOrder.upperEdge(c, floats, doubles));
        return new Bounds(first, first, last, last);
    }

    /** A decimal literal with exactly the value {@code v}, as a probe for the dictionary's binary search. */
    private static Node probe(BigDecimal v) {
        return NodeFactory.createLiteralDT(v.toPlainString(), XSDDatatype.XSDdecimal);
    }

    /** First id whose exact numeric value is {@code >= v} (ids are 1-based). */
    private static long firstIdAtLeast(Dictionary dict, BigDecimal v) {
        long raw = dict.search(probe(v));
        long pos = raw >= 0 ? raw : (-raw - 1);
        // Value-equal terms sort among themselves by lexical form, so the
        // probe may land inside such a run: back up to its first member.
        while (pos - 1 >= 1 && sameValue(dict, pos - 1, v)) pos--;
        return pos;
    }

    /** Last id whose exact numeric value is {@code <= v}. */
    private static long lastIdAtMost(Dictionary dict, BigDecimal v) {
        long raw = dict.search(probe(v));
        long last = raw >= 0 ? raw : (-raw - 1) - 1;
        long n = dict.getNumberOfNodes();
        while (last + 1 >= 1 && last + 1 <= n && sameValue(dict, last + 1, v)) last++;
        return last;
    }

    private static boolean sameValue(Dictionary dict, long id, BigDecimal v) {
        Node n = dict.extract(id); // valid id; a failure is real and must surface (BG-68)
        if (n == null || !n.isLiteral()) return false;
        NodeValue nv = nodeValueOrNull(n);
        return nv != null && nv.isNumber() && NumericOrder.isFinite(nv) && NumericOrder.exact(nv).compareTo(v) == 0;
    }

    private static NodeValue nodeValueOrNull(Node n) {
        try {
            return NodeValue.makeNode(n);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Node extractOrNull(Dictionary dict, long id) {
        try {
            return dict.extract(id);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static boolean valueEqual(Node a, Node b) {
        if (a == null || !a.isLiteral()) return false;
        try {
            // Value-only comparison. compareAlways would be wrong here: it never
            // answers "equal" for distinct terms (it breaks value ties on the term),
            // which is exactly the distinction this cluster must ignore. Throws for
            // non-comparable values (e.g. string vs number) - not value-equal.
            return NodeValue.compare(NodeValue.makeNode(a), NodeValue.makeNode(b)) == Expr.CMP_EQUAL;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
