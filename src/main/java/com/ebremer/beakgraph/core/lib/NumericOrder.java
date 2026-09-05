package com.ebremer.beakgraph.core.lib;

import java.math.BigDecimal;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * Exact ordering of numeric literals across every XSD numeric datatype, and
 * the promotion neighbourhoods range pushdown must respect.
 * <p>
 * SPARQL compares mixed numeric types by <em>promotion</em>: a decimal
 * against a float is compared as floats, against a double as doubles. That
 * is lossy - distinct decimals collapse onto one float - and the dictionary
 * comparator used to mix that lossy order (decimal vs float) with the exact
 * order (decimal vs decimal) and a lexical tie-break, which is cyclic:
 * {@code "0.100000000099"^^decimal < "0.10000000010"^^decimal} exactly, both
 * tie with {@code "0.10000000005"^^float} under promotion, and the lexical
 * tie-break puts the float between them. A cyclic comparator makes sorts
 * input-order dependent and binary searches miss stored terms. Numbers are
 * therefore ordered by their EXACT value here: {@code -INF < finite < +INF
 * < NaN}, finite values as {@link BigDecimal} (float and double values
 * convert exactly). Value-equal terms fall to the exact-term tie-break.
 * <p>
 * The exact order disagrees with ARQ's promoted comparison only where
 * promotion loses information, and there ARQ's own {@code <} is not
 * transitive, so no total order can agree with it everywhere. Range pushdown
 * stays sound by widening its bounds to the promotion neighbourhood
 * ({@link #lowerEdge}, {@link #upperEdge}): rounding is monotone, so every
 * stored value ARQ accepts for {@code ?x >= c} has an exact value of at least
 * {@code lowerEdge(c)}, and the enclosing FILTER removes the extra candidates.
 */
public final class NumericOrder {

    private NumericOrder() {}

    /** {@code -INF} = 0, finite = 1, {@code +INF} = 2, {@code NaN} = 3. */
    public static int rank(NodeValue nv) {
        if (isFloatKind(nv) || isDoubleKind(nv)) {
            double d = isFloatKind(nv) ? nv.getFloat() : nv.getDouble();
            if (Double.isNaN(d)) return 3;
            if (d == Double.POSITIVE_INFINITY) return 2;
            if (d == Double.NEGATIVE_INFINITY) return 0;
        }
        return 1;
    }

    public static boolean isFinite(NodeValue nv) {
        return rank(nv) == 1;
    }

    /** The concrete kind: a NodeValueInteger answers true to isDecimal/isFloat/isDouble as well. */
    static boolean isFloatKind(NodeValue nv) {
        return nv.isFloat() && !nv.isDecimal();
    }

    static boolean isDoubleKind(NodeValue nv) {
        return nv.isDouble() && !nv.isFloat();
    }

    /** The exact value of a finite number. */
    public static BigDecimal exact(NodeValue nv) {
        if (nv.isInteger()) return new BigDecimal(nv.getInteger());
        if (nv.isDecimal()) return nv.getDecimal();
        if (isFloatKind(nv)) return new BigDecimal((double) nv.getFloat());
        return new BigDecimal(nv.getDouble());
    }

    /** Exact comparison of two numbers; 0 for value-equal terms of any datatypes. */
    public static int compare(NodeValue a, NodeValue b) {
        int ra = rank(a);
        int rb = rank(b);
        if (ra != rb) return Integer.compare(ra, rb);
        if (ra != 1) return 0;
        return exact(a).compareTo(exact(b));
    }

    /**
     * The smallest exact value a stored number can have while ARQ still
     * compares it as {@code >= c} (or equal to c). A float constant is met by
     * every decimal or integer that rounds to it, so the edge is the midpoint
     * below the float; likewise for a double. A decimal or integer constant
     * is compared as a float against float rows and as a double against
     * double rows - the edges {@code (float) c} and {@code (double) c} apply
     * only when the section holds such rows, which keeps integer hints (the
     * spatial index's Hilbert ranges) exact where no floats are stored.
     */
    public static BigDecimal lowerEdge(NodeValue c, boolean floatRows, boolean doubleRows) {
        BigDecimal x = exact(c);
        BigDecimal low = x;
        if (isFloatKind(c)) {
            low = min(low, midpointBelow(c.getFloat()));
        } else if (isDoubleKind(c)) {
            low = min(low, midpointBelow(c.getDouble()));
        } else {
            if (floatRows) low = min(low, new BigDecimal((double) x.floatValue()));
            if (doubleRows) low = min(low, new BigDecimal(x.doubleValue()));
        }
        return low;
    }

    /** The mirror of {@link #lowerEdge}: the largest exact value ARQ can still compare as {@code <= c}. */
    public static BigDecimal upperEdge(NodeValue c, boolean floatRows, boolean doubleRows) {
        BigDecimal x = exact(c);
        BigDecimal high = x;
        if (isFloatKind(c)) {
            high = max(high, midpointAbove(c.getFloat()));
        } else if (isDoubleKind(c)) {
            high = max(high, midpointAbove(c.getDouble()));
        } else {
            if (floatRows) high = max(high, new BigDecimal((double) x.floatValue()));
            if (doubleRows) high = max(high, new BigDecimal(x.doubleValue()));
        }
        return high;
    }

    private static BigDecimal midpointBelow(float f) {
        float prev = Math.nextDown(f);
        if (Float.isInfinite(prev)) return new BigDecimal((double) f);
        return new BigDecimal((double) prev).add(new BigDecimal((double) f)).divide(BigDecimal.valueOf(2));
    }

    private static BigDecimal midpointAbove(float f) {
        float next = Math.nextUp(f);
        if (Float.isInfinite(next)) return new BigDecimal((double) f);
        return new BigDecimal((double) f).add(new BigDecimal((double) next)).divide(BigDecimal.valueOf(2));
    }

    private static BigDecimal midpointBelow(double d) {
        double prev = Math.nextDown(d);
        if (Double.isInfinite(prev)) return new BigDecimal(d);
        return new BigDecimal(prev).add(new BigDecimal(d)).divide(BigDecimal.valueOf(2));
    }

    private static BigDecimal midpointAbove(double d) {
        double next = Math.nextUp(d);
        if (Double.isInfinite(next)) return new BigDecimal(d);
        return new BigDecimal(d).add(new BigDecimal(next)).divide(BigDecimal.valueOf(2));
    }

    private static BigDecimal min(BigDecimal a, BigDecimal b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static BigDecimal max(BigDecimal a, BigDecimal b) {
        return a.compareTo(b) >= 0 ? a : b;
    }
}
