package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.Dictionary;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * Locates the contiguous id range of dictionary entries that compare
 * <em>value-equal</em> to a filter constant.
 * <p>
 * The dictionaries order literals by value first (NodeValue.compareAlways) and
 * break value ties on the exact RDF term, so value-equal but term-distinct
 * literals ("5"^^xsd:int, "5"^^xsd:integer, "5.0"^^xsd:double) occupy a run of
 * adjacent ids - and an exact-term binary search can land anywhere inside that
 * run. Range-filter pushdown (FILTER(?o >= 5)) must therefore snap its bound to
 * the edges of the whole cluster: with [lo, hi] from {@link #of},
 * <pre>
 *   &gt;   -&gt; min = hi + 1        &gt;=  -&gt; min = lo
 *   &lt;   -&gt; max = lo - 1        &lt;=  -&gt; max = hi
 * </pre>
 * For a constant with no value-equal entries (including all non-literals) the
 * cluster is empty ({@code hi == lo - 1}, both at the insertion point) and the
 * formulas reduce exactly to the plain insertion-point bounds.
 */
final class ValueCluster {

    private ValueCluster() {}

    /**
     * Returns {@code {lo, hi}}: the inclusive 1-based id range of entries in
     * {@code dict} that are value-equal to {@code value}. Empty when {@code hi < lo}.
     */
    static long[] of(Dictionary dict, Node value) {
        long raw = dict.search(value);
        boolean found = raw >= 0;
        long pos = found ? raw : (-raw - 1);
        long lo = pos;
        long hi = found ? pos : pos - 1;
        if (!value.isLiteral()) {
            // URIs/bnodes are ordered by term, never by value: no cluster to grow.
            return new long[]{lo, hi};
        }
        long n = dict.getNumberOfNodes();
        while (hi + 1 <= n && valueEqual(extractOrNull(dict, hi + 1), value)) hi++;
        while (lo - 1 >= 1 && valueEqual(extractOrNull(dict, lo - 1), value)) lo--;
        return new long[]{lo, hi};
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
