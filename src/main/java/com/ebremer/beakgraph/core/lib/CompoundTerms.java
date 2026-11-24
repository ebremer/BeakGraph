package com.ebremer.beakgraph.core.lib;

import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;

/**
 * Terms made of terms: RDF 1.2 triple terms and RDF-array sequences, either
 * nested in the other (RDFARRY.md §3.2). Every per-term normalization at
 * ingest (relative IRIs, blank-node scoping, numeric canonicalization) and the
 * query-time IRI resolver rebuild a term through {@link #map}, so no engine
 * walks one kind of compound term deeper than another (the mirror-topology
 * hazard of CHANGELOG.md "Format v5 design notes").
 */
public final class CompoundTerms {

    private CompoundTerms() {}

    /** Whether {@code n} is made of other terms: a triple term or a sequence. */
    public static boolean isCompound(Node n) {
        return n != null && (n.isTripleTerm() || n.isNodeSeq());
    }

    /**
     * Rebuilds {@code n} by applying {@code f} to every leaf (an IRI, blank
     * node or literal) at every depth: the components of a triple term and the
     * elements of a sequence, recursing into nested ones. A slot is not a term
     * and is kept as it is. Returns the SAME instance when no leaf changed, so
     * callers can skip rebuilding a quad on identity.
     *
     * <p>{@code f} is never handed a compound term or a slot: nesting is the
     * recursion's job, so the per-kind transforms plug in unchanged. A sequence
     * nested deeper than {@link SeqTerms#MAX_DEPTH} is refused here, before
     * any engine recurses through it.
     */
    public static Node map(Node n, UnaryOperator<Node> f) {
        return map(n, f, 0);
    }

    private static Node map(Node n, UnaryOperator<Node> f, int depth) {
        if (n.isTripleTerm()) {
            Triple t = n.getTriple();
            Node s = t.getSubject();
            Node p = t.getPredicate();
            Node o = t.getObject();
            Node s2 = isCompound(s) ? map(s, f, depth + 1) : f.apply(s);
            Node p2 = isCompound(p) ? map(p, f, depth + 1) : f.apply(p);
            Node o2 = isCompound(o) ? map(o, f, depth + 1) : f.apply(o);
            if (s2 == s && p2 == p && o2 == o) {
                return n;
            }
            return NodeFactory.createTripleTerm(s2, p2, o2);
        }
        if (n.isNodeSeq()) {
            requireDepth(n, depth);
            List<Node> elements = n.getSeq();
            Node[] changed = null;
            for (int i = 0, size = elements.size(); i < size; i++) {
                Node e = elements.get(i);
                Node e2;
                if (e.isNodeUndef()) {
                    e2 = e;
                } else if (isCompound(e)) {
                    e2 = map(e, f, depth + 1);
                } else {
                    e2 = f.apply(e);
                }
                if (e2 != e && changed == null) {
                    changed = elements.toArray(new Node[0]);
                }
                if (changed != null) {
                    changed[i] = e2;
                }
            }
            return (changed == null) ? n : SeqTerms.create(Arrays.asList(changed));
        }
        return f.apply(n);
    }

    /** Refuses a sequence at nesting depth {@code depth} (0 for a top-level one) past {@link SeqTerms#MAX_DEPTH}. */
    public static void requireDepth(Node seq, int depth) {
        if (depth >= SeqTerms.MAX_DEPTH) {
            throw new IllegalStateException("Sequence nested deeper than " + SeqTerms.MAX_DEPTH
                    + " levels (SPECIFICATIONS.md §9.5)");
        }
    }
}
