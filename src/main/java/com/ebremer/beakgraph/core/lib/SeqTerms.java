package com.ebremer.beakgraph.core.lib;

import java.util.List;
import java.util.RandomAccess;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 * BeakGraph's one door to RDF-array sequences ({@code ( e1 e2 ... )}) and
 * their empty slots ({@code undef}), the terms of Jena's {@code rdf-array}
 * branch (RDFARRY.md §3.1). Everything else in BeakGraph asks this class, so a
 * rename upstream, or the terms' arrival in stock Jena, changes one file.
 *
 * <p>A sequence is an RDF term: it may stand as an object, or as an element of
 * a sequence, and nowhere else (rdf-array's data model of 2026-11-03;
 * SPECIFICATIONS.md §9.5). A slot is not a term: it stands only as an element,
 * and counts toward the length.
 */
public final class SeqTerms {

    /**
     * The deepest nesting of sequences a store accepts (SPECIFICATIONS.md
     * §9.5): 1,000 levels. Ingest, extract, compare and verify recurse through
     * nested sequences, and a bound turns a stack overflow deep in a parallel
     * build into a plain refusal at ingest.
     */
    public static final int MAX_DEPTH = 1000;

    /** The one empty slot. */
    public static final Node UNDEF = Node.UNDEF;

    private SeqTerms() {}

    /** Whether {@code n} is a sequence. */
    public static boolean isSeq(Node n) {
        return n != null && n.isNodeSeq();
    }

    /** Whether {@code n} is the empty slot. */
    public static boolean isUndef(Node n) {
        return n != null && n.isNodeUndef();
    }

    /** The elements of sequence {@code seq}, in order; a slot is {@link #UNDEF}. Unmodifiable. */
    public static List<Node> elements(Node seq) {
        return seq.getSeq();
    }

    /** The sequence of {@code elements} (copied). */
    public static Node create(List<Node> elements) {
        return NodeFactory.createSeqNode(elements);
    }

    /**
     * The sequence over {@code elements} as given, not copied: a list that
     * reads its elements when asked (a reader's lazy row, SPECIFICATIONS.md
     * §7.7). The list must be immutable and {@link RandomAccess}.
     */
    public static Node view(List<Node> elements) {
        return NodeFactory.createSeqNodeView(elements);
    }

    /**
     * The clause a position guard adds to its refusal when the refused node is
     * a sequence or a slot: where each may stand (SPECIFICATIONS.md §9.5).
     * Empty for any other node.
     */
    public static String positionNote(Node n) {
        if (isSeq(n)) {
            return "; an RDF-array sequence may stand only as an object or as an element of a sequence";
        }
        if (isUndef(n)) {
            return "; undef may stand only as an element of a sequence";
        }
        return "";
    }

    /**
     * Whether {@code n} is a sequence PATTERN, or holds one at any depth: a
     * sequence with a variable, a gap or a nested pattern inside, which no
     * store holds and which only matching answers. A concrete sequence (slots
     * included) is a term like any other, and is not one.
     */
    public static boolean isPattern(Node n) {
        return n != null && !n.isConcrete() && holdsSequence(n);
    }

    /** Whether a position of {@code t} is a sequence pattern ({@link #isPattern}). */
    public static boolean holdsPattern(org.apache.jena.graph.Triple t) {
        return isPattern(t.getSubject()) || isPattern(t.getPredicate()) || isPattern(t.getObject());
    }

    /** Whether a triple of {@code bgp} holds a sequence pattern ({@link #isPattern}). */
    public static boolean holdsPattern(Iterable<org.apache.jena.graph.Triple> bgp) {
        for (org.apache.jena.graph.Triple t : bgp) {
            if (holdsPattern(t)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code n} is a sequence or holds one at any depth: a sequence
     * element of a sequence, or a component of a triple term.
     */
    public static boolean holdsSequence(Node n) {
        if (n == null) {
            return false;
        }
        if (n.isNodeSeq()) {
            return true;
        }
        if (n.isTripleTerm()) {
            org.apache.jena.graph.Triple t = n.getTriple();
            return holdsSequence(t.getSubject()) || holdsSequence(t.getPredicate()) || holdsSequence(t.getObject());
        }
        return false;
    }
}
