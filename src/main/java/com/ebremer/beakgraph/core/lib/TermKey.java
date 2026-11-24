package com.ebremer.beakgraph.core.lib;

import java.util.Comparator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * A term with its place in {@link NodeComparator}'s order worked out once.
 *
 * <p>A sort compares each term with about log n others. The comparator works
 * out, for both terms of every pair, what kind of term it has, a literal's
 * value, whether that is a number or a date, and for two numbers their exact
 * values: of a build's processor time, 40 to 60% went there (PERFORMANCE.md).
 * A key holds those answers for its term - made at the key's first
 * comparison, by the thread that asks - and two keys are compared by reading
 * them:
 * <ul>
 *   <li><b>Terms of different kinds</b> by the kind's rank: the default
 *       graph's names, blank nodes, IRIs, literals, triple terms.</li>
 *   <li><b>Two blank nodes or two IRIs</b> by the first seven characters,
 *       packed into a long ({@link #prefix}), and by the whole label or IRI
 *       when those are the same.</li>
 *   <li><b>Two numbers</b> by the double nearest to each
 *       ({@link NumericOrder#nearest}), and by their exact values only when
 *       those doubles are equal and are not both the numbers themselves.</li>
 *   <li><b>Any other two literals</b> by the comparator's own code, given
 *       the values the keys hold ({@link NodeComparator#compareByValue}).</li>
 * </ul>
 * The order is {@link NodeComparator}'s for every pair: the parts that decide
 * a pair are that class's, and what is new here - the prefix and the nearest
 * double - only ever answers where it agrees with it. {@code TermKeyTest}
 * holds the two to each other pair by pair, and every dictionary a writer
 * makes in memory is checked against the comparator before it is stored
 * ({@link NodeSorter#adjacentOrder}).
 *
 * <p>A literal's value is made without Jena's cache ({@link NodeValues#of}),
 * so sorts on several threads do not wait for each other there.
 *
 * <p>Keys of one term are equal. A key may be shared by the rows of its term
 * and compared from several threads at once: what it works out is one
 * immutable object, and two threads that both find it missing make the same
 * thing twice.
 */
public final class TermKey {

    /** {@link NodeComparator}'s order, on keys. */
    public static final Comparator<TermKey> ORDER = TermKey::compare;

    /** Heap a key adds to its term once it has been compared: the key, what it worked out, a value. For byte budgets. */
    public static final long HEAP_BYTES = 96;

    private static final int NOT_A_TERM = -1;
    private static final int DEFAULT_GRAPH = 0;
    private static final int BLANK = 1;
    private static final int IRI = 2;
    private static final int LITERAL = 3;
    private static final int TRIPLE_TERM = 4;

    private static final int BY_VALUE = 0;
    private static final int COMPOSITE = 1;
    private static final int LANGUAGE = 2;
    private static final int NUMBER = 3;

    private final Node node;
    private Place place;

    public TermKey(Node node) {
        this.node = node;
    }

    /** The term; null stands for the default graph, as it does for the comparator. */
    public Node node() {
        return node;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TermKey key && java.util.Objects.equals(node, key.node);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hashCode(node);
    }

    @Override
    public String toString() {
        return String.valueOf(node);
    }

    /** What the order needs to know of one term. Immutable: see the class comment on sharing. */
    private static final class Place {
        final int kind;
        final long prefix;          // blank nodes and IRIs
        final int literal;          // literals: which of the comparator's branches
        final NodeValue value;      // literals compared by value, numbers included; null where it is made on demand
        final int temporal;         // NodeComparator.temporalGroup(value)
        final int rank;             // numbers: NumericOrder.rank
        final double nearest;       // finite numbers: NumericOrder.nearest
        final boolean binary;       // finite numbers: the number IS that double

        Place(int kind, long prefix) {
            this(kind, prefix, BY_VALUE, null, 0, 0, 0, false);
        }

        Place(int kind, long prefix, int literal, NodeValue value, int temporal, int rank, double nearest, boolean binary) {
            this.kind = kind;
            this.prefix = prefix;
            this.literal = literal;
            this.value = value;
            this.temporal = temporal;
            this.rank = rank;
            this.nearest = nearest;
            this.binary = binary;
        }
    }

    private Place place() {
        Place p = place;
        if (p == null) {
            p = placeOf(node);
            place = p;
        }
        return p;
    }

    private static Place placeOf(Node n) {
        if (NodeComparator.isDefaultGraph(n)) {
            return new Place(DEFAULT_GRAPH, 0);
        }
        if (n.isBlank()) {
            return new Place(BLANK, prefix(n));
        }
        if (n.isURI()) {
            return new Place(IRI, prefix(n));
        }
        if (n.isLiteral()) {
            if (CdtTerms.isComposite(n)) {
                return new Place(LITERAL, 0, COMPOSITE, null, 0, 0, 0, false);
            }
            if (NodeComparator.hasLanguage(n)) {
                return new Place(LITERAL, 0, LANGUAGE, null, 0, 0, 0, false);
            }
            NodeValue value = NodeComparator.keyValue(n);
            if (value.isNumber()) {
                int rank = NumericOrder.rank(value);
                boolean finite = rank == 1;
                return new Place(LITERAL, 0, NUMBER, value, 0, rank,
                        finite ? NumericOrder.nearest(value) : 0, finite && NumericOrder.isBinary(value));
            }
            return new Place(LITERAL, 0, BY_VALUE, value, NodeComparator.temporalGroup(value), 0, 0, false);
        }
        if (n.isTripleTerm()) {
            return new Place(TRIPLE_TERM, 0);
        }
        return new Place(NOT_A_TERM, 0);
    }

    /** The order of two keys: {@code NodeComparator.compare} of their terms. */
    public static int compare(TermKey a, TermKey b) {
        Place x = a.place();
        Place y = b.place();
        if (x.kind == NOT_A_TERM || y.kind == NOT_A_TERM) {
            // A variable, Node.ANY: the comparator's to refuse, as it refuses them.
            return NodeComparator.INSTANCE.compare(a.node, b.node);
        }
        if (a.node == b.node) {
            return 0;   // one term: the rows of a term share its key, or at least its node
        }
        if (x.kind != y.kind) {
            return Integer.compare(x.kind, y.kind);
        }
        switch (x.kind) {
            case BLANK, IRI -> {
                if (x.prefix != y.prefix) {
                    return Long.compareUnsigned(x.prefix, y.prefix);
                }
                return FrozenNodeCmp.compareRDFTerms(a.node, b.node);
            }
            case LITERAL -> {
                return compareLiterals(a.node, x, b.node, y);
            }
            default -> {
                // The default graph's two names; two triple terms, compared part by part.
                return NodeComparator.INSTANCE.compare(a.node, b.node);
            }
        }
    }

    private static int compareLiterals(Node n1, Place x, Node n2, Place y) {
        if (x.literal == NUMBER && y.literal == NUMBER) {
            if (x.rank != y.rank) {
                return Integer.compare(x.rank, y.rank);
            }
            if (x.rank == 1) {
                if (x.nearest < y.nearest) {
                    return -1;
                }
                if (x.nearest > y.nearest) {
                    return 1;
                }
                if (!(x.binary && y.binary)) {
                    int exact = NumericOrder.exact(x.value).compareTo(NumericOrder.exact(y.value));
                    if (exact != 0) {
                        return exact;
                    }
                }
            }
            return NodeComparator.compareExactLiteralTerms(n1, n2);
        }
        if (x.literal == COMPOSITE && y.literal == COMPOSITE) {
            return NodeComparator.compareComposites(n1, n2);
        }
        if (x.literal == LANGUAGE && y.literal == LANGUAGE) {
            return NodeComparator.compareLanguageTagged(n1, n2);
        }
        // A composite or a language-tagged literal beside a literal of another
        // branch is the rare pair: its value is made for the pair, not kept.
        NodeValue v1 = (x.value != null) ? x.value : NodeComparator.keyValue(n1);
        NodeValue v2 = (y.value != null) ? y.value : NodeComparator.keyValue(n2);
        int g1 = (x.value != null) ? x.temporal : NodeComparator.temporalGroup(v1);
        int g2 = (y.value != null) ? y.temporal : NodeComparator.temporalGroup(v2);
        return NodeComparator.compareByValue(n1, v1, g1, n2, v2, g2);
    }

    /**
     * An 8-byte key with the ONE property that matters: {@code prefix(a) !=
     * prefix(b)} implies unsigned key order equals NodeComparator order; equal
     * keys decide nothing. Byte 7 is the macro rank (default-graph sentinels
     * 0, bnode 1, URI 2, literal or triple term 3 - NodeComparator's macro
     * order, literals before triple terms left to it); bytes 6..0 are the
     * first chars of the label/URI, ASCII-compared exactly as
     * {@code String.compareTo} does. Any char &ge; 0xFF becomes a terminal
     * 0xFF marker (chars past it are NOT encoded - encoding them would
     * disagree with UTF-16 order). Literals encode rank only: their order is
     * by value, which the key holds apart from this.
     */
    public static long prefix(Node n) {
        int rank;
        String s;
        if (n == null || n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated)) {
            rank = 0;
            s = (n == null ? Quad.defaultGraphIRI : n).getURI();
        } else if (n.isBlank()) {
            rank = 1;
            s = n.getBlankNodeLabel();
        } else if (n.isURI()) {
            rank = 2;
            s = n.getURI();
        } else {
            rank = 3;
            s = null;
        }
        long key = ((long) rank) << 56;
        if (s != null) {
            int len = Math.min(7, s.length());
            int shift = 48;
            for (int i = 0; i < len; i++, shift -= 8) {
                char c = s.charAt(i);
                if (c >= 0xFF) {
                    key |= 0xFFL << shift;
                    break;
                }
                key |= ((long) c) << shift;
            }
        }
        return key;
    }
}
