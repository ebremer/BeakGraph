package com.ebremer.beakgraph.core;

import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface Dictionary {
    
    /**
     * Locates a Node and returns its ID.
     * @param element The Node to find.
     * @return The ID if found, or -1 if not found.
     */
    public long locate(Node element);

    /**
     * Searches for a Node and returns its ID or its insertion point.
     * <p>
     * This method follows the standard binary search contract:
     * <ul>
     * <li>If found: returns the positive ID.</li>
     * <li>If not found: returns {@code -(insertion_point) - 1}.</li>
     * </ul>
     * The {@code insertion_point} is the ID where the element would be inserted to maintain sort order.
     * This allows range queries (e.g. find values > X) to determine the correct starting ID even if X doesn't exist.
     * Ids are 1-based, so the smallest legal miss is {@code -2} (insertion point 1): an empty
     * dictionary answers exactly that, never {@code -1}.
     * @param element The Node to search for.
     * @return The ID or the encoded insertion point.
     */
    public long search(Node element);

    /**
     * The null object for an ABSENT section: nothing is stored, every lookup
     * misses, and the only insertion point is id 1 (encoded {@code -2}). One
     * instance for the reader and writer sides, which used to disagree
     * ({@code -1} - insertion point 0, not a 1-based id - versus {@code -2}; BG-312).
     */
    public static final Dictionary EMPTY = new Dictionary() {
        @Override public long locate(Node element) { return -1; }
        @Override public long search(Node element) { return -2; }
        @Override public Node extract(long id) {
            throw new IllegalArgumentException("empty dictionary holds no id " + id);
        }
        @Override public long getNumberOfNodes() { return 0; }
        @Override public Stream<Node> streamNodes() { return Stream.empty(); }
        @Override public boolean hasFloatLiterals() { return false; }
        @Override public boolean hasDoubleLiterals() { return false; }
    };

    public Node extract(long id);
    public long getNumberOfNodes();
    public Stream<Node> streamNodes();

    /**
     * Whether this dictionary holds any xsd:float literal. Range pushdown
     * widens an integer or decimal bound to the float rounding of the
     * constant only when float rows exist; the conservative default says
     * they might.
     */
    public default boolean hasFloatLiterals() { return true; }

    /** Whether this dictionary holds any xsd:double literal (see {@link #hasFloatLiterals}). */
    public default boolean hasDoubleLiterals() { return true; }
    
}