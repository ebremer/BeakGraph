package com.ebremer.beakgraph.hdf5.jena;

/**
 * A NodeId is a primitive {@code long}: 3 type bits ({@link NodeType} ordinal)
 * in bits 63-61, the dictionary id in bits 60-0.
 *
 * <p>Formerly a (long id, NodeType) object allocated per bound value per result
 * row - the dominant scan-time garbage. The 61-bit id space (2.3e18) sits ~16x
 * above the on-disk format's own ceiling (bit-packed id widths stop at 57
 * bits), so the encoding can never become the store's capacity limit. Packed
 * values exist only at runtime; nothing on disk depends on this layout.
 *
 * <p>Special values, all outside every real id-space:
 * <ul>
 *   <li>{@link #NONE} (-1): "no binding here" - what lookups return for an
 *       absent variable (the old API's null). All-ones = type bits 111, which
 *       no {@link NodeType} ordinal produces.</li>
 *   <li>{@link #DOES_NOT_EXIST}: the term is not in this store (SPECIAL-typed,
 *       reserved id). Recorded in bindings so absence is cached and pattern
 *       short-circuits work; never dereferenced.</li>
 * </ul>
 *
 * @author Erich Bremer
 */
public final class NodeId {

    private NodeId() {}

    private static final int TYPE_SHIFT = 61;
    private static final long ID_MASK = (1L << TYPE_SHIFT) - 1;

    /** "No binding": what lookups return for an absent variable. */
    public static final long NONE = -1L;

    /**
     * The term does not exist in this store (the old NodeDoesNotExist sentinel).
     * Id bits deliberately 0: every id consumer treats {@code < 1} as "no match",
     * so even a hypothetical unguarded leak of this sentinel into an id lookup
     * yields an empty result rather than entity #N - the same failure shape the
     * old negative-id sentinel had.
     */
    public static final long DOES_NOT_EXIST = pack(NodeType.SPECIAL, 0);

    public static long pack(NodeType type, long id) {
        // Callers never produce ids past the 57-bit on-disk ceiling; assert-only
        // so the hot path carries no branch in production.
        assert (id & ~ID_MASK) == 0 : "id overflows 61 bits: " + id;
        return ((long) type.ordinal() << TYPE_SHIFT) | id;
    }

    public static long id(long nodeId) {
        return nodeId & ID_MASK;
    }

    public static NodeType type(long nodeId) {
        return NodeType.VALUES[(int) (nodeId >>> TYPE_SHIFT)];
    }

    /** Whether the id lives in the isolated predicate id-space. */
    public static boolean isPredicateSpace(long nodeId) {
        return (nodeId >>> TYPE_SHIFT) == NodeType.PREDICATE.ordinal();
    }

    public static boolean isSpecial(long nodeId) {
        return (nodeId >>> TYPE_SHIFT) == NodeType.SPECIAL.ordinal();
    }

    public static boolean isDoesNotExist(long nodeId) {
        return nodeId == DOES_NOT_EXIST;
    }

    /** Debug rendering; matches the old object's "NodeID [id type]" shape. */
    public static String toString(long nodeId) {
        if (nodeId == NONE) {
            return "NodeID [NONE]";
        }
        return String.format("NodeID [%s %s]", id(nodeId), type(nodeId));
    }
}
