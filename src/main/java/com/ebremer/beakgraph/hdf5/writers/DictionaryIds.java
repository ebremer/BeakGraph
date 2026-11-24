package com.ebremer.beakgraph.hdf5.writers;

import java.util.function.ToLongFunction;
import org.apache.jena.graph.Node;

/**
 * The id-space rules the three dictionary writers share (BG-300): a
 * component's id is its 1-based rank in its section, the object id space
 * places the literals section (literals, RDF 1.2 triple terms and RDF-array
 * sequences) above the entities, and during a write every term is already in
 * its dictionary, so a
 * miss is a build-invariant violation reported by {@link #require}. The
 * sequential, parallel and ultra writers differ only in how a rank is looked
 * up (binary search, or an O(1) map) and pass that in as a function.
 */
public final class DictionaryIds {

    private DictionaryIds() {}

    /** {@code id} when positive; otherwise the writers' "Cannot resolve <role> (not in dictionary)" failure. */
    public static long require(long id, String role, Node element) {
        if (id > 0) {
            return id;
        }
        throw new IllegalStateException("Cannot resolve " + role + " (not in dictionary): " + element);
    }

    /**
     * An OBJECT's id: a literal, triple term or sequence resolves through the
     * literals section's rank offset by {@code maxEntityId}, anything else
     * through the entities section. {@code -1} (or the lookup's own
     * non-positive answer) for a miss.
     */
    public static long objectId(Node element, ToLongFunction<Node> literalRank,
                                ToLongFunction<Node> entityId, long maxEntityId) {
        if (inLiteralsSection(element)) {
            long lid = literalRank.applyAsLong(element);
            return (lid > 0) ? lid + maxEntityId : -1;
        }
        return entityId.applyAsLong(element);
    }

    /** Whether an object of this kind is a row of the literals section: a literal, a triple term or a sequence. */
    public static boolean inLiteralsSection(Node element) {
        return element.isLiteral() || element.isTripleTerm() || element.isNodeSeq();
    }

    /**
     * Element ids of a sequence for the literals section's encoder
     * (SPECIFICATIONS.md §7.6b): each element in the object space - a literal,
     * triple term or nested sequence through the section's OWN ranks
     * ({@code ownSectionRank}), offset by {@code maxEntityId} - and 0 for an
     * empty slot. A miss on any element fails the build.
     */
    public static long[] encodeSequence(Node seq, ToLongFunction<Node> entityId,
                                        ToLongFunction<Node> ownSectionRank, long maxEntityId) {
        java.util.List<Node> elements = seq.getSeq();
        long[] ids = new long[elements.size()];
        for (int i = 0; i < ids.length; i++) {
            Node e = elements.get(i);
            if (e.isNodeUndef()) {
                continue;   // 0: a slot
            }
            long id = objectId(e, ownSectionRank, entityId, maxEntityId);
            if (id < 1) {
                throw new IllegalStateException("Cannot resolve sequence element " + i + " (not in dictionaries): "
                        + e + " in " + seq);
            }
            ids[i] = id;
        }
        return ids;
    }

    /**
     * Component ids of a triple term for the literals section's encoder
     * (CHANGELOG.md "Format v5 design notes"): subject in the entity space,
     * predicate in the predicate space, object in the object space - where a
     * literal or nested triple term resolves through the section's OWN ranks
     * ({@code ownSectionRank}, the section still under construction), offset
     * by {@code maxEntityId}. A miss on any component fails the build.
     */
    public static long[] encodeTripleTerm(Node tt, ToLongFunction<Node> entityId, ToLongFunction<Node> predicateId,
                                          ToLongFunction<Node> ownSectionRank, long maxEntityId) {
        org.apache.jena.graph.Triple t = tt.getTriple();
        long s = entityId.applyAsLong(t.getSubject());
        long p = predicateId.applyAsLong(t.getPredicate());
        long oid = objectId(t.getObject(), ownSectionRank, entityId, maxEntityId);
        if (s < 1 || p < 1 || oid < 1) {
            throw new IllegalStateException("Cannot resolve triple-term components (not in dictionaries): "
                    + tt + " (s=" + s + ", p=" + p + ", o=" + oid + ")");
        }
        return new long[]{s, p, oid};
    }
}
