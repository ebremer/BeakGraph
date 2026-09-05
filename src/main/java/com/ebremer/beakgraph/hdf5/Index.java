package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.core.lib.NodeComparator;
import org.apache.jena.sparql.core.Quad;
import java.util.Comparator;

/**
 * The quad orderings BeakGraph materializes. Exactly these two are written and
 * read (HDF5Writer / BGIteratorMaster); the other 22 permutations that used to
 * be enumerated here were never constructed and were removed in the dead-code
 * sweep - add a constant back only together with a writer and an access path
 * that use it.
 */
public enum Index {
    GSPO { // Graph, Subject, Predicate, Object
        @Override
        public Comparator<Quad> getComparator(NodeComparator cmp) {
            return Comparator
                .comparing(Quad::getGraph, cmp)
                .thenComparing(Quad::getSubject, cmp)
                .thenComparing(Quad::getPredicate, cmp)
                .thenComparing(Quad::getObject, cmp);
        }
    },
    GPOS { // Graph, Predicate, Object, Subject
        @Override
        public Comparator<Quad> getComparator(NodeComparator cmp) {
            return Comparator
                .comparing(Quad::getGraph, cmp)
                .thenComparing(Quad::getPredicate, cmp)
                .thenComparing(Quad::getObject, cmp)
                .thenComparing(Quad::getSubject, cmp);
        }
    };

    /** The quad order on the shared {@link NodeComparator#INSTANCE}. */
    public Comparator<Quad> getComparator() {
        return getComparator(NodeComparator.INSTANCE);
    }

    /**
     * The quad order on {@code cmp} - a per-sort
     * {@link com.ebremer.beakgraph.core.lib.CachingNodeComparator} for the
     * RAM writer's quad sorts, whose literal objects otherwise convert
     * through Jena's global cache on every comparison (BG-249).
     */
    public abstract Comparator<Quad> getComparator(NodeComparator cmp);
}
