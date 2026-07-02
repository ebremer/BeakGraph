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
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE);
        }
    },
    GPOS { // Graph, Predicate, Object, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE);
        }
    };

    public abstract Comparator<Quad> getComparator();
}
