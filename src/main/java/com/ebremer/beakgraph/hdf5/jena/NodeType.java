package com.ebremer.beakgraph.hdf5.jena;

/**
 * Position/id-space tag carried in a packed NodeId's top bits (see {@link NodeId}).
 * Ordinals are part of the RUNTIME encoding only - packed NodeIds are never
 * persisted - but reordering constants still changes {@code NodeId.pack}'s
 * output shape, so append new members rather than reordering.
 *
 * @author erich
 */
public enum NodeType {
    GRAPH, SUBJECT, PREDICATE, OBJECT, SPECIAL;

    /** Cached {@link #values()} for ordinal decode without the defensive-copy allocation. */
    static final NodeType[] VALUES = values();
}
