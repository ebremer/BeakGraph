package com.ebremer.beakgraph.core;

import org.apache.jena.graph.Node;

/**
 * Node ⇄ packed-NodeId resolution (see {@code com.ebremer.beakgraph.hdf5.jena.NodeId}
 * for the long encoding). Primitive throughout: ids flow through bindings and
 * iterators as raw longs, so resolution never allocates per value.
 *
 * @author Erich Bremer
 */
public interface NodeTable extends AutoCloseable {
    /** The node's packed id, or {@code NodeId.DOES_NOT_EXIST} when absent from this store. */
    public long getNodeIdForNode(Node n);

    /** The node for a packed id; null when the id cannot be resolved. */
    public Node getNodeForNodeId(long nodeId);
}
