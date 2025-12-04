package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.jena.NodeId;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface NodeTable extends AutoCloseable {
    public NodeId getNodeIdForNode(Node n);
    public Node getNodeForNodeId(NodeId id);
}
