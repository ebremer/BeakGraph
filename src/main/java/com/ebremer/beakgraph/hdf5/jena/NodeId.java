package com.ebremer.beakgraph.hdf5.jena;

final public class NodeId implements Comparable<NodeId> {
    public static final NodeId NodeDoesNotExist = new NodeId( -8, NodeType.SPECIAL) ;
    public static final NodeId NodeIdAny = new NodeId( -9, NodeType.SPECIAL ) ;
    private final long id;
    private final NodeType type;

    public NodeId(long id, NodeType type) {
        this.id = id;
        this.type = type;
        toString();
    }
    
    public NodeType getType() {
        return type;
    }
    
    public long getId() {
        return id;
    }

    
    public static final boolean isAny(NodeId nodeId) {
        return nodeId == NodeIdAny || nodeId == null ;
    }
    
    public static final boolean isDoesNotExist(NodeId nodeId) {
       return NodeDoesNotExist.equals(nodeId);
    }
    
    @Override
    public String toString() {
        return String.format("NodeID [%s %s]", id, type);
    }

    @Override
    public int compareTo(NodeId o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
