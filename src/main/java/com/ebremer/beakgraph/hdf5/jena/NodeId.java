package com.ebremer.beakgraph.hdf5.jena;

import java.util.Objects;

final public class NodeId implements Comparable<NodeId> {
    public static final NodeId NodeDoesNotExist = new NodeId( -8, NodeType.SPECIAL );
    public static final NodeId NodeIdAny = new NodeId( -9, NodeType.SPECIAL );
    private final long id;
    private final NodeType type;

    public NodeId(long id, NodeType type) {
        this.id = id;
        this.type = type;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeId nodeId = (NodeId) o;
        return id == nodeId.id && type == nodeId.type;
    }

    @Override
    public int hashCode() {
        // Objects.hash handles the null check for 'type' and 
        // effectively mixes the bits of the long 'id'
        return Objects.hash(id, type);
    }

    @Override
    public int compareTo(NodeId o) {
        int res = Long.compare(this.id, o.id);
        if (res == 0) {
            return this.type.compareTo(o.type);
        }
        return res;
    }
}
