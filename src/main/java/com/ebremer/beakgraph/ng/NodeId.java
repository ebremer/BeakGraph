package com.ebremer.beakgraph.ng;

final public class NodeId implements Comparable<NodeId> {
    public static final NodeId NodeDoesNotExist = new NodeId(-8) ;
    public static final NodeId NodeIdAny = new NodeId(-9) ;
    private final int id;
    private final Object value;
    private final NodeType type;

    public NodeId(int c) {
        id = c;
        type = NodeType.RESOURCE;
        value = null;
        toString();
    }
    
    public NodeId(Object c) {
        value = c;
        id = -1;
        type = NodeType.LITERAL;
        toString();
    }
    
    public NodeType getType() {
        return type;
    }
    
    public int getID() {
        return id;
    }
    
    public Object getValue() {
        return value;
    }

    @Override
    public int compareTo(NodeId o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public String toString() {
        return "NodeId : ID["+id+"] VALUE["+value+"]";
    }
    
    public static final boolean isAny(NodeId nodeId) { return nodeId == NodeIdAny || nodeId == null ; }
    public static final boolean isDoesNotExist(NodeId nodeId) { return nodeId == NodeDoesNotExist ; }
}
