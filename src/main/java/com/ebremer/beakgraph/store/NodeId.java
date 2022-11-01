package com.ebremer.beakgraph.store;

final public class NodeId implements Comparable<NodeId> {
    public static final NodeId NodeDoesNotExist = new NodeId(-8) ;
    public static final NodeId NodeIdAny = new NodeId(-9) ;
    private int id = -1;
    private Object value;
    private NodeType type;

    public NodeId(int c) {
        //System.out.println("Create a NodeId(int c) : "+c);
        id = c;
        type = NodeType.RESOURCE;
    }
    
    public NodeId(Object c) {
        //String yah = (c==null) ? "NULL" : c.getClass().toGenericString();
        //System.out.println("Create a NodeId(Object c) : "+c+" "+yah);
        value = c;
        type = NodeType.LITERAL;
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
