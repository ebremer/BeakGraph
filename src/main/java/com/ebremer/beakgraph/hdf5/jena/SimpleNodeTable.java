package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.FiveSectionDictionaryReader;
import org.apache.jena.graph.Node;

public class SimpleNodeTable implements NodeTable {
    
    private final FiveSectionDictionaryReader dict;
    
    public SimpleNodeTable(FiveSectionDictionaryReader dict) {
        this.dict = dict;
    }

    @Override
    public NodeId getNodeIdForNode(Node n) {
        long id;
        id = dict.getGraphs().locate(n);
        if (id != -1) return new NodeId(id, NodeType.GRAPH);
        id = dict.getPredicates().locate(n);
        if (id != -1) return new NodeId(id, NodeType.PREDICATE);
        id = dict.getSubjects().locate(n);
        if (id != -1) return new NodeId(id, NodeType.SUBJECT);
        id = dict.getObjects().locate(n);
        if (id != -1) return new NodeId(id, NodeType.OBJECT);        
        return NodeId.NodeDoesNotExist;
    }

    @Override
    public Node getNodeForNodeId(NodeId id) {
        if (id == null) return null; //|| NodeId.isDoesNotExist(id)) return null;
        return switch (id.getType()) {
            case NodeType.SUBJECT -> dict.getSubjects().extract(id.getId());
            case NodeType.PREDICATE -> dict.getPredicates().extract(id.getId());
            case NodeType.OBJECT -> dict.getObjects().extract(id.getId());
            case NodeType.GRAPH -> dict.getGraphs().extract(id.getId());
            default -> throw new Error("Unknown Node Type ID: " + id.getType());
        };
    }

    @Override
    public void close() throws Exception {}

    @Override
    public int getNGID(Node node) {
        long id = dict.getGraphs().locate(node);
        return (int) id;
    }
}