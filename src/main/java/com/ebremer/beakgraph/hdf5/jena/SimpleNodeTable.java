package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.graph.Node;

public class SimpleNodeTable implements NodeTable {
    
    private final PositionalDictionaryReader dict;
    private static final ConcurrentHashMap<NodeId, Node> nodeId2nodemap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Node, NodeId> node2nodeIdmap = new ConcurrentHashMap<>();
    
    public SimpleNodeTable(PositionalDictionaryReader dict) {
        this.dict = dict;
    }

    @Override
    public NodeId getNodeIdForNode(Node n) {
        if (node2nodeIdmap.contains(n)) {
            return node2nodeIdmap.get(n);
        }
        long id;
        NodeId nid = NodeId.NodeDoesNotExist;
        id = dict.getGraphs().locate(n);
        if (id != -1) nid = new NodeId(id, NodeType.GRAPH);
        id = dict.getPredicates().locate(n);
        if (id != -1) nid = new NodeId(id, NodeType.PREDICATE);
        id = dict.getSubjects().locate(n);
        if (id != -1) nid = new NodeId(id, NodeType.SUBJECT);
        id = dict.getObjects().locate(n);
        if (id != -1) nid = new NodeId(id, NodeType.OBJECT);
        nodeId2nodemap.putIfAbsent(nid, n);
        return nid;
    }

    @Override
    public Node getNodeForNodeId(NodeId id) {
        if (id == null) throw new Error("getNodeForNodeId : "+id); //return null; //|| NodeId.isDoesNotExist(id)) return null;
        if (nodeId2nodemap.contains(id)) {
            return nodeId2nodemap.get(id);
        }
        Node node = switch (id.getType()) {
            case NodeType.SUBJECT -> dict.getSubjects().extract(id.getId());
            case NodeType.PREDICATE -> dict.getPredicates().extract(id.getId());
            case NodeType.OBJECT -> dict.getObjects().extract(id.getId());
            case NodeType.GRAPH -> dict.getGraphs().extract(id.getId());
            default -> throw new Error("Unknown Node Type ID: " + id.getType());
        };
        nodeId2nodemap.putIfAbsent(id, node);
        node2nodeIdmap.putIfAbsent(node, id);
        return node;
    }

    @Override
    public void close() throws Exception {}
}