package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.graph.Node;

public class SimpleNodeTable implements NodeTable {
    
    private final PositionalDictionaryReader dict;
    private final ConcurrentHashMap<NodeId, Node> nodeId2nodemap = new ConcurrentHashMap<>(1_000);
    private final ConcurrentHashMap<Node, NodeId> node2nodeIdmap = new ConcurrentHashMap<>(1_000);
    
    public SimpleNodeTable(PositionalDictionaryReader dict) {
        this.dict = dict;
    }
    
    private NodeId findInDictionaries(Node n) {
        long id;
        if ((id = dict.getGraphs().locate(n)) != -1) return new NodeId(id, NodeType.GRAPH);
        if ((id = dict.getPredicates().locate(n)) != -1) return new NodeId(id, NodeType.PREDICATE);
        if ((id = dict.getSubjects().locate(n)) != -1) return new NodeId(id, NodeType.SUBJECT);
        if ((id = dict.getObjects().locate(n)) != -1) return new NodeId(id, NodeType.OBJECT);    
        return NodeId.NodeDoesNotExist;
    }    

    @Override
    public NodeId getNodeIdForNode(Node n) {
        if (node2nodeIdmap.containsKey(n)) {
            return node2nodeIdmap.get(n);
        }
        NodeId nid = findInDictionaries(n);
        nodeId2nodemap.putIfAbsent(nid, n);
        return nid;
    }

    @Override
    public Node getNodeForNodeId(NodeId id) {
        if (id == null) throw new Error("getNodeForNodeId : "+id);
        if (nodeId2nodemap.containsKey(id)) {
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
    
    public void status() {
        IO.println(String.format("nodeId2nodemap : %d, node2nodeIdmap : %d", nodeId2nodemap.size(), node2nodeIdmap.size()));
    }

    @Override
    public void close() throws Exception {}
}
