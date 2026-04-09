package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.jena.graph.Node;

public class SimpleNodeTable implements NodeTable {
    
    private final PositionalDictionaryReader dict;
    
    // Caffeine LRU Caches for extreme high-performance concurrent caching
    // Adjust maximumSize based on your typical heap allocation
    private final Cache<NodeId, Node> nodeId2nodemap = Caffeine.newBuilder()
            .maximumSize(100_000_000)
            .build();
            
    private final Cache<Node, NodeId> node2nodeIdmap = Caffeine.newBuilder()
            .maximumSize(100_000_000)
            .build();
    
    public SimpleNodeTable(PositionalDictionaryReader dict) {
        this.dict = dict;
    }
    
    /**
     * Optimized search using the Monolithic Dictionary layout.
     */
    private NodeId findInDictionaries(Node n) {
        if (n == null || n.isVariable()) {
            return NodeId.NodeDoesNotExist;
        }

        long id;

        // 1. If it's a Literal, it MUST be in the Object dictionary (Literals dataset)
        if (n.isLiteral()) {
            if ((id = dict.getObjects().locate(n)) != -1) {
                return new NodeId(id, NodeType.OBJECT);
            }
            return NodeId.NodeDoesNotExist;
        }

        // 2. If it's a URI, it could be a Predicate OR an Entity (G/S/O)
        if (n.isURI()) {
            // Check Predicates first (it's a much smaller dictionary, so binary search is faster)
            if ((id = dict.getPredicates().locate(n)) != -1) {
                return new NodeId(id, NodeType.PREDICATE);
            }
            // If not a predicate, check the universal Entity dictionary (accessed via getSubjects)
            if ((id = dict.getSubjects().locate(n)) != -1) {
                // We default to SUBJECT for entities, but it applies globally to G, S, and O
                return new NodeId(id, NodeType.SUBJECT); 
            }
            return NodeId.NodeDoesNotExist;
        }

        // 3. If it's a Blank Node, it MUST be in the Entity dictionary
        if (n.isBlank()) {
            if ((id = dict.getSubjects().locate(n)) != -1) {
                return new NodeId(id, NodeType.SUBJECT);
            }
        }
        
        return NodeId.NodeDoesNotExist;
    }    

    @Override
    public NodeId getNodeIdForNode(Node n) {
        NodeId cachedId = node2nodeIdmap.getIfPresent(n);
        if (cachedId != null) {
            return cachedId;
        }
        
        NodeId nid = findInDictionaries(n);
        
        if (nid != NodeId.NodeDoesNotExist) {
            nodeId2nodemap.put(nid, n);
            node2nodeIdmap.put(n, nid);
        }
        
        return nid;
    }

    @Override
    public Node getNodeForNodeId(NodeId id) {
        if (id == null) throw new Error("getNodeForNodeId : null ID");
        
        Node cachedNode = nodeId2nodemap.getIfPresent(id);
        if (cachedNode != null) {
            return cachedNode;
        }
        
        // Because of the monolithic design, SUBJECT and GRAPH both point to the Entity dictionary.
        // OBJECT points to the hybrid Entity+Literal dictionary wrapper.
        Node node = switch (id.getType()) {
            case NodeType.SUBJECT, NodeType.GRAPH -> dict.getSubjects().extract(id.getId());
            case NodeType.PREDICATE -> dict.getPredicates().extract(id.getId());
            case NodeType.OBJECT -> dict.getObjects().extract(id.getId());
            default -> throw new Error("Unknown Node Type ID: " + id.getType());
        };
        
        if (node != null) {
            nodeId2nodemap.put(id, node);
            node2nodeIdmap.put(node, id);
        }
        
        return node;
    }
    
    public void status() {
        // Caffeine evaluates size concurrently, so we use estimatedSize()
        IO.println(String.format("nodeId2nodemap size: %d, node2nodeIdmap size: %d", 
                nodeId2nodemap.estimatedSize(), node2nodeIdmap.estimatedSize()));
    }

    @Override
    public void close() throws Exception {
        nodeId2nodemap.invalidateAll();
        node2nodeIdmap.invalidateAll();
    }
}
