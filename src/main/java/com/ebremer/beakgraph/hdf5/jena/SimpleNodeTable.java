package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.jena.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleNodeTable implements NodeTable {
    private static final Logger logger = LoggerFactory.getLogger(SimpleNodeTable.class);
    
    private final PositionalDictionaryReader dict;

    /** Entries per direction; override with -Dbeakgraph.nodetable.cache.size. */
    private static final long CACHE_SIZE = Long.getLong("beakgraph.nodetable.cache.size", 1_000_000L);

    // Caffeine LRU Caches for extreme high-performance concurrent caching
    private final Cache<NodeId, Node> nodeId2nodemap = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build();

    private final Cache<Node, NodeId> node2nodeIdmap = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build();
    
    public SimpleNodeTable(PositionalDictionaryReader dict) {
        this.dict = dict;
    }
    
    /**
     * Resolves a Node to its NodeId. Predicates and entities (G/S/O URIs and blank
     * nodes) occupy SEPARATE id-spaces, so a URI used in both roles - e.g. {@code :p}
     * in {@code :p a rdf:Property} (entity) and in {@code :a :p :b} (predicate) - has a
     * distinct id in each dictionary. With no position context available here, a
     * dual-role URI is resolved predicate-first and returned as a single NodeId.
     * <p>
     * That single answer is safe because a NodeId is always consumed position-aware: a
     * bound variable is turned back into its Node via {@link #getNodeForNodeId} (keyed
     * by the full NodeId, so it returns the correct URI no matter which role's id it
     * carries) and then re-located in the dictionary for the position it is used at -
     * see {@code HDF5Reader.substitute}. The raw id is never indexed directly into a
     * different id-space.
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
        // Single cache operation (lookup-or-compute) instead of getIfPresent+put.
        // Misses are cached too: the store is immutable, so absence is permanent,
        // and an uncached miss re-ran up to two dictionary binary searches on every
        // lookup of the same foreign term (VALUES/BIND-heavy queries). The shared
        // does-not-exist sentinel is deliberately NOT seeded into nodeId2nodemap.
        return node2nodeIdmap.get(n, key -> {
            NodeId nid = findInDictionaries(key);
            if (nid != NodeId.NodeDoesNotExist) {
                nodeId2nodemap.put(nid, key);
            }
            return nid;
        });
    }

    @Override
    public Node getNodeForNodeId(NodeId id) {
        if (id == null) throw new IllegalArgumentException("getNodeForNodeId: null NodeId");
        
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
            default -> throw new IllegalStateException("Unknown NodeType: " + id.getType());
        };
        
        if (node != null) {
            nodeId2nodemap.put(id, node);
            // Deliberately NOT seeding node2nodeIdmap here. A dual-role URI has two valid
            // NodeIds (predicate vs entity id-space); writing the reverse mapping from
            // whichever role was reconstructed first would make getNodeIdForNode flip
            // between roles on successive lookups. Leaving the Node -> NodeId mapping
            // owned solely by getNodeIdForNode keeps it deterministic (predicate-first).
            // nodeId2nodemap above is keyed by the full NodeId, so it stays correct for
            // both roles.
        }

        return node;
    }
    
    public void status() {
        // Caffeine evaluates size concurrently, so we use estimatedSize()
        logger.debug("nodeId2nodemap size: {}, node2nodeIdmap size: {}",
                nodeId2nodemap.estimatedSize(), node2nodeIdmap.estimatedSize());
    }

    @Override
    public void close() throws Exception {
        nodeId2nodemap.invalidateAll();
        node2nodeIdmap.invalidateAll();
    }
}
