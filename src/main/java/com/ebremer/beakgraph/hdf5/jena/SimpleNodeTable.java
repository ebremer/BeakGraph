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

    // Caffeine LRU Caches for extreme high-performance concurrent caching.
    // Keys/values are packed NodeId longs (boxed at the cache boundary only).
    private final Cache<Long, Node> nodeId2nodemap = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build();

    private final Cache<Node, Long> node2nodeIdmap = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build();

    public SimpleNodeTable(PositionalDictionaryReader dict) {
        this.dict = dict;
    }

    /**
     * Resolves a Node to its packed NodeId. Predicates and entities (G/S/O URIs and
     * blank nodes) occupy SEPARATE id-spaces, so a URI used in both roles - e.g.
     * {@code :p} in {@code :p a rdf:Property} (entity) and in {@code :a :p :b}
     * (predicate) - has a distinct id in each dictionary. With no position context
     * available here, a dual-role URI is resolved predicate-first and returned as a
     * single id.
     * <p>
     * That single answer is safe because a NodeId is always consumed position-aware:
     * a bound variable is turned back into its Node via {@link #getNodeForNodeId}
     * (keyed by the full packed id, so it returns the correct URI no matter which
     * role's id it carries) and then re-located in the dictionary for the position
     * it is used at when the id-spaces differ - see HDF5Reader.substituteIfCrossSpace.
     * The raw id is never indexed directly into a different id-space.
     */
    private long findInDictionaries(Node n) {
        if (n == null || n.isVariable()) {
            return NodeId.DOES_NOT_EXIST;
        }

        long id;

        // 1. If it's a Literal, it MUST be in the Object dictionary (Literals dataset)
        if (n.isLiteral()) {
            if ((id = dict.getObjects().locate(n)) != -1) {
                return NodeId.pack(NodeType.OBJECT, id);
            }
            return NodeId.DOES_NOT_EXIST;
        }

        // 2. If it's a URI, it could be a Predicate OR an Entity (G/S/O)
        if (n.isURI()) {
            // Check Predicates first (it's a much smaller dictionary, so binary search is faster)
            if ((id = dict.getPredicates().locate(n)) != -1) {
                return NodeId.pack(NodeType.PREDICATE, id);
            }
            // If not a predicate, check the universal Entity dictionary (accessed via getSubjects)
            if ((id = dict.getSubjects().locate(n)) != -1) {
                // We default to SUBJECT for entities, but it applies globally to G, S, and O
                return NodeId.pack(NodeType.SUBJECT, id);
            }
            return NodeId.DOES_NOT_EXIST;
        }

        // 3. If it's a Blank Node, it MUST be in the Entity dictionary
        if (n.isBlank()) {
            if ((id = dict.getSubjects().locate(n)) != -1) {
                return NodeId.pack(NodeType.SUBJECT, id);
            }
        }

        return NodeId.DOES_NOT_EXIST;
    }

    @Override
    public long getNodeIdForNode(Node n) {
        // Single cache operation (lookup-or-compute) instead of getIfPresent+put.
        // Misses are cached too: the store is immutable, so absence is permanent,
        // and an uncached miss re-ran up to two dictionary binary searches on every
        // lookup of the same foreign term (VALUES/BIND-heavy queries). The
        // does-not-exist sentinel is deliberately NOT seeded into nodeId2nodemap.
        return node2nodeIdmap.get(n, key -> {
            long nid = findInDictionaries(key);
            if (nid != NodeId.DOES_NOT_EXIST) {
                nodeId2nodemap.put(nid, key);
            }
            return nid;
        });
    }

    @Override
    public Node getNodeForNodeId(long nodeId) {
        if (nodeId == NodeId.NONE) throw new IllegalArgumentException("getNodeForNodeId: NONE");

        Node cachedNode = nodeId2nodemap.getIfPresent(nodeId);
        if (cachedNode != null) {
            return cachedNode;
        }

        // Because of the monolithic design, SUBJECT and GRAPH both point to the Entity dictionary.
        // OBJECT points to the hybrid Entity+Literal dictionary wrapper.
        Node node = switch (NodeId.type(nodeId)) {
            case SUBJECT, GRAPH -> dict.getSubjects().extract(NodeId.id(nodeId));
            case PREDICATE -> dict.getPredicates().extract(NodeId.id(nodeId));
            case OBJECT -> dict.getObjects().extract(NodeId.id(nodeId));
            default -> throw new IllegalStateException("Unresolvable NodeId: " + NodeId.toString(nodeId));
        };

        if (node != null) {
            nodeId2nodemap.put(nodeId, node);
            // Deliberately NOT seeding node2nodeIdmap here. A dual-role URI has two valid
            // NodeIds (predicate vs entity id-space); writing the reverse mapping from
            // whichever role was reconstructed first would make getNodeIdForNode flip
            // between roles on successive lookups. Leaving the Node -> NodeId mapping
            // owned solely by getNodeIdForNode keeps it deterministic (predicate-first).
            // nodeId2nodemap above is keyed by the full packed id, so it stays correct
            // for both roles.
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
