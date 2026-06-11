package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.jena.BGReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-time cardinality statistics used for join reordering, recovered from the VoID description the
 * writer persists in the {@link Params#BGVOID} graph. Each {@code void:propertyPartition} gives an
 * exact per-predicate triple count; the total is their sum (every triple has exactly one predicate),
 * and distinct subject/object/predicate counts come from the dictionary partitions.
 *
 * <p>Loading reads the VoID graph through {@code BGReader.graphBaseFind}, which goes straight to the
 * index iterators - not through the query engine - so building the reorder transform cannot recurse
 * back into {@code getReorderTransform}.
 */
final class VoidStats {
    private static final Logger logger = LoggerFactory.getLogger(VoidStats.class);

    private final long totalTriples;
    private final long distinctSubjects;
    private final long distinctObjects;
    private final long distinctPredicates;
    private final Map<Node, Long> predicateCount;

    private VoidStats(long totalTriples, long distinctSubjects, long distinctObjects,
                      long distinctPredicates, Map<Node, Long> predicateCount) {
        this.totalTriples = totalTriples;
        this.distinctSubjects = distinctSubjects;
        this.distinctObjects = distinctObjects;
        this.distinctPredicates = distinctPredicates;
        this.predicateCount = predicateCount;
    }

    /** Package-visible factory (tests, or assembling stats from a non-VoID source). */
    static VoidStats of(long totalTriples, long distinctSubjects, long distinctObjects,
                        long distinctPredicates, Map<Node, Long> predicateCount) {
        return new VoidStats(totalTriples, distinctSubjects, distinctObjects, distinctPredicates, predicateCount);
    }

    long totalTriples()       { return totalTriples; }
    long distinctSubjects()   { return distinctSubjects; }
    long distinctObjects()    { return distinctObjects; }
    long distinctPredicates() { return distinctPredicates; }

    /** Exact triple count for a concrete predicate, or -1 if not recorded. */
    long predicateCount(Node predicate) {
        return predicateCount.getOrDefault(predicate, -1L);
    }

    /** True when there is enough information to drive reordering. */
    boolean usable() {
        return totalTriples > 0;
    }

    static VoidStats load(BGReader reader) {
        final Node bgvoid = Params.BGVOID;
        final Node any = Node.ANY;

        // void:propertyPartition resource -> the predicate it describes
        Map<Node, Node> partitionPredicate = new HashMap<>();
        reader.graphBaseFind(bgvoid, Triple.create(any, VOID.property.asNode(), any))
              .forEachRemaining(t -> partitionPredicate.put(t.getSubject(), t.getObject()));

        // resource -> void:triples value (covers both the partitions and the graph resource)
        Map<Node, Long> resourceTriples = new HashMap<>();
        reader.graphBaseFind(bgvoid, Triple.create(any, VOID.triples.asNode(), any))
              .forEachRemaining(t -> {
                  long n = asLong(t.getObject());
                  if (n >= 0) resourceTriples.put(t.getSubject(), n);
              });

        Map<Node, Long> predicateCount = new HashMap<>();
        long total = 0;
        for (Map.Entry<Node, Node> e : partitionPredicate.entrySet()) {
            Long n = resourceTriples.get(e.getKey());
            if (n != null) {
                // One partition per (graph, predicate) pair: the same predicate can
                // appear in several graph descriptions, so counts must SUM. A plain
                // put kept whichever partition iteration visited last, feeding the
                // reorder cost model a per-graph count against a dataset-wide total.
                predicateCount.merge(e.getValue(), n, Long::sum);
                total += n;
            }
        }

        GSPODictionary dict = reader.getDictionary();
        long ds = dict.getSubjects().getNumberOfNodes();
        long dobj = dict.getObjects().getNumberOfNodes();
        long dp = dict.getPredicates().getNumberOfNodes();

        logger.debug("VoID stats: {} triples across {} predicates (Ds={}, Do={}, Dp={})",
                total, predicateCount.size(), ds, dobj, dp);
        return new VoidStats(total, ds, dobj, dp, predicateCount);
    }

    private static long asLong(Node o) {
        if (o == null || !o.isLiteral()) {
            return -1;
        }
        try {
            return Long.parseLong(o.getLiteralLexicalForm().trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
