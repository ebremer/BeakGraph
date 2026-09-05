package com.ebremer.beakgraph.hdf5.writers.ultra;

import com.ebremer.beakgraph.hdf5.writers.DictionaryIds;
import com.ebremer.beakgraph.core.Futures;
import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.lib.NodeSorter;
import com.ebremer.beakgraph.hdf5.DictionarySection;
import com.ebremer.beakgraph.hdf5.writers.MultiTypeDictionaryWriter;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import org.apache.jena.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ultra writer's dictionary stage. One {@code NodeComparator} sort per
 * sub-dictionary (entities, predicates, literals) is shared THREE ways:
 *
 * <ol>
 * <li>the storage {@link MultiTypeDictionaryWriter} builds consume it via
 *     {@code setSortedNodes} (no internal re-sort),</li>
 * <li>a {@code Node -> id} hash map is built straight from sorted rank
 *     (dictionary ids ARE 1-based NodeComparator ranks - the invariant the
 *     whole id-tuple index design already rests on), replacing every
 *     binary-search {@code locate()} downstream with an O(1) lookup,</li>
 * <li>the columnar graph/subject/object id lists are populated positionally
 *     and in parallel from those maps.</li>
 * </ol>
 *
 * The storage builds and column fills are submitted asynchronously and joined
 * only in {@link #awaitStorage()}: the index stage needs nothing but the maps,
 * so key packing and index sorting overlap dictionary encoding instead of
 * waiting behind it.
 */
final class UltraDictionary {

    private static final Logger logger = LoggerFactory.getLogger(UltraDictionary.class);

    private final String name;
    private final long numQuads;
    private final long maxEntityId;
    private final long numPredicates;
    private final long numLiterals;

    // Released by releaseRankMaps() once the packing pass and the column
    // fills are done: ~50-60 bytes per term of overhead that otherwise stayed
    // live through the whole HDF5 emission (BG-115).
    private ConcurrentHashMap<Node, Long> entityIds;
    private ConcurrentHashMap<Node, Long> predicateIds;
    private ConcurrentHashMap<Node, Long> literalIds;

    private final ForkJoinTask<DictionaryWriter> entitiesTask;
    private final ForkJoinTask<DictionaryWriter> predicatesTask;
    private final ForkJoinTask<DictionaryWriter> literalsTask;
    private final ForkJoinTask<UltraPackedBuffer> graphsTask;
    private final ForkJoinTask<UltraPackedBuffer> subjectsTask;
    private final ForkJoinTask<UltraPackedBuffer> objectsTask;

    private DictionaryWriter entitiesdict;
    private DictionaryWriter predicatesdict;
    private DictionaryWriter literalsdict;
    private UltraPackedBuffer graphs;
    private UltraPackedBuffer subjects;
    private UltraPackedBuffer objects;

    UltraDictionary(UltraIngest ingest, ForkJoinPool pool) throws IOException {
        this.name = ingest.getName();
        this.numQuads = ingest.getNumberOfQuads();

        // ---- shared sorts (blocking, but internally parallel in the pool) ----
        logger.info("Sorting dictionary node sets ({} entities, {} predicates, {} literals; one shared sort each)...",
                ingest.getEntities().size(), ingest.getPredicates().size(), ingest.getLiterals().size());
        long phase = System.nanoTime();
        ForkJoinTask<Node[]> sortE = pool.submit(() -> sortedArray(ingest.getEntities()));
        ForkJoinTask<Node[]> sortP = pool.submit(() -> sortedArray(ingest.getPredicates()));
        ForkJoinTask<Node[]> sortL = pool.submit(() -> sortedArray(ingest.getLiterals()));
        final Node[] ents = join(sortE, "sort entities");
        final Node[] preds = join(sortP, "sort predicates");
        final Node[] lits = join(sortL, "sort literals");
        logger.info("Dictionary node sets sorted in {} ms", (System.nanoTime() - phase) / 1_000_000L);

        this.maxEntityId = ents.length;
        this.numPredicates = preds.length;
        this.numLiterals = lits.length;

        // ---- storage dictionary builds: async, joined in awaitStorage() ----
        logger.info("Dictionary encoding (entities/predicates/literals) started in the background");
        entitiesTask = pool.submit(() -> new MultiTypeDictionaryWriter.Builder()
                .setName("entities")
                .setSortedNodes(new ArrayList<>(Arrays.asList(ents)))
                .setStats(ingest.getStats())
                .section(DictionarySection.ENTITIES)
                .build());
        predicatesTask = pool.submit(() -> new MultiTypeDictionaryWriter.Builder()
                .setName("predicates")
                .setSortedNodes(new ArrayList<>(Arrays.asList(preds)))
                .setStats(ingest.getStats())
                .section(DictionarySection.PREDICATES)
                .build());
        literalsTask = pool.submit(() -> new MultiTypeDictionaryWriter.Builder()
                .setName("literals")
                .setSortedNodes(new ArrayList<>(Arrays.asList(lits)))
                .setDataTypes(ingest.getDataTypes())
                .setStats(ingest.getStats())
                .section(DictionarySection.LITERALS)
                // Component ids resolve against the SORTED ARRAYS, not the rank
                // maps: this task is submitted before those maps are built and
                // would race them. Binary search over the same arrays yields
                // identical ids (ids ARE ranks in these arrays).
                .setTripleTermEncoder((tt, own) -> encodeTripleTerm(tt, ents, preds, lits))
                .setTripleTermComponentIdBound((long) ents.length + lits.length)
                .build());

        // ---- id maps from sorted rank (the only thing the index stage needs) ----
        logger.info("Building node->id rank maps ({} entities, {} predicates, {} literals; no binary searches)...",
                ents.length, preds.length, lits.length);
        phase = System.nanoTime();
        entityIds = rankMap(ents, pool);
        predicateIds = rankMap(preds, pool);
        literalIds = rankMap(lits, pool);
        logger.info("Rank maps built in {} ms; index stage can start", (System.nanoTime() - phase) / 1_000_000L);

        // ---- columnar id lists: async, positional-parallel fills ----
        logger.info("Columnar id list population (graphs/subjects/objects) started in the background");
        int gBits = byteRoundedWidth(getNumberOfGraphs() + 1);
        int sBits = byteRoundedWidth(getNumberOfSubjects() + 1);
        int oBits = byteRoundedWidth(getNumberOfObjects() + 1);
        graphsTask = pool.submit(() -> populate("graphs", ingest.getUniqueGraphs(), this::locateGraph, gBits, pool));
        subjectsTask = pool.submit(() -> populate("subjects", ingest.getUniqueSubjects(), this::locateSubject, sBits, pool));
        objectsTask = pool.submit(() -> populate("objects", ingest.getUniqueObjects(), this::locateObject, oBits, pool));
    }

    private static Node[] sortedArray(Set<Node> nodes) {
        Node[] arr = nodes.toArray(Node[]::new);
        NodeSorter.parallelSort(arr);   // per-sort memoizing comparator (BG-249)
        return arr;
    }

    /**
     * Triple-term component resolution for the literals encoder (CHANGELOG.md "Format v5 design notes", ultra flavor): s in the entity space, p in the predicate space,
     * o in the object space - entity id, or maxEntityId + literals-section
     * rank for literals and nested triple terms.
     */
    private static long[] encodeTripleTerm(Node tt, Node[] ents, Node[] preds, Node[] lits) {
        return DictionaryIds.encodeTripleTerm(tt, n -> rankOf(ents, n), n -> rankOf(preds, n), n -> rankOf(lits, n), ents.length);
    }

    /** 1-based rank of {@code n} in the NodeComparator-sorted array, or -1. */
    private static long rankOf(Node[] sorted, Node n) {
        int i = Arrays.binarySearch(sorted, n, NodeComparator.INSTANCE);
        return (i >= 0) ? i + 1 : -1;
    }

    /** id(node) = 1 + rank in NodeComparator order; built with zero locate() calls. */
    private static ConcurrentHashMap<Node, Long> rankMap(Node[] sorted, ForkJoinPool pool) {
        ConcurrentHashMap<Node, Long> map = new ConcurrentHashMap<>(Math.max(16, sorted.length * 4 / 3 + 1));
        ParallelRadixSort.runChunks(pool, Math.max(1, Math.min(pool.getParallelism() * 2, sorted.length)),
                sorted.length, (c, from, to) -> {
                    for (int i = from; i < to; i++) {
                        map.put(sorted[i], (long) (i + 1));
                    }
                });
        return map;
    }

    /** Sorted unique nodes -> ids via the maps -> byte-aligned positional writes. */
    private UltraPackedBuffer populate(String bufferName, Set<Node> nodes, java.util.function.ToLongFunction<Node> locator,
                                       int bits, ForkJoinPool pool) {
        Node[] sorted = nodes.toArray(Node[]::new);
        NodeSorter.parallelSort(sorted);   // per-sort memoizing comparator (BG-249)
        UltraPackedBuffer target = new UltraPackedBuffer(bufferName, sorted.length, bits);
        ParallelRadixSort.runChunks(pool, Math.max(1, Math.min(pool.getParallelism() * 2, sorted.length)),
                sorted.length, (c, from, to) -> {
                    for (int i = from; i < to; i++) {
                        target.set(i, locator.applyAsLong(sorted[i]));
                    }
                });
        return target;
    }

    // ------------------------------------------------------------------
    // id resolution (O(1) map lookups)
    // ------------------------------------------------------------------

    /**
     * Drops the rank maps. Legal only after {@link #awaitStorage()}: the
     * asynchronous column fills resolve ids through them until then. The
     * writer calls this right after the storage join so the maps are
     * collectable during the HDF5 emission (BG-115).
     */
    void releaseRankMaps() {
        entityIds = null;
        predicateIds = null;
        literalIds = null;
    }

    private ConcurrentHashMap<Node, Long> map(ConcurrentHashMap<Node, Long> m) {
        if (m == null) {
            throw new IllegalStateException("Rank maps already released (releaseRankMaps)");
        }
        return m;
    }

    long locateGraph(Node element) {
        return DictionaryIds.require(idOf(map(entityIds), element), "Graph", element);
    }

    long locateSubject(Node element) {
        return DictionaryIds.require(idOf(map(entityIds), element), "Subject", element);
    }

    long locatePredicate(Node element) {
        return DictionaryIds.require(idOf(map(predicateIds), element), "Predicate", element);
    }

    long locateObject(Node element) {
        return DictionaryIds.require(DictionaryIds.objectId(element,
                n -> idOf(map(literalIds), n), n -> idOf(map(entityIds), n), maxEntityId), "Object", element);
    }

    /** The map's id, or -1 for a miss (the shared rule reports it). */
    private static long idOf(ConcurrentHashMap<Node, Long> m, Node element) {
        Long c = m.get(element);
        return (c == null) ? -1 : c;
    }

    long getNumberOfQuads() { return numQuads; }
    long getNumberOfGraphs() { return maxEntityId; }
    long getNumberOfSubjects() { return maxEntityId; }
    long getNumberOfPredicates() { return numPredicates; }
    long getNumberOfObjects() { return maxEntityId + numLiterals; }

    /** Test access: the storage dictionary for map-vs-locate verification. */
    DictionaryWriter entitiesDictionary() throws IOException {
        awaitStorage();
        return entitiesdict;
    }

    DictionaryWriter predicatesDictionary() throws IOException {
        awaitStorage();
        return predicatesdict;
    }

    DictionaryWriter literalsDictionary() throws IOException {
        awaitStorage();
        return literalsdict;
    }

    // ------------------------------------------------------------------
    // storage
    // ------------------------------------------------------------------

    /** Joins the async dictionary builds and column fills. Idempotent. */
    synchronized void awaitStorage() throws IOException {
        if (entitiesdict == null) {
            logger.info("Waiting for background dictionary encoding and columnar id lists...");
            long phase = System.nanoTime();
            entitiesdict = join(entitiesTask, "build 'entities' dictionary");
            predicatesdict = join(predicatesTask, "build 'predicates' dictionary");
            literalsdict = join(literalsTask, "build 'literals' dictionary");
            graphs = join(graphsTask, "populate 'graphs' id list");
            subjects = join(subjectsTask, "populate 'subjects' id list");
            objects = join(objectsTask, "populate 'objects' id list");
            logger.info("Dictionary storage complete ({} ms wait): {} graphs, {} subjects, {} objects in columnar lists",
                    (System.nanoTime() - phase) / 1_000_000L,
                    graphs.getNumEntries(), subjects.getNumEntries(), objects.getNumEntries());
        }
    }

    /** Same group layout and gating as the other in-memory writers. */
    void add(WritableGroup group) throws IOException {
        awaitStorage();
        WritableGroup dictionary = group.putGroup(name);
        if (entitiesdict.getNumberOfNodes() > 0) entitiesdict.add(dictionary);
        if (predicatesdict.getNumberOfNodes() > 0) predicatesdict.add(dictionary);
        if (literalsdict.getNumberOfNodes() > 0) literalsdict.add(dictionary);
        if (graphs.getNumEntries() > 0) {
            graphs.add(dictionary);
            subjects.add(dictionary);
            objects.add(dictionary);
        }
    }

    private static <T> T join(ForkJoinTask<T> task, String what) throws IOException {
        return Futures.join(task, what);
    }
}
