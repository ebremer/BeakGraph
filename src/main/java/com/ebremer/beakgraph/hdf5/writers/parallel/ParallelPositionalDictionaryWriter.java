package com.ebremer.beakgraph.hdf5.writers.parallel;

import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.lib.NodeSorter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.DictionarySection;
import com.ebremer.beakgraph.hdf5.writers.MultiTypeDictionaryWriter;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-threaded twin of
 * {@link com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriter}: the
 * same Monolithic Entity Dictionary with columnar ID lists, but the three
 * independent sub-dictionaries (entities, predicates, literals) are built
 * concurrently, and the three columnar ID lists (graphs, subjects, objects)
 * are populated concurrently - with the per-node dictionary lookups fanned out
 * across the writer's pool and only the append-only bit-packed writes kept
 * sequential. Every buffer, name, width, and ordering matches the sequential
 * writer, so given the same parsed quads the resulting HDF5 groups are
 * identical.
 *
 * @author Erich Bremer
 */
public class ParallelPositionalDictionaryWriter implements GSPODictionary, AutoCloseable, DictionaryWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParallelPositionalDictionaryWriter.class);
    private final DictionaryWriter entitiesdict;
    private final DictionaryWriter predicatesdict;
    private final DictionaryWriter literalsdict;
    private final long numQuads;
    private final String name;
    private Quad[] quads;
    private final long maxEntityId;

    // Columnar ID Storage
    private final BitPackedUnSignedLongBuffer graphs;
    private final BitPackedUnSignedLongBuffer subjects;
    private final BitPackedUnSignedLongBuffer objects;

    public ParallelPositionalDictionaryWriter(ParallelPositionalDictionaryWriterBuilder builder, ForkJoinPool pool) throws IOException {
        this.name = builder.getName();
        this.numQuads = builder.getNumberOfQuads();
        this.quads = builder.getQuads();

        Stats stats = builder.getStats();
        logger.debug("{}", stats);

        // 1-3. The three sub-dictionaries read disjoint node sets and write
        // disjoint buffers, so they build concurrently; each internal
        // NodeSorter.parallelSort forks into this same pool, keeping the whole
        // stage inside the -cores bound.
        ForkJoinTask<DictionaryWriter> entitiesTask = pool.submit(() -> new MultiTypeDictionaryWriter.Builder()
                .setName("entities")
                .setNodes(builder.getEntities())
                .setStats(stats)
                .section(DictionarySection.ENTITIES)
                .build());
        ForkJoinTask<DictionaryWriter> predicatesTask = pool.submit(() -> new MultiTypeDictionaryWriter.Builder()
                .setName("predicates")
                .setNodes(builder.getPredicates())
                .setStats(stats)
                .section(DictionarySection.PREDICATES)
                .build());
        MultiTypeDictionaryWriter.Builder literalsBuilder = new MultiTypeDictionaryWriter.Builder()
                .setName("literals")
                .setNodes(builder.getLiterals())
                .setDataTypes(builder.getDataTypes())
                .setStats(stats)
                .section(DictionarySection.LITERALS);

        if (stats.numTripleTerms > 0) {
            // Triple-term component ids resolve against COMPLETED entities and
            // predicates dictionaries (CHANGELOG.md "Format v5 design notes"'s parallel-writer
            // sequencing constraint): join those two first, then build the
            // literals section. Triple-term-free datasets - the case that
            // matters for throughput - keep the historical full concurrency in
            // the else branch.
            entitiesdict = joinDictionary(entitiesTask, "entities");
            predicatesdict = joinDictionary(predicatesTask, "predicates");
            final DictionaryWriter ents = entitiesdict;
            final DictionaryWriter preds = predicatesdict;
            final long maxEnt = entitiesdict.getNumberOfNodes();
            literalsBuilder
                    .setTripleTermEncoder((tt, own) -> encodeTripleTerm(tt, ents, preds, own, maxEnt))
                    .setTripleTermComponentIdBound(maxEnt + builder.getLiterals().size());
            literalsdict = joinDictionary(pool.submit(literalsBuilder::build), "literals");
        } else {
            ForkJoinTask<DictionaryWriter> literalsTask = pool.submit(literalsBuilder::build);
            entitiesdict = joinDictionary(entitiesTask, "entities");
            predicatesdict = joinDictionary(predicatesTask, "predicates");
            literalsdict = joinDictionary(literalsTask, "literals");
        }

        // Cache this for fast offset math in locateObject
        this.maxEntityId = entitiesdict.getNumberOfNodes();

        // 4. Initialize Bit-Packed Buffers for columnar ID lists
        // Determine required bit-widths based on the dictionary sizes
        int gBits = (int) (Math.ceil(MinBits(getNumberOfGraphs() + 1) / 8.0) * 8);
        int sBits = (int) (Math.ceil(MinBits(getNumberOfSubjects() + 1) / 8.0) * 8);
        int oBits = (int) (Math.ceil(MinBits(getNumberOfObjects() + 1) / 8.0) * 8);

        this.graphs = new BitPackedUnSignedLongBuffer(Path.of("graphs"), null, 0, gBits);
        this.subjects = new BitPackedUnSignedLongBuffer(Path.of("subjects"), null, 0, sBits);
        this.objects = new BitPackedUnSignedLongBuffer(Path.of("objects"), null, 0, oBits);

        // 5. Populate ID lists from the unique sets collected by the Builder.
        // The three lists are independent (the dictionaries are read-only from
        // here on), so they run concurrently.
        logger.info("Populating columnar ID lists...");
        ForkJoinTask<?> graphsTask = pool.submit(() -> populate(builder.getUniqueGraphs(), this::locateGraph, graphs));
        ForkJoinTask<?> subjectsTask = pool.submit(() -> populate(builder.getUniqueSubjects(), this::locateSubject, subjects));
        ForkJoinTask<?> objectsTask = pool.submit(() -> populate(builder.getUniqueObjects(), this::locateObject, objects));
        joinColumn(graphsTask, "graphs");
        joinColumn(subjectsTask, "subjects");
        joinColumn(objectsTask, "objects");
        logger.info("Columnar ID lists populated");
    }

    /**
     * Sorts one unique-node set, resolves each node's id in parallel, then
     * appends the ids in order (the bit-packed buffer is append-only, so the
     * write itself must stay sequential) and finalizes the buffer for reading.
     */
    private static void populate(Set<Node> nodes, ToLongFunction<Node> locator, BitPackedUnSignedLongBuffer target) {
        Node[] sorted = nodes.toArray(Node[]::new);
        NodeSorter.parallelSort(sorted);   // per-sort memoizing comparator (BG-249)
        long[] ids = new long[sorted.length];
        IntStream.range(0, sorted.length).parallel().forEach(i -> ids[i] = locator.applyAsLong(sorted[i]));
        for (long id : ids) {
            target.writeLong(id);
        }
        target.prepareForReading();
    }

    /**
     * Triple-term component resolution (CHANGELOG.md "Format v5 design notes"): entities and
     * predicates are complete when this runs (the sequencing branch above);
     * literal and nested-triple-term objects resolve through the literals
     * section's own already-sorted ranks, offset into the object space.
     */
    private static long[] encodeTripleTerm(Node tt, DictionaryWriter ents, DictionaryWriter preds,
                                           Dictionary ownSection, long maxEntityId) {
        org.apache.jena.graph.Triple t = tt.getTriple();
        long s = ((Dictionary) ents).locate(t.getSubject());
        long p = ((Dictionary) preds).locate(t.getPredicate());
        Node o = t.getObject();
        long oid;
        if (o.isLiteral() || o.isTripleTerm()) {
            long lid = ownSection.locate(o);
            oid = (lid > 0) ? lid + maxEntityId : -1;
        } else {
            oid = ((Dictionary) ents).locate(o);
        }
        if (s < 1 || p < 1 || oid < 1) {
            throw new IllegalStateException("Cannot resolve triple-term components (not in dictionaries): "
                    + tt + " (s=" + s + ", p=" + p + ", o=" + oid + ")");
        }
        return new long[]{s, p, oid};
    }

    private static DictionaryWriter joinDictionary(ForkJoinTask<DictionaryWriter> task, String which) throws IOException {
        try {
            return task.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while building '" + which + "' dictionary", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException("Failed to build '" + which + "' dictionary", cause);
        }
    }

    private static void joinColumn(ForkJoinTask<?> task, String which) throws IOException {
        try {
            task.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while populating '" + which + "' id list", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException("Failed to populate '" + which + "' id list", cause);
        }
    }

    public Quad[] getQuads() {
        return quads;
    }

    /**
     * Drops this writer's reference to the parsed quad array. Called by
     * {@link ParallelHDF5Writer} once the per-quad id tuples have been
     * resolved: from that point the index builds work on ids only, and the
     * Quad wrappers (the nodes stay alive inside the dictionaries) can be
     * reclaimed before the two index sorts allocate.
     */
    public void releaseQuads() {
        this.quads = null;
    }

    public long getNumberOfQuads() {
        return numQuads;
    }

    public long getNumberOfGraphs() {
        return maxEntityId;
    }

    public long getNumberOfSubjects() {
        return maxEntityId;
    }

    public long getNumberOfPredicates() {
        return predicatesdict.getNumberOfNodes();
    }

    public long getNumberOfObjects() {
        return maxEntityId + literalsdict.getNumberOfNodes();
    }

    @Override
    public long locateGraph(Node element) {
        long c = ((Dictionary) entitiesdict).locate(element);
        if (c > 0) return c;
        throw new IllegalStateException("Cannot resolve Graph (not in dictionary): " + element);
    }

    @Override
    public long locateSubject(Node element) {
        long c = ((Dictionary) entitiesdict).locate(element);
        if (c > 0) return c;
        throw new IllegalStateException("Cannot resolve Subject (not in dictionary): " + element);
    }

    @Override
    public long locatePredicate(Node element) {
        long c = ((Dictionary) predicatesdict).locate(element);
        if (c > 0) return c;
        throw new IllegalStateException("Cannot resolve Predicate (not in dictionary): " + element);
    }

    @Override
    public long locateObject(Node element) {
        if (element.isLiteral() || element.isTripleTerm()) {
            long c = ((Dictionary) literalsdict).locate(element);
            if (c > 0) return c + maxEntityId; // Offset by Entity block size
        } else {
            long c = ((Dictionary) entitiesdict).locate(element);
            if (c > 0) return c;
        }
        // Consistent with the other locate* methods: during a write every quad's nodes
        // are already in the dictionary, so a miss is a build-invariant violation.
        throw new IllegalStateException("Cannot resolve Object (not in dictionary): " + element);
    }

    @Override
    public void add(WritableGroup group) {
        WritableGroup dictionary = group.putGroup(name);

        // Add Sub-dictionaries
        if (entitiesdict.getNumberOfNodes() > 0) entitiesdict.add(dictionary);
        if (predicatesdict.getNumberOfNodes() > 0) predicatesdict.add(dictionary);
        if (literalsdict.getNumberOfNodes() > 0) literalsdict.add(dictionary);

        // Add columnar ID lists whenever any quads are stored (see the
        // sequential writer for why this gates on the lists, not numQuads).
        if (graphs.getNumEntries() > 0) {
            graphs.add(dictionary);
            subjects.add(dictionary);
            objects.add(dictionary);
        }
    }

    @Override
    public void close() {
        // Implementation for AutoCloseable if needed
    }

    // --- Interface Boilerplate / Unsupported Methods ---

    @Override public Node extractGraph(long id) { throw new UnsupportedOperationException(); }
    @Override public Node extractSubject(long id) { throw new UnsupportedOperationException(); }
    @Override public Node extractPredicate(long id) { throw new UnsupportedOperationException(); }
    @Override public Node extractObject(long id) { throw new UnsupportedOperationException(); }
    @Override public long getNumberOfNodes() { throw new UnsupportedOperationException(); }
    @Override public List<Node> getNodes() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamSubjects() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamPredicates() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamObjects() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamGraphs() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getGraphs() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getSubjects() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getPredicates() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getObjects() { throw new UnsupportedOperationException(); }
}
