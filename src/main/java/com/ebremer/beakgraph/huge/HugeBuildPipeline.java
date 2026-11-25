package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.Futures;
import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.CdtTerms;
import com.ebremer.beakgraph.core.lib.NodeSorter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.core.lib.TermKey;
import com.ebremer.beakgraph.core.lib.TripleTerms;
import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.DictionarySection;
import com.ebremer.beakgraph.huge.HugeRecords.IdQuad;
import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import com.ebremer.beakgraph.core.lib.RelativeIris;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The disk-based BeakGraph build: parse once, spill per-column (term, row)
 * records, external-sort them, merge the sorted columns once into the
 * distinct dictionaries and every row's ids (no random lookups, and no term
 * read twice), zip the id columns into encoded quads, external-sort those
 * twice for the GSPO/GPOS indexes, and stream every buffer into the HDF5
 * file. With a stage pool and sorters that can be read by range
 * ({@link RangeSorter}: -method 4 and 5) the merge runs a range of the term
 * order at a time, as many ranges at once as the pool has threads, and the
 * dictionaries' terms are decoded and their strings compressed ahead of the
 * threads that encode them; without (-method 1) it is the one merge of the
 * whole columns, on the calling thread. RAM stays bounded by the sorter batch sizes (a
 * record cap AND a byte budget per term sorter, BG-125) plus the small
 * in-memory populations (predicates, datatypes, language tags, VoID
 * statistics) regardless of quad count - with one exception: a JSON-LD
 * source has no streaming parser and is expanded whole in memory before its
 * first quad reaches the sorters, so for JSON-LD the bound is per document
 * (BG-425; {@link RdfSources#warnIfMaterialized}).
 *
 * <p>Quad-level transforms (default-graph rewrite, relative-IRI handling,
 * numeric canonicalization, spatial/feature augmentation, VoID metadata) mirror
 * {@link com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder}.
 * The ONE deliberate divergence: blank nodes are NOT relabelled to
 * first-appearance b%020d form (that map is unbounded RAM). Labels only order
 * bnodes among themselves - the format stores bnodes by dictionary rank and
 * readers regenerate labels from ids - so the output is isomorphic, not
 * byte-identical, to the RAM writer's.
 *
 * @author Erich Bremer
 */
public final class HugeBuildPipeline implements AutoCloseable {
    static {
        // Deterministic datatype registration before any document is parsed.
        com.ebremer.halcyon.hilbert.WKTDatatype.register();
    }


    private static final Logger logger = LoggerFactory.getLogger(HugeBuildPipeline.class);

    // Same sentinel base as the RAM builder (one constant, RelativeIris):
    // relative references resolve against it during parsing and are stripped
    // back to relative form for storage. Public: parallel-ingest
    // implementations must parse against the same base.
    public static final String REL_BASE = RelativeIris.SENTINEL_BASE;

    /** Source documents; more than one means a -merge build into a single store. */
    private final List<File> sources;
    private final boolean spatial;
    private final boolean features;
    private final Path workDir;

    // ---- Pass A state ----
    private final Stats stats = new Stats();
    private final TreeSet<String> dataTypes = new TreeSet<>();
    private final TreeSet<String> langSet = new TreeSet<>();
    // True when any literal carries a base direction (rdf:dirLangString) -
    // gates the langDirs column, exactly as langSet gates langs/langTags.
    private boolean langDirSeen = false;
    // Predicates are the one population kept in RAM (real-world predicate counts
    // are tiny next to entities/literals); they get temp ids during the parse
    // and final rank ids once the set is complete.
    private final HashMap<Node, Long> predTempIds = new HashMap<>();
    private final ArrayList<Node> predByTempId = new ArrayList<>();
    /** Null when voidMode == NONE (the default): no statistics graph is written. */
    private BGVoIDSD xvoid;
    private final com.ebremer.beakgraph.core.VoidMode voidMode;
    /**
     * A pluggable Pass A: parse the sources with whatever concurrency the
     * implementation likes, delivering TRANSFORMED quads (default-graph
     * rewrite, bnode scoping, relativize, numeric canonicalization,
     * spatial/feature augmentation already applied - the public static
     * helpers on this class plus {@link SpatialAugmenter} are the shared
     * implementations) in batches to the sink. The sink is the pipeline's one
     * serial section: it assigns row numbers and feeds the sorters, keeping
     * the positional predicate column's entry-i-equals-row-i invariant
     * without any post-sort. The plaid writer (-method 5) plugs in per-file
     * parallel parsing here.
     */
    public interface ParallelIngest {
        /**
         * @param sourceRoot root the documents' stored relative references are
         *                   taken from when merging (may be null: common
         *                   ancestor); see {@link RelativeIris#parseBase}
         */
        void run(List<File> sources, File sourceRoot, boolean spatial, boolean features,
                 BGVoIDSD voidStats, BatchSink sink) throws IOException;

        interface BatchSink {
            /**
             * Appends a batch of rows to the store. {@code sourceQuads} =
             * how many of them were parsed from a document (vs derived
             * spatial/feature quads); drives the numQuads attribute and
             * progress logging. Thread-safe; callers may commit concurrently.
             */
            void commit(List<Quad> batch, long sourceQuads) throws IOException;
        }
    }

    private RecordSorter<TermRow> gSorter;
    private RecordSorter<TermRow> sSorter;
    private RecordSorter<TermRow> oSorter;
    // Interior terms of RDF 1.2 triple terms (CHANGELOG.md "Format v5 design notes"): they need
    // DICTIONARY entries but never role-list membership, so they spill to
    // dedicated sorters merged into the dictionary derivation only - never into
    // the positional columns the id joins and columnar lists are built from.
    // Lazily created on the first triple term, so triple-term-free builds keep
    // their exact temp-file footprint.
    private RecordSorter<TermRow> iEntSorter;
    private RecordSorter<TermRow> iLitSorter;
    // -arrays-only (format v8): the leaves and inner sequences of the quads' objects held only in the
    // arrays, each with its quad's row. They take no place in the dictionaries: the merge reads them only
    // to find which also have one, which makes the sequence's class shared (SPECIFICATIONS.md 7.6e).
    private RecordSorter<TermRow> packedSorter;
    /** The graph and the subject keys of the last row, for the rows that repeat them (see {@link #sharedKey}). */
    private TermKey graphKey;
    private TermKey subjectKey;
    private RecordFile<Long> pTempFile;
    private final SorterProvider provider;
    /** Non-null: independent stage groups run concurrently on it (-method 4). */
    private final java.util.concurrent.ExecutorService stagePool;
    /** Non-null: Pass A runs through it instead of the sequential loop (-method 5). */
    private final ParallelIngest parallelIngest;
    private File sourceRoot;
    /** {@code -arrays}: on with these settings, or off (null). */
    private com.ebremer.beakgraph.hdf5.writers.SequenceArrays.Options arrays;
    /** {@code -seqindex}: write the element index (RDFARRY.md step 8). */
    private boolean sequenceIndex;
    /** Rows a range of the term order holds at least: fewer rows than two of them are merged as one range. */
    private long minRangeRows = Long.getLong("beakgraph.build.rangeRows", 1L << 18);

    /**
     * Rows a range of the term order holds at least (default 262,144, or the
     * {@code beakgraph.build.rangeRows} property): with a stage pool and
     * sorters that have ranges the sorted columns are merged into the
     * dictionaries in up to four ranges a thread, but never in more than this
     * leaves rows for. A range leaves five small files behind it, to be
     * opened again by the stages that follow, so ranges of few rows cost
     * more than their merge saves (a store of 4 million rows on 16 threads:
     * 1.2 s to encode its dictionaries from 64 ranges, 0.5 s from 15). Call
     * before {@link #run}.
     */
    public HugeBuildPipeline setMinRangeRows(long rows) {
        if (rows < 1) {
            throw new IllegalArgumentException("minRangeRows must be >= 1, got " + rows);
        }
        this.minRangeRows = rows;
        return this;
    }

    /** Ranges of the term order the sorted columns were merged in; 0 before {@link #run} has come to it. */
    public int ranges() {
        return rangesMerged;
    }

    private volatile int rangesMerged = 0;

    /** Merge mode: root the documents' stored relative references are taken from (see {@link RelativeIris#parseBase}). */
    public HugeBuildPipeline setSourceRoot(File root) {
        this.sourceRoot = root;
        return this;
    }

    /** {@code -arrays} (format v7, RDFARRY.md §3.5): the native arrays, as the in-memory engines write them; null for none. */
    public HugeBuildPipeline setArrays(com.ebremer.beakgraph.hdf5.writers.SequenceArrays.Options options) {
        this.arrays = options;
        return this;
    }

    /** {@code -seqindex} (format v7, RDFARRY.md step 8): the element index, as the in-memory engines write it. */
    public HugeBuildPipeline setSequenceIndex(boolean on) {
        this.sequenceIndex = on;
        return this;
    }

    /** IRI of the sd:Dataset resource the statistics graph describes; call before {@link #run} (BG-109). */
    public HugeBuildPipeline setVoidDatasetIri(String iri) {
        this.xvoid = BGVoIDSD.forMode(voidMode, iri);
        return this;
    }
    private long rows = 0;
    private long parsedQuads = 0;

    // ---- Build products (owned; released in close()) ----
    private final List<AutoCloseable> resources = new ArrayList<>();

    HugeBuildPipeline(List<File> sources, boolean spatial, boolean features,
                      com.ebremer.beakgraph.core.VoidMode voidMode, Path workDir,
                      int termSpillBatch, int idSpillBatch, int mergeFanIn) {
        this(sources, spatial, features, voidMode, workDir,
                SorterProvider.sequential(termSpillBatch, idSpillBatch, mergeFanIn), null, null);
    }

    HugeBuildPipeline(List<File> sources, boolean spatial, boolean features,
                      com.ebremer.beakgraph.core.VoidMode voidMode, Path workDir,
                      int termSpillBatch, int idSpillBatch, int mergeFanIn, long termSpillBytes) {
        this(sources, spatial, features, voidMode, workDir,
                SorterProvider.sequential(termSpillBatch, idSpillBatch, mergeFanIn, termSpillBytes), null, null);
    }

    /**
     * The injectable form: {@code provider} supplies every sorter the build
     * uses, and a non-null {@code stagePool} runs independent stage groups
     * (column sorts, the ranges of the dictionary merge, dictionary encodes,
     * id columns) concurrently.
     * This is how the hugeUltra writer (-method 4) turbocharges the pipeline
     * without changing its flow or output.
     */
    public HugeBuildPipeline(List<File> sources, boolean spatial, boolean features,
                             com.ebremer.beakgraph.core.VoidMode voidMode, Path workDir,
                             SorterProvider provider, java.util.concurrent.ExecutorService stagePool) {
        this(sources, spatial, features, voidMode, workDir, provider, stagePool, null);
    }

    public HugeBuildPipeline(List<File> sources, boolean spatial, boolean features,
                             com.ebremer.beakgraph.core.VoidMode voidMode, Path workDir,
                             SorterProvider provider, java.util.concurrent.ExecutorService stagePool,
                             ParallelIngest parallelIngest) {
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("At least one source document is required");
        }
        this.sources = List.copyOf(sources);
        this.spatial = spatial;
        this.features = features;
        this.voidMode = voidMode;
        this.xvoid = BGVoIDSD.forMode(voidMode, com.ebremer.beakgraph.Params.VOID_DATASET_IRI);
        this.workDir = workDir;
        this.provider = provider;
        this.stagePool = stagePool;
        this.parallelIngest = parallelIngest;
        this.gSorter = track(provider.termSorter(workDir, "gcol"));
        this.sSorter = track(provider.termSorter(workDir, "scol"));
        this.oSorter = track(provider.termSorter(workDir, "ocol"));
    }

    // Synchronized: concurrent stages register their resources too.
    private synchronized <T extends AutoCloseable> T track(T resource) {
        resources.add(resource);
        return resource;
    }

    @FunctionalInterface
    private interface Stage {
        void run() throws IOException;
    }

    /**
     * Runs independent stages sequentially (no pool: -method 1 behaviour,
     * bounded RAM) or concurrently on the stage pool (-method 4). Every stage
     * is awaited before returning. Stages are watched in COMPLETION order, so
     * the first failure surfaces the moment it happens; the siblings are then
     * cancelled (interrupted) and waited for before the failure is thrown -
     * they used to run to the end of their multi-hour sorts first, and the
     * pipeline never raced ahead of a stage still touching its files (BG-213).
     */
    private void runStages(String what, Stage... stages) throws IOException {
        if (stagePool == null) {
            for (Stage s : stages) {
                s.run();
            }
            return;
        }
        java.util.concurrent.BlockingQueue<TrackedTask<Void>> completed = new java.util.concurrent.LinkedBlockingQueue<>();
        List<TrackedTask<Void>> tasks = new ArrayList<>(stages.length);
        for (Stage s : stages) {
            tasks.add(TrackedTask.submit(stagePool, () -> {
                s.run();
                return null;
            }, completed::add));
        }
        IOException first = null;
        for (int i = 0; i < stages.length && first == null; i++) {
            try {
                completed.take().get();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                first = new IOException("Interrupted during " + what, ex);
            } catch (ExecutionException ex) {
                Throwable c = ex.getCause();
                c = unwrapPoolWrappers(c);
                first = (c instanceof IOException io) ? io
                        : (c instanceof UncheckedIOException uio) ? uio.getCause()
                        : new IOException(what + " failed", c);
            } catch (java.util.concurrent.CancellationException ex) {
                first = new IOException(what + " cancelled", ex);
            }
        }
        if (first != null) {
            for (TrackedTask<Void> t : tasks) {
                t.abandon();
            }
            throw first;
        }
    }

    /**
     * The exception a stage threw, without the pool's wrappers: ForkJoinTask.get()
     * re-creates the recorded exception through its (Throwable) constructor to
     * attach the caller's stack (same class, message = cause.toString()), and a
     * Callable's checked exception may also arrive inside a bare RuntimeException.
     */
    static Throwable unwrapPoolWrappers(Throwable c) {
        while (c != null && c.getCause() != null) {
            Throwable cause = c.getCause();
            boolean bareRuntime = c.getClass() == RuntimeException.class;
            boolean recreated = c.getClass() == cause.getClass() && java.util.Objects.equals(c.getMessage(), cause.toString());
            if (!bareRuntime && !recreated) {
                break;
            }
            c = cause;
        }
        return c;
    }

    /** Cooperative cancellation point for the long loops: an interrupted stage stops between records. */
    public static void checkCancelled(String what) throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("Cancelled: " + what);
        }
    }

    /** Runs the whole build, producing the finished HDF5 file at {@code tmpH5}. */
    public void run(Path tmpH5) throws IOException {
        run(tmpH5, com.ebremer.beakgraph.store.Container.HDF5);
    }

    /** Runs the whole build, producing the finished store at {@code tmpH5} in {@code container}. */
    public void run(Path tmpH5, com.ebremer.beakgraph.store.Container container) throws IOException {
        ingest();

        // ---- Predicates: final rank ids from the in-RAM population ----
        Node[] sortedPreds = predByTempId.toArray(Node[]::new);
        NodeSorter.parallelSort(sortedPreds);
        long numPredicates = sortedPreds.length;
        long[] tempToFinal = new long[(int) Math.min(Integer.MAX_VALUE, predByTempId.size())];
        // Kept beyond the remap: triple-term predicate components resolve
        // through this map during the component-reference join (HugeTripleTerms).
        HashMap<Node, Long> predFinalIds = new HashMap<>();
        {
            for (int i = 0; i < sortedPreds.length; i++) {
                predFinalIds.put(sortedPreds[i], (long) i + 1); // 1-based
            }
            for (int t = 0; t < predByTempId.size(); t++) {
                tempToFinal[t] = predFinalIds.get(predByTempId.get(t));
            }
        }

        // ---- The sorted term columns: ingestion ended, the runs merged down to one merge's worth ----
        logger.info("Sorting {} rows per column (external merge sort{})...", rows,
                stagePool == null ? "" : ", 3 columns concurrently");
        final Column[] columns = new Column[COLUMNS];
        columns[G] = track(new Column(gSorter));
        columns[S] = track(new Column(sSorter));
        columns[O] = track(new Column(oSorter));
        gSorter = null;
        sSorter = null;
        oSorter = null;
        // Interior triple-term components (absent for triple-term-free builds).
        if (iEntSorter != null) {
            columns[INTERIOR_ENTITIES] = track(new Column(iEntSorter));
            columns[INTERIOR_LITERALS] = track(new Column(iLitSorter));
            iEntSorter = null;
            iLitSorter = null;
        }
        // -arrays-only: the parts of the sequences held only in the arrays (absent without one).
        if (packedSorter != null) {
            columns[PACKED] = track(new Column(packedSorter));
            packedSorter = null;
        }
        List<Stage> sorts = new ArrayList<>();
        for (Column c : columns) {
            if (c != null) {
                sorts.add(c::sort);
            }
        }
        runStages("column sort", sorts.toArray(Stage[]::new));

        // ---- Distinct sorted dictionaries on disk, and every row's place in them:
        // one merge of the columns, a range of the term order at a time ----
        final List<TermRow> bounds = chooseBounds(columns);
        final int ranges = bounds.size() + 1;
        this.rangesMerged = ranges;
        final RangeOutput[] merged = new RangeOutput[ranges];
        int atOnce = parallelism();
        for (Column c : columns) {
            if (c != null) {
                atOnce = Math.min(atOnce, c.maxOpen());
            }
        }
        if (ranges > 1) {
            logger.info("Merging the columns into dictionaries in {} ranges of the term order, {} at a time",
                    ranges, Math.min(atOnce, ranges));
        }
        runRanges("dictionary merge", ranges, atOnce, r -> merged[r] = mergeRange(r,
                (r == 0) ? null : bounds.get(r - 1), (r == ranges - 1) ? null : bounds.get(r), columns));
        for (Column c : columns) {
            if (c != null) {
                c.close();   // the runs go: nothing reads the terms a second time
            }
        }
        List<RecordFile<Node>> entityParts = new ArrayList<>(ranges);
        List<RecordFile<Node>> literalParts = new ArrayList<>(ranges);
        // What a range's codes are counted from: the entities and the literals of the ranges before it.
        final long[] entityBase = new long[ranges];
        final long[] literalBase = new long[ranges];
        long entitiesSoFar = 0;
        long literalsSoFar = 0;
        long sequencesSoFar = 0;
        java.util.BitSet topSequences = new java.util.BitSet();
        for (int r = 0; r < ranges; r++) {
            java.util.BitSet top = merged[r].topSequences;
            for (int i = top.nextSetBit(0); i >= 0; i = top.nextSetBit(i + 1)) {
                topSequences.set(Math.toIntExact(sequencesSoFar + i));
            }
            sequencesSoFar += merged[r].sequences;
            entityParts.add(merged[r].entities);
            literalParts.add(merged[r].literals);
            entityBase[r] = entitiesSoFar;
            literalBase[r] = literalsSoFar;
            entitiesSoFar += merged[r].entities.count();
            literalsSoFar += merged[r].literals.count();
            for (int c = G; c <= O; c++) {
                columns[c].merged += merged[r].ids[c].count();
            }
        }
        for (int c = G; c <= O; c++) {
            // A run cut short ends a range early without a sound (BG-129): the ranges must hold every row.
            if (columns[c].merged != rows) {
                throw new IOException("Sorted " + ROLES[c] + " column holds " + columns[c].merged
                        + " rows where " + rows + " were added (a spill run is truncated)");
            }
        }
        final long numSequences = sequencesSoFar;
        final RecordSegments<Node> entFile = new RecordSegments<>(entityParts);
        final RecordSegments<Node> litFile = new RecordSegments<>(literalParts);
        long numEntities = entFile.count();
        long numLiterals = litFile.count();
        for (int r = 0; r < ranges; r++) {
            literalBase[r] += numEntities;   // literal ids follow the entities'
        }
        long numObjects = numEntities + numLiterals;
        logger.info("Dictionary populations: {} entities, {} predicates, {} literals",
                numEntities, numPredicates, numLiterals);

        // ---- Encode the three dictionary sections (independent; concurrent with a pool) ----
        // Triple-term component machinery, only when the build saw any: refs
        // spill during the literals encode, and the join below resolves them
        // against entFile/litFile (complete by now) + the predicate rank map.
        final HugeTripleTerms ttSupport = (stats.numTripleTerms > 0)
                ? track(new HugeTripleTerms(provider, workDir, predFinalIds))
                : null;
        // Sequence machinery likewise (format v7): element references spill
        // during the literals encode and resolve against the same files.
        final HugeSequences seqSupport = (stats.numSequences > 0)
                ? track(new HugeSequences(provider, workDir))
                : null;
        // -arrays: the encode probes each top-level sequence as it reaches it
        // (the sequences are the literals' suffix, so the first is object
        // numEntities + numLiterals - numSequences + 1).
        final HugeArrays arraysSupport = (arrays != null && seqSupport != null)
                ? track(new HugeArrays(arrays, topSequences, numEntities + numLiterals - numSequences + 1, workDir, 64))
                : null;
        if (arraysSupport != null) {
            seqSupport.setArrays(arraysSupport);
        }
        if (seqSupport != null) {
            seqSupport.setIndex(sequenceIndex);
        }
        final StreamingDictionaryWriter[] dicts = new StreamingDictionaryWriter[3];
        // With a pool, a section's terms are decoded and its strings compressed
        // by the pool's threads, this many pieces ahead of the stage's own
        // thread, which takes them in order.
        final int ahead = (stagePool == null) ? 0 : parallelism() + 2;
        runStages("dictionary encode",
                () -> {
                    if (numEntities > 0) {
                        dicts[0] = track(new StreamingDictionaryWriter(workDir, "entities", numEntities, stats,
                                DictionarySection.ENTITIES, new TreeSet<>(), new TreeSet<>(), false, null,
                                stagePool, ahead));
                        try (var s = sortedTerms(entFile, ahead)) {
                            dicts[0].encode(s);
                        }
                    }
                },
                () -> {
                    if (numPredicates > 0) {
                        dicts[1] = track(new StreamingDictionaryWriter(workDir, "predicates", numPredicates, stats,
                                DictionarySection.PREDICATES, new TreeSet<>(), new TreeSet<>(), false, null));
                        dicts[1].encode(Arrays.asList(sortedPreds).iterator());
                    }
                },
                () -> {
                    if (numLiterals > 0) {
                        dicts[2] = track(new StreamingDictionaryWriter(workDir, "literals", numLiterals, stats,
                                DictionarySection.LITERALS,
                                dataTypes, langSet, langDirSeen, ttSupport, stagePool, ahead));
                        if (seqSupport != null) {
                            dicts[2].setSequenceSupport(seqSupport);
                        }
                        try (var s = sortedTerms(litFile, ahead)) {
                            dicts[2].encode(s);
                        }
                        // Reference join (CHANGELOG.md "Format v5 design notes"): the entity
                        // files are safe to read concurrently with the entities encode stage -
                        // each reader has streams of its own.
                        if (ttSupport != null && ttSupport.count() > 0) {
                            dicts[2].setTripleTermsBuffer(
                                    ttSupport.resolve(entFile, litFile, numEntities, numEntities + numLiterals));
                        }
                        if (seqSupport != null && seqSupport.count() > 0) {
                            dicts[2].setSequenceElementsBuffer(
                                    seqSupport.resolve(entFile, litFile, numEntities, numEntities + numLiterals));
                        }
                        if (arraysSupport != null) {
                            dicts[2].setSequenceArrays(arraysSupport.classNumbers());   // the classes decided
                        }
                    }
                });
        StreamingDictionaryWriter entitiesDict = dicts[0];
        StreamingDictionaryWriter predicatesDict = dicts[1];
        StreamingDictionaryWriter literalsDict = dicts[2];

        // ---- Columnar unique-id lists (same widths as PositionalDictionaryWriter) ----
        int gBits = byteRoundedWidth(numEntities + 1);
        int sBits = byteRoundedWidth(numEntities + 1);
        int oBits = byteRoundedWidth(numObjects + 1);
        SpillBitPackedBuffer graphsList = track(new SpillBitPackedBuffer(workDir.resolve("columnar.graphs"), gBits));
        SpillBitPackedBuffer subjectsList = track(new SpillBitPackedBuffer(workDir.resolve("columnar.subjects"), sBits));
        SpillBitPackedBuffer objectsList = track(new SpillBitPackedBuffer(workDir.resolve("columnar.objects"), oBits));

        // ---- Ids (independent columns; concurrent with a pool): a row's id is its
        // range's count of the terms before it plus the code the merge gave it. Plus the
        // predicate temp->final remap, which depends on nothing here ----
        logger.info("Assigning ids ({} ranges{})...", ranges, stagePool == null ? "" : ", 3 columns concurrently");
        RecordSorter<RowId> gIds = track(provider.rowIdSorter(workDir, "gid", rows, numObjects));
        RecordSorter<RowId> sIds = track(provider.rowIdSorter(workDir, "sid", rows, numObjects));
        RecordSorter<RowId> oIds = track(provider.rowIdSorter(workDir, "oid", rows, numObjects));
        RecordFile<Long> pFinal = track(new RecordFile<>(workDir.resolve("pcol.final"), HugeRecords.VAR_LONG_CODEC));
        runStages("id join",
                () -> assignIds(G, merged, entityBase, literalBase, graphsList, gIds),
                () -> assignIds(S, merged, entityBase, literalBase, subjectsList, sIds),
                () -> assignIds(O, merged, entityBase, literalBase, objectsList, oIds),
                () -> {
                    try (var s = pTempFile.read()) {
                        while (s.hasNext()) {
                            pFinal.append(tempToFinal[Math.toIntExact(s.next())]);
                        }
                    }
                    pFinal.finish();
                });
        entFile.delete();
        litFile.delete();
        pTempFile.delete();
        if (pFinal.count() != rows) {
            throw new IllegalStateException("Predicate column has " + pFinal.count() + " entries for " + rows + " rows");
        }

        // ---- Zip id columns into encoded quads. Only GSPO sorts the raw
        // (duplicate-laden) quads: the GSPO scan below tees each DISTINCT quad
        // into the GPOS sorter, so GPOS sorts the deduplicated set only ----
        logger.info("Encoding {} quads...", rows);
        RecordSorter<IdQuad> gspoSorter = track(provider.quadSorter(workDir, "gspo", Index.GSPO,
                numEntities, numPredicates, numObjects));
        RecordSorter<IdQuad> gposSorter = track(provider.quadSorter(workDir, "gpos", Index.GPOS,
                numEntities, numPredicates, numObjects));
        // -arrays-only: the rows of the quads whose objects, held only in the
        // arrays, share a term with the dictionary, in row order; this pass
        // reads each row's object id, which names the sequence.
        RecordSorter<RowId> sharingByRow = sharingRows(merged, numObjects);
        final java.util.BitSet sharingSequences = new java.util.BitSet();
        final long firstSequenceId = numEntities + numLiterals - numSequences + 1;
        try (var gs = gIds.sorted(); var ss = sIds.sorted(); var os = oIds.sorted(); var ps = pFinal.read();
             var sh = (sharingByRow == null) ? null : sharingByRow.sorted()) {
            RowId sharing = (sh != null && sh.hasNext()) ? sh.next() : null;
            for (long row = 0; row < rows; row++) {
                RowId g = gs.next();
                RowId s = ss.next();
                RowId o = os.next();
                long p = ps.next();
                if (g.row() != row || s.row() != row || o.row() != row) {
                    throw new IllegalStateException("Row misalignment at " + row + ": g=" + g.row()
                            + " s=" + s.row() + " o=" + o.row());
                }
                gspoSorter.add(new IdQuad(g.id(), s.id(), p, o.id()));
                if (sharing != null && sharing.row() == row) {
                    if (o.id() < firstSequenceId) {
                        throw new IllegalStateException("Row " + row + " shares a packed sequence's term, but its object "
                                + o.id() + " is not a sequence");
                    }
                    sharingSequences.set(Math.toIntExact(o.id() - firstSequenceId));
                    while (sharing != null && sharing.row() == row) {
                        sharing = sh.hasNext() ? sh.next() : null;
                    }
                }
            }
        }
        pFinal.delete();
        if (arraysSupport != null) {
            arraysSupport.setSharing(sharingSequences);
        }

        // ---- Indexes ----
        HugeIndexWriter gspo = track(new HugeIndexWriter(workDir, Index.GSPO,
                numEntities, numEntities, numPredicates, numObjects, rows));
        try (var it = gspoSorter.sorted()) {
            gspo.build(teeDistinctTo(it, gposSorter));
        }
        HugeIndexWriter gpos = track(new HugeIndexWriter(workDir, Index.GPOS,
                numEntities, numEntities, numPredicates, numObjects, rows));
        try (var it = gposSorter.sorted()) {
            gpos.build(it);
        }

        // ---- Assemble the store (same names/attributes as HDF5Writer) ----
        logger.info("Writing {} store {}", container, tmpH5);
        try (StreamingHdf5File hdf = StreamingHdf5.create(tmpH5, container)) {
            StreamingHdf5Group hdt = hdf.rootGroup().putGroup(Params.BG);
            hdt.putAttribute(Params.NUM_QUADS, parsedQuads);
            // 8 when -arrays-only holds some sequence only in the arrays, else 7.
            hdt.putAttribute(Params.FORMAT_VERSION_ATTR, (arraysSupport != null && arraysSupport.heldCount() > 0)
                    ? Params.ARRAYS_ONLY_FORMAT_VERSION : Params.FORMAT_VERSION);
            StreamingHdf5Group dictionary = hdt.putGroup(Params.DICTIONARY);
            if (entitiesDict != null) entitiesDict.transferTo(dictionary);
            if (predicatesDict != null) predicatesDict.transferTo(dictionary);
            if (literalsDict != null) literalsDict.transferTo(dictionary);
            if (graphsList.getNumEntries() > 0) {
                // Block-structured, like the index id sequences (SPECIFICATIONS.md §5.1a).
                graphsList.transferBlockedTo(dictionary, "graphs");
                subjectsList.transferBlockedTo(dictionary, "subjects");
                objectsList.transferBlockedTo(dictionary, "objects");
            }
            gspo.transferTo(hdt);
            gpos.transferTo(hdt);
            if (arraysSupport != null) {
                arraysSupport.write(hdt.writable());   // after the indexes, as the in-memory writers write it
            }
        }
        logger.info("{} assembly complete", container);
    }

    // ------------------------------------------------------------------
    // Pass A: parse + transforms + spills
    // ------------------------------------------------------------------

    private void ingest() throws IOException {
        this.pTempFile = track(new RecordFile<>(workDir.resolve("pcol.tmpids"), HugeRecords.VAR_LONG_CODEC));
        if (parallelIngest != null) {
            parallelIngest.run(sources, sourceRoot, spatial, features, xvoid, this::commitBatch);
            finishIngest();
            return;
        }
        for (int i = 0; i < sources.size(); i++) {
            // Blank-node labels are document-scoped in RDF: two merged sources
            // may both say _:b0 and mean different nodes (labels are parsed as
            // given). Prefixing every label with the source ordinal keeps
            // documents from colliding WITHOUT the unbounded RAM map the RAM
            // writer's AlignBnodes uses. The prefix never reaches the output:
            // bnodes are stored by dictionary rank and readers regenerate
            // labels from ids. Single-source builds stay untouched.
            ingestSource(sources.get(i), sources.size() > 1 ? (i + "/") : null,
                    RelativeIris.parseBase(sources.get(i), sources, sourceRoot));
        }
        finishIngest();
    }

    /** VoID/SD metadata quads over ALL sources (when requested), then the p-column seal. */
    private void finishIngest() throws IOException {
        if (xvoid != null) {
            // (mirrors the RAM writer; the prefixes BGVoIDSD sets on the model
            // are presentation-only and irrelevant to quads)
            for (Iterator<org.apache.jena.rdf.model.Statement> it = xvoid.getModel().listStatements(); it.hasNext(); ) {
                Triple ff = it.next().asTriple();
                Quad qqq = canonicalizeNumericObject(Quad.create(Params.BGVOID, ff));
                acceptRow(qqq);
            }
        }
        pTempFile.finish();
        logger.info("Parse complete: {} source quads, {} total rows", parsedQuads, rows);
    }

    /**
     * The parallel-ingest sink: the ONE serial section of Pass A. Row numbers,
     * the positional predicate column, statistics, and the sorter buffers all
     * advance under this lock, so every single-threaded invariant of
     * {@link #acceptRow} holds unchanged; spill sorting/writing still happens
     * on background workers, so the lock is held only for buffer appends.
     */
    private synchronized void commitBatch(List<Quad> batch, long sourceQuads) throws IOException {
        for (Quad q : batch) {
            acceptRow(q);
        }
        long before = parsedQuads;
        parsedQuads += sourceQuads;
        if (before / 1_000_000 != parsedQuads / 1_000_000) {
            logger.info("Loaded {} quads...", parsedQuads);
        }
    }

    private void ingestSource(File input, String bnodeScope, String parseBase) throws IOException {
        logger.info("Parsing {} (disk-based build)", input);
        // Syntax from the file name (TriG, N-Quads, N-Triples, RDF/XML, JSON-LD,
        // Turtle); .gz and .zip are decompressed transparently - one shared rule
        // with the RAM writer and the CLI filter (RdfSources).
        try (RdfSources.OpenedSource opened = RdfSources.open(input)) {
            RdfSources.warnIfMaterialized(input, opened.lang(), logger);   // JSON-LD: whole document in RAM (BG-425)
            AsyncParserBuilder parserBuilder = RdfSources.parser(opened, parseBase, input);
            SpatialAugmenter augmenter = new SpatialAugmenter(features);
            // Spatial tasks run concurrently exactly like the RAM writer, but the
            // completion window is BOUNDED: results are drained and spilled as
            // soon as the window fills instead of accumulating until end of parse.
            final int maxInFlight = Math.max(8, Runtime.getRuntime().availableProcessors() * 4);
            final ArrayDeque<Future<ArrayList<Quad>>> inFlight = new ArrayDeque<>();
            // The quad stream closes before the executor: that aborts and joins
            // the parser thread when the loop throws (BG-100).
            try (ExecutorService scope = Executors.newVirtualThreadPerTaskExecutor();
                 Stream<Quad> quads = parserBuilder.streamQuads()) {
                quads
                    .map(quad -> quad.isDefaultGraph()
                            ? new Quad(Quad.defaultGraphIRI, quad.getSubject(), quad.getPredicate(), quad.getObject())
                            : quad)
                    .map(quad -> scopeBlankNodes(quad, bnodeScope))
                    .map(HugeBuildPipeline::relativize)
                    .map(HugeBuildPipeline::canonicalizeNumericObject)
                    .forEach(quad -> {
                        parsedQuads++;
                        if (parsedQuads % 1_000_000 == 0) {
                            logger.info("Loaded {} quads...", parsedQuads);
                        }
                        try {
                            acceptRow(quad);
                            if (xvoid != null) {
                                xvoid.add(quad);
                            }
                            if (spatial && isGeoLiteral(quad)) {
                                inFlight.add(scope.submit(() -> augmenter.addSpatial(quad)));
                                while (inFlight.size() >= maxInFlight) {
                                    drainOne(inFlight, input);
                                }
                            }
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
                while (!inFlight.isEmpty()) {
                    drainOne(inFlight, input);
                }
            } catch (UncheckedIOException ex) {
                throw new IOException("Failed while parsing/processing RDF source: " + input, ex.getCause());
            } catch (Exception ex) {
                throw new IOException("Failed while parsing/processing RDF source: " + input, ex);
            }
        }
    }

    /** See ingest(): per-document blank-node scoping for -merge builds; no-op when scope is null. */
    public static Quad scopeBlankNodes(Quad q, String scope) {
        if (scope == null) {
            return q;
        }
        Node g = scopeNode(q.getGraph(), scope);
        Node s = scopeNode(q.getSubject(), scope);
        Node o = scopeNode(q.getObject(), scope);
        if (g == q.getGraph() && s == q.getSubject() && o == q.getObject()) {
            return q;
        }
        return new Quad(g, s, q.getPredicate(), o);
    }

    private static Node scopeNode(Node n, String scope) {
        if (com.ebremer.beakgraph.core.lib.CompoundTerms.isCompound(n)) {
            // Blank nodes inside a triple term or a sequence share the quad's
            // document scope - the same prefix keeps inside/outside co-reference intact.
            return com.ebremer.beakgraph.core.lib.CompoundTerms.map(n, c -> scopeNode(c, scope));
        }
        return n.isBlank() ? NodeFactory.createBlankNode(scope + n.getBlankNodeLabel()) : n;
    }

    private void drainOne(ArrayDeque<Future<ArrayList<Quad>>> inFlight, File input) throws IOException {
        Future<ArrayList<Quad>> task = inFlight.poll();
        if (task == null) return;
        ArrayList<Quad> extraQuads = Futures.join(task, "collecting spatial results for " + input);
        for (Quad q : extraQuads) {
            acceptRow(canonicalizeNumericObject(q));
        }
    }

    /** One row of the store: spill each column and account stats (mirrors ProcessQuad). */
    private void acceptRow(Quad quad) throws IOException {
        Node g = quad.getGraph();
        Node s = quad.getSubject();
        Node p = quad.getPredicate();
        Node o = quad.getObject();
        countEntityKind(g, "graph");
        countEntityKind(s, "subject");
        if (o.isLiteral()) {
            collectLiteralStats(o);
        } else if (o.isTripleTerm()) {
            collectTripleTerm(o);
        } else if (o.isNodeSeq()) {
            collectSequence(o, rows);   // the row this quad is about to take
        } else {
            countEntityKind(o, "object");
        }
        com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder.requirePredicate(p);
        stats.numIRI++; // the predicate (RAM counts distinct; only >0 gates buffer allocation)

        long row = rows++;
        graphKey = sharedKey(g, graphKey);
        subjectKey = sharedKey(s, subjectKey);
        gSorter.add(new TermRow(graphKey, row));
        sSorter.add(new TermRow(subjectKey, row));
        oSorter.add(new TermRow(o, row));
        pTempFile.append(predTempIds.computeIfAbsent(p, k -> {
            predByTempId.add(k);
            return (long) (predByTempId.size() - 1);
        }));
    }

    /**
     * The sort key of the row before, where this row's term is the same
     * object - a graph's rows follow one another, and so do a subject's - or
     * a new key: the sort then works the term's place in the order out once
     * for those rows, not once for each.
     */
    private static TermKey sharedKey(Node term, TermKey last) {
        return (last != null && last.node() == term) ? last : new TermKey(term);
    }

    private void countEntityKind(Node n, String position) {
        if (n.isBlank()) {
            stats.numBlankNodes++;
        } else if (n.isURI()) {
            stats.numIRI++;
        } else {
            // Same guard as ProcessQuad: an unstorable node kind aborts the build.
            throw new IllegalStateException("Unexpected " + position + " node type"
                    + com.ebremer.beakgraph.core.lib.SeqTerms.positionNote(n) + ": " + n);
        }
    }

    /**
     * Triple-term object (mirrors ProcessQuad.registerTripleTerm, per-occurrence
     * like every stat here): accounts the term - stats.numTripleTerms gates the
     * component store - and spills every interior term to the interior sorters
     * so dictionary derivation sees it. Interior predicates join the in-RAM
     * predicate population for their final rank id WITHOUT appending to the
     * positional predicate column (they occupy no row). Component kinds are
     * guarded loudly, matching the top-level position guards.
     */
    private void collectTripleTerm(Node tt) throws IOException {
        if (iEntSorter == null) {
            iEntSorter = track(provider.termSorter(workDir, "icol-ent"));
            iLitSorter = track(provider.termSorter(workDir, "icol-lit"));
        }
        stats.numTripleTerms++;
        try {
            TripleTerms.walk(tt, new TripleTerms.ComponentVisitor() {
                @Override
                public void component(TripleTerms.Position position, Node n) {
                    try {
                        switch (position) {
                            case SUBJECT -> {
                                countEntityKind(n, "triple-term subject");
                                iEntSorter.add(new TermRow(n, 0));
                            }
                            case PREDICATE -> {
                                if (!n.isURI()) {
                                    throw new IllegalStateException(
                                            "Unexpected triple-term predicate (not URI): " + n + " in " + tt);
                                }
                                stats.numIRI++;
                                predTempIds.computeIfAbsent(n, k -> {
                                    predByTempId.add(k);
                                    return (long) (predByTempId.size() - 1);
                                });
                            }
                            case OBJECT -> {
                                if (n.isLiteral()) {
                                    collectLiteralStats(n);
                                    iLitSorter.add(new TermRow(n, 0));
                                } else if (n.isNodeSeq()) {
                                    throw new IllegalStateException(
                                            "Unexpected triple-term object: a sequence inside a triple term is not stored: "
                                            + n + " in " + tt);
                                } else {
                                    countEntityKind(n, "triple-term object");
                                    iEntSorter.add(new TermRow(n, 0));
                                }
                            }
                        }
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }

                @Override
                public void nestedTripleTerm(Node nested) {
                    stats.numTripleTerms++;
                    try {
                        iLitSorter.add(new TermRow(nested, 0));
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }
            });
        } catch (UncheckedIOException ex) {
            throw ex.getCause();
        }
    }

    /**
     * Sequence object (mirrors ProcessQuad.registerSequence through the one
     * shared walk, {@link com.ebremer.beakgraph.core.lib.SeqTerms#walk}, and its
     * guards; per occurrence like every stat here): stats.numSequences gates
     * the sequence stores, nested sequences and literal, triple-term and
     * nested-sequence elements spill to the interior literal sorter, entity
     * elements to the interior entity sorter, so dictionary derivation sees
     * every one. The sequence itself is the row's object.
     *
     * <p>With {@code -arrays-only} a sequence that packs as numbers is not
     * walked so: its leaves and inner sequences go to the packed column, with
     * the quad's {@code row}, and take a dictionary entry only when another
     * column has them too (the rule of
     * {@link com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder#splitPacked}).
     */
    private void collectSequence(Node seq, long row) throws IOException {
        if (arrays != null && arrays.only()
                && com.ebremer.beakgraph.hdf5.writers.SequenceArrays.packsAsNumbers(seq, arrays.minLeaves())) {
            stats.numSequences++;   // its row; what is inside counts only where something else holds it
            collectPacked(seq, row);
            return;
        }
        if (iEntSorter == null) {
            iEntSorter = track(provider.termSorter(workDir, "icol-ent"));
            iLitSorter = track(provider.termSorter(workDir, "icol-lit"));
        }
        stats.numSequences++;
        try {
            com.ebremer.beakgraph.core.lib.SeqTerms.walk(seq, new com.ebremer.beakgraph.core.lib.SeqTerms.ElementVisitor() {
                @Override
                public void literal(Node n) {
                    collectLiteralStats(n);
                    spill(iLitSorter, n);
                }

                @Override
                public void entity(Node n) {
                    countEntityKind(n, "sequence element");
                    spill(iEntSorter, n);
                }

                @Override
                public void tripleTerm(Node n) {
                    try {
                        collectTripleTerm(n);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                    spill(iLitSorter, n);
                }

                @Override
                public boolean nestedSequence(Node n) {
                    stats.numSequences++;
                    spill(iLitSorter, n);
                    return true;
                }
            });
        } catch (UncheckedIOException ex) {
            throw ex.getCause();
        }
    }

    /** The parts of a quad's object that packs, at every depth, to the packed column with the quad's row. */
    private void collectPacked(Node seq, long row) throws IOException {
        if (packedSorter == null) {
            packedSorter = track(provider.termSorter(workDir, "pcol-packed"));
        }
        try {
            com.ebremer.beakgraph.core.lib.SeqTerms.walk(seq, new com.ebremer.beakgraph.core.lib.SeqTerms.ElementVisitor() {
                @Override
                public void literal(Node n) {
                    spill(packedSorter, n, row);
                }

                @Override
                public void entity(Node n) {
                    throw new IllegalStateException("A sequence that packs as numbers holds an entity: " + n);
                }

                @Override
                public void tripleTerm(Node n) {
                    throw new IllegalStateException("A sequence that packs as numbers holds a triple term: " + n);
                }

                @Override
                public boolean nestedSequence(Node n) {
                    spill(packedSorter, n, row);
                    return true;
                }
            });
        } catch (UncheckedIOException ex) {
            throw ex.getCause();
        }
    }

    private static void spill(RecordSorter<TermRow> sorter, Node n, long row) {
        try {
            sorter.add(new TermRow(n, row));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static void spill(RecordSorter<TermRow> sorter, Node n) {
        try {
            sorter.add(new TermRow(n, 0));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Literal stats accounting: the CDT guard and the datatype / language
     * bookkeeping here, the storage-class routing in the RAM builder's ONE
     * {@code countLiteralStats} (BG-297). RAM counts distinct literals;
     * per-occurrence counting yields identical min/max and identical
     * zero-ness of every count, which is all the dictionary widths and
     * buffer-allocation gates consume.
     */
    private void collectLiteralStats(Node o) {
        // Same guard as ProcessQuad: blank nodes or relative IRIs inside a
        // composite (cdt:) literal would silently stop co-referring. This pipeline
        // has no distinct-literal set, so the check runs per occurrence - it
        // parses composite values only, everything else is one instanceof.
        CdtTerms.requireStorable(o);
        String dt = o.getLiteralDatatypeURI();
        dataTypes.add(dt);
        String lang = o.getLiteralLanguage();
        if (lang != null && !lang.isEmpty()) {
            langSet.add(lang);
            if (o.getLiteralBaseDirection() != null) {
                langDirSeen = true;
            }
        }
        PositionalDictionaryWriterBuilder.countLiteralStats(o, stats);
    }

    private static boolean isGeoLiteral(Quad quad) {
        return SpatialAugmenter.isGeoLiteral(quad);
    }

    // ------------------------------------------------------------------
    // Quad transforms (mirrors of PositionalDictionaryWriterBuilder)
    // ------------------------------------------------------------------

    public static Quad relativize(Quad q) {
        return RelativeIris.relativizeQuad(q);   // the one implementation (BG-432)
    }

    /**
     * Numeric canonicalization, quad level. Delegates the per-node rule to the
     * RAM builder's single implementation (the former byte-identical copy here
     * was the mirror-topology hazard CHANGELOG.md "Format v5 design notes" (mirror topology) warns about), recursing into
     * triple-term objects exactly as the RAM builder does.
     */
    public static Quad canonicalizeNumericObject(Quad quad) {
        Node o = quad.getObject();
        Node canon = com.ebremer.beakgraph.core.lib.CompoundTerms.isCompound(o)
                ? com.ebremer.beakgraph.core.lib.CompoundTerms.map(o, PositionalDictionaryWriterBuilder::canonicalizeNumericNode)
                : PositionalDictionaryWriterBuilder.canonicalizeNumericNode(o);
        if (canon == o) return quad;
        return new Quad(quad.getGraph(), quad.getSubject(), quad.getPredicate(), canon);
    }

    // ------------------------------------------------------------------
    // Phase helpers
    // ------------------------------------------------------------------

    /**
     * Forwards {@code sorted} unchanged while adding each DISTINCT quad to
     * {@code alsoDistinct} (duplicates are consecutive in a sorted stream).
     * This is the dedup-once trick: GPOS receives exactly the unique quads the
     * GSPO scan keeps, so it never sorts a duplicate.
     */
    private static Iterator<IdQuad> teeDistinctTo(Iterator<IdQuad> sorted, RecordSorter<IdQuad> alsoDistinct) {
        return new Iterator<>() {
            private IdQuad prev = null;

            @Override
            public boolean hasNext() {
                return sorted.hasNext();
            }

            @Override
            public IdQuad next() {
                IdQuad q = sorted.next();
                if (prev == null || !q.equals(prev)) {
                    try {
                        alsoDistinct.add(q);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to tee distinct quad into GPOS sorter", e);
                    }
                    prev = q;
                }
                return q;
            }
        };
    }

    // ------------------------------------------------------------------
    // The sorted columns, merged a range of the term order at a time
    // ------------------------------------------------------------------

    /** The columns a range is merged from: the three of a quad's terms, and the interior terms of triple terms. */
    private static final int G = 0, S = 1, O = 2, INTERIOR_ENTITIES = 3, INTERIOR_LITERALS = 4, PACKED = 5, COLUMNS = 6;
    private static final String[] ROLES = {"graph", "subject", "object", "triple-term component", "triple-term object",
            "packed-sequence part"};

    /** Ranges cut for each worker thread: more than one, so that an uneven range does not hold a thread's share. */
    private static final int RANGES_PER_THREAD = 4;

    /** Threads the stage pool runs at once. */
    private int parallelism() {
        return (stagePool instanceof java.util.concurrent.ForkJoinPool pool)
                ? pool.getParallelism()
                : Runtime.getRuntime().availableProcessors();
    }

    /**
     * One sorted term column. A sorter that has ranges ({@link RangeSorter})
     * is read a range at a time, by as many threads as there are ranges open;
     * any other is read whole, as the one range there then is.
     */
    private static final class Column implements AutoCloseable {
        private final RecordSorter<TermRow> sorter;
        private RangeSorter.Ranges<TermRow> ranges;
        private RecordSorter.SortedCursor<TermRow> whole;
        /** Rows the ranges' merges have read of it. */
        long merged = 0;

        Column(RecordSorter<TermRow> sorter) {
            this.sorter = sorter;
        }

        /** Ends ingestion; a sorter of many runs merges them down here, which is the long part. */
        void sort() throws IOException {
            if (sorter instanceof RangeSorter<TermRow> ranged) {
                ranges = ranged.ranges();
            } else {
                whole = sorter.sorted();
            }
        }

        boolean ranged() {
            return ranges != null;
        }

        int maxOpen() {
            return (ranges != null) ? ranges.maxOpen() : Integer.MAX_VALUE;
        }

        RecordSorter.SortedCursor<TermRow> open(TermRow from, TermRow to) throws IOException {
            if (ranges != null) {
                return ranges.open(from, to);
            }
            if (from != null || to != null || whole == null) {
                throw new IllegalStateException("A sorter without ranges is read once, whole");
            }
            RecordSorter.SortedCursor<TermRow> cursor = whole;
            whole = null;
            return cursor;
        }

        @Override
        public void close() {
            if (whole != null) {
                whole.close();
                whole = null;
            }
            ranges = null;
            sorter.close();
        }
    }

    /**
     * The bounds of the ranges the columns are merged in: terms, ascending,
     * each the first of its range. They are chosen from the sorters' samples
     * so that the ranges weigh about the same in bytes of runs to read. No
     * bounds - one range - without a stage pool, for a small build, and where
     * a sorter has no ranges (the sequential engine's).
     */
    private List<TermRow> chooseBounds(Column[] columns) {
        if (stagePool == null) {
            return List.of();
        }
        long wanted = Math.min((long) parallelism() * RANGES_PER_THREAD, rows / Math.max(1, minRangeRows));
        if (wanted < 2) {
            return List.of();
        }
        List<RangeSorter.Sample<TermRow>> samples = new ArrayList<>();
        long total = 0;
        for (Column c : columns) {
            if (c == null) {
                continue;
            }
            if (!c.ranged()) {
                return List.of();
            }
            for (RangeSorter.Sample<TermRow> sample : c.ranges.samples()) {
                samples.add(sample);
                total += sample.weight();
            }
        }
        samples.sort((a, b) -> TermKey.compare(a.record().key(), b.record().key()));
        List<TermRow> bounds = new ArrayList<>();
        long sinceBound = 0;
        long remaining = total;
        long rangesLeft = wanted;
        int i = 0;
        while (i < samples.size() && rangesLeft > 1) {
            // The samples of one term together: a term is in one range.
            TermKey key = samples.get(i).record().key();
            long weight = 0;
            int j = i;
            while (j < samples.size() && TermKey.compare(samples.get(j).record().key(), key) == 0) {
                weight += samples.get(j).weight();
                j++;
            }
            if (i > 0 && sinceBound >= remaining / rangesLeft) {
                bounds.add(samples.get(i).record());
                remaining -= sinceBound;
                sinceBound = 0;
                rangesLeft--;
            }
            sinceBound += weight;
            i = j;
        }
        return bounds;
    }

    /**
     * The terms of a dictionary section in order: read by this thread
     * without a stage pool, else decoded - and their literal values worked
     * out - by the pool's threads, {@code ahead} chunks before this one takes
     * them.
     */
    private RecordSorter.SortedCursor<Node> sortedTerms(RecordSegments<Node> files, int ahead) {
        return (stagePool == null)
                ? files.read()
                : files.readAhead(stagePool, ahead,
                        com.ebremer.beakgraph.hdf5.writers.DictionaryNodeEncoder::prepare);
    }

    @FunctionalInterface
    private interface RangeTask {
        void run(int range) throws IOException;
    }

    /**
     * Runs {@code task} for the ranges {@code 0..ranges-1}: in order on this
     * thread without a stage pool (or for one range), else on the pool, at
     * most {@code atOnce} at a time - each of that many stages takes the next
     * range until none is left, so a failure ends them as it ends any stage
     * group ({@link #runStages}).
     */
    private void runRanges(String what, int ranges, int atOnce, RangeTask task) throws IOException {
        if (stagePool == null || ranges == 1) {
            for (int r = 0; r < ranges; r++) {
                task.run(r);
            }
            return;
        }
        java.util.concurrent.atomic.AtomicInteger next = new java.util.concurrent.atomic.AtomicInteger();
        Stage worker = () -> {
            for (int r = next.getAndIncrement(); r < ranges; r = next.getAndIncrement()) {
                checkCancelled(what);
                task.run(r);
            }
        };
        Stage[] workers = new Stage[Math.max(1, Math.min(atOnce, ranges))];
        Arrays.fill(workers, worker);
        runStages(what, workers);
    }

    /**
     * What the merge of one range leaves: its distinct terms, each column's
     * rows with their terms' codes, its distinct sequences, and which of them
     * are the object of a quad (by their place among the range's sequences):
     * the top-level ones {@code -arrays} classifies. With {@code -arrays-only}
     * also the rows of the quads whose objects, held only in the arrays, hold
     * a leaf or inner sequence that has an entry (null without a packed column).
     */
    private record RangeOutput(RecordFile<Node> entities, RecordFile<Node> literals, RangeIds[] ids,
                               long sequences, java.util.BitSet topSequences, RecordFile<Long> sharingRows) {}

    /** The head of one column's cursor in a range's merge. */
    private static final class Head {
        final int column;
        final RecordSorter.SortedCursor<TermRow> cursor;
        TermRow row;

        Head(int column, RecordSorter.SortedCursor<TermRow> cursor) {
            this.column = column;
            this.cursor = cursor;
        }

        boolean advance() {
            row = cursor.hasNext() ? cursor.next() : null;
            return row != null;
        }
    }

    /**
     * Merges one range of the sorted columns - the terms {@code t} with
     * {@code from <= t < to}, a null bound being an open end - in a single
     * pass that does what the build used to read the sorted columns three
     * times for:
     * <ul>
     * <li>the range's distinct entities (graphs, subjects, non-literal
     *     objects, the entities inside triple terms) and its distinct
     *     literals-section terms (literal and triple-term objects, the
     *     literals and nested terms inside triple terms), each in
     *     NodeComparator order - the dictionaries' parts for this range;
     *     NodeComparator's macro order (bnode &lt; IRI &lt; literal &lt;
     *     triple term) puts every entity before every literals-section term,
     *     so one merge of all the columns yields both, in turn;</li>
     * <li>for every row of the three quad columns, the code of its term:
     *     the term's 1-based place among the range's entities (shifted left
     *     once) or among its literals-section terms (shifted, plus one). The
     *     id is that place plus the count of the terms in the ranges before
     *     ({@link #assignIds}), so no term is compared with a dictionary
     *     afterwards.</li>
     * </ul>
     * Interior terms take their place in the dictionaries and give no row.
     *
     * <p>With {@code -arrays-only}, the packed column's terms (what the
     * sequences held only in the arrays hold) take no place: a term there
     * with an entry from another column names, by its rows, the quads whose
     * objects share it ({@code sharingRows}). And a quad's object that packs
     * but that a walked sequence also holds (an interior literal too) is no
     * longer top-level for the classes: it keeps its elements, and no class
     * takes it ({@link com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder#getHeldBack}).
     */
    private RangeOutput mergeRange(int range, TermRow from, TermRow to, Column[] columns) throws IOException {
        RecordFile<Node> entities = track(new RecordFile<>(workDir.resolve("entities.sorted." + range), NodeCodec.INSTANCE));
        RecordFile<Node> literals = track(new RecordFile<>(workDir.resolve("literals.sorted." + range), NodeCodec.INSTANCE));
        RangeIds[] ids = {
            track(new RangeIds(workDir.resolve("gcol.ids." + range))),
            track(new RangeIds(workDir.resolve("scol.ids." + range))),
            track(new RangeIds(workDir.resolve("ocol.ids." + range)))};
        List<Head> heads = new ArrayList<>(COLUMNS);
        long sequences = 0;
        java.util.BitSet topSequences = new java.util.BitSet();
        RecordFile<Long> sharingRows = (columns[PACKED] == null) ? null
                : track(new RecordFile<>(workDir.resolve("packed.sharing." + range), HugeRecords.VAR_LONG_CODEC));
        final boolean arraysOnly = arrays != null && arrays.only();
        try {
            // Equal terms in column order: the packed column's rows of a term
            // come after every other column's, so whether it has an entry is
            // known by then, and an object's before its interior rows.
            PriorityQueue<Head> heap = new PriorityQueue<>(COLUMNS, (a, b) -> {
                int c = TermKey.compare(a.row.key(), b.row.key());
                return (c != 0) ? c : Integer.compare(a.column, b.column);
            });
            for (int c = 0; c < COLUMNS; c++) {
                if (columns[c] != null) {
                    Head head = new Head(c, columns[c].open(from, to));
                    heads.add(head);
                    if (head.advance()) {
                        heap.add(head);
                    }
                }
            }
            TermKey last = null;
            long code = 0;
            boolean literalSpace = false;
            long n = 0;
            boolean sequence = false;
            boolean object = false;   // the current term is the object of a quad
            while (!heap.isEmpty()) {
                Head head = heap.poll();
                TermKey key = head.row.key();
                // equals() is the cheap equivalent of compare()==0 here: the
                // comparator tie-breaks on the exact RDF term, so it returns 0
                // only for term-equal nodes.
                boolean same = last != null && (last == key || last.node().equals(key.node()));
                if (head.column == PACKED) {
                    do {
                        if (same) {
                            sharingRows.append(head.row.row());
                        }
                        if ((++n & 0xFFFF) == 0) {
                            checkCancelled("merging the sorted columns");
                        }
                    } while (head.advance() && (head.row.key() == key || head.row.key().node().equals(key.node())));
                    if (head.row != null) {
                        heap.add(head);
                    }
                    continue;
                }
                if (!same) {
                    Node term = key.node();
                    sequence = term.isNodeSeq();
                    if (sequence) {
                        sequences++;
                    }
                    literalSpace = term.isLiteral() || term.isTripleTerm() || sequence;
                    if (literalSpace) {
                        literals.append(term);
                        code = (literals.count() << 1) | 1;
                    } else {
                        entities.append(term);
                        code = entities.count() << 1;
                    }
                    last = key;
                    object = false;
                }
                // A graph or a subject is an entity, and so is an interior
                // entity (the ingest refuses any other): one in the literals'
                // space, or an interior literal out of it, would be a row or
                // a dictionary entry in the wrong section.
                if (literalSpace != (head.column == INTERIOR_LITERALS) && head.column != O) {
                    throw new IllegalStateException("Unexpected " + ROLES[head.column] + " node type: " + key.node());
                }
                if (sequence && head.column == O) {
                    topSequences.set(Math.toIntExact(sequences - 1));   // an object of a quad, not only an element
                    object = true;
                }
                if (sequence && object && arraysOnly && head.column == INTERIOR_LITERALS
                        && com.ebremer.beakgraph.hdf5.writers.SequenceArrays.packsAsNumbers(key.node(), arrays.minLeaves())) {
                    topSequences.clear(Math.toIntExact(sequences - 1));   // held back: it keeps its elements
                }
                // Every row this column has of the term: they follow one
                // another, and the term is still the smallest there is.
                RangeIds out = (head.column <= O) ? ids[head.column] : null;
                do {
                    if (out != null) {
                        out.add(code, head.row.row());
                    }
                    if ((++n & 0xFFFF) == 0) {
                        checkCancelled("merging the sorted columns");
                    }
                } while (head.advance() && (head.row.key() == last || head.row.key().node().equals(last.node())));
                if (head.row != null) {
                    heap.add(head);
                }
            }
        } catch (UncheckedIOException ex) {
            throw ex.getCause();
        } finally {
            for (Head head : heads) {
                head.cursor.close();
            }
        }
        entities.finish();
        literals.finish();
        for (RangeIds column : ids) {
            column.finish();
        }
        if (sharingRows != null) {
            sharingRows.finish();
        }
        return new RangeOutput(entities, literals, ids, sequences, topSequences, sharingRows);
    }

    /**
     * {@code -arrays-only}: the ranges' sharing rows, sorted by row (a quad's
     * row once for each term it shares), or null when the build had no
     * packed column or nothing shared. The ranges' files go.
     */
    private RecordSorter<RowId> sharingRows(RangeOutput[] merged, long numObjects) throws IOException {
        RecordSorter<RowId> sorter = null;
        for (RangeOutput r : merged) {
            if (r.sharingRows() == null) {
                continue;
            }
            if (r.sharingRows().count() > 0) {
                if (sorter == null) {
                    sorter = track(provider.rowIdSorter(workDir, "packed-sharing", rows, numObjects));
                }
                try (var it = r.sharingRows().read()) {
                    while (it.hasNext()) {
                        sorter.add(new RowId(it.next(), 0));
                    }
                }
            }
            r.sharingRows().delete();
        }
        return sorter;
    }

    /**
     * The ids of one column's rows, range after range: a code's place plus
     * the count of its space's terms in the ranges before - an entity's
     * 1-based rank among the entities, a literals-section term's among those
     * terms after all the entities: exactly locateGraph/locateSubject/
     * locateObject semantics. Emits (row, id) for every row and the id of
     * each DISTINCT term - in term order, i.e. ascending id order - into the
     * columnar list.
     */
    private void assignIds(int column, RangeOutput[] merged, long[] entityBase, long[] literalBase,
                           SpillBitPackedBuffer columnar, RecordSorter<RowId> out) throws IOException {
        final String what = "assigning " + ROLES[column] + " ids";
        long last = 0;   // ids begin at 1
        for (int r = 0; r < merged.length; r++) {
            RangeIds ids = merged[r].ids[column];
            try (RangeIds.Reader groups = ids.read()) {
                while (groups.next()) {
                    long id = (((groups.code & 1) == 0) ? entityBase[r] : literalBase[r]) + (groups.code >>> 1);
                    if (id != last) {
                        if (id < last) {
                            throw new IllegalStateException("Ids of the " + ROLES[column] + " column descend: "
                                    + id + " after " + last + " (range " + r + ")");
                        }
                        columnar.writeLong(id);
                        last = id;
                    }
                    for (int i = 0; i < groups.size; i++) {
                        out.add(new RowId(groups.rows[i], id));
                    }
                    checkCancelled(what);
                }
            }
            ids.delete();
        }
    }

    /**
     * A monotone cursor over a sorted dictionary section: the id of a term is
     * its 1-based rank in the stream, computed without random access.
     */
    // Package-visible: HugeTripleTerms resolves the component references with it.
    static final class DictCursor implements AutoCloseable {
        private final Iterator<Node> stream;
        private final Runnable onClose;
        private TermKey current = null;
        private long id = 0;

        DictCursor(Iterator<Node> stream, Runnable onClose) {
            this.stream = stream;
            this.onClose = onClose;
        }

        /**
         * 1-based id of {@code key}'s term; the cursor only moves forward.
         * Where the dictionary stands against the term is asked of the
         * sort keys - the column's and one per dictionary entry - so a
         * literal's value is made once on each side, without Jena's cache.
         */
        long locate(TermKey key, String role) {
            Node term = key.node();
            // Cheap term-equality fast path before any comparator work.
            if (current != null && current.node().equals(term)) {
                return id;
            }
            while (current == null || TermKey.compare(current, key) < 0) {
                if (!stream.hasNext()) {
                    throw new IllegalStateException("Cannot resolve " + role + " (not in dictionary): " + term);
                }
                current = new TermKey(stream.next());
                id++;
                if (current.node().equals(term)) {
                    return id;
                }
            }
            if (!current.node().equals(term)) {
                throw new IllegalStateException("Cannot resolve " + role + " (not in dictionary): " + term);
            }
            return id;
        }

        @Override
        public void close() {
            onClose.run();
        }
    }

    @Override
    public void close() {
        if (pTempFile != null) pTempFile.delete();
        for (AutoCloseable r : resources) {
            try {
                r.close();
            } catch (Exception e) {
                logger.warn("Cleanup failure", e);
            }
        }
        resources.clear();
    }
}
