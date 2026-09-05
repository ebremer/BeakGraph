package com.ebremer.beakgraph.huge;

import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.CdtTerms;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.lib.NodeSorter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.core.lib.TripleTerms;
import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.DictionarySection;
import com.ebremer.beakgraph.huge.HugeRecords.IdQuad;
import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
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
 * records, external-sort them, derive the dictionaries by merge-dedup, assign
 * ids by sort-merge join (no random lookups), zip the id columns into encoded
 * quads, external-sort those twice for the GSPO/GPOS indexes, and stream every
 * buffer into the HDF5 file. RAM stays bounded by the sorter batch sizes (a
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
    private RecordFile<Long> pTempFile;
    private final SorterProvider provider;
    /** Non-null: independent stage groups run concurrently on it (-method 4). */
    private final java.util.concurrent.ExecutorService stagePool;
    /** Non-null: Pass A runs through it instead of the sequential loop (-method 5). */
    private final ParallelIngest parallelIngest;
    private File sourceRoot;

    /** Merge mode: root the documents' stored relative references are taken from (see {@link RelativeIris#parseBase}). */
    public HugeBuildPipeline setSourceRoot(File root) {
        this.sourceRoot = root;
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
     * (column materializations, dictionary encodes, id joins) concurrently.
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

        // ---- Materialize the sorted columns (each is re-read several times) ----
        logger.info("Sorting {} rows per column (external merge sort{})...", rows,
                stagePool == null ? "" : ", 3 columns concurrently");
        final List<RecordFile<TermRow>> cols = Arrays.asList(null, null, null);
        final RecordSorter<TermRow> gS = gSorter, sS = sSorter, oS = oSorter;
        gSorter = null;
        sSorter = null;
        oSorter = null;
        runStages("column sort",
                () -> cols.set(0, materialize(gS, "gcol.sorted")),
                () -> cols.set(1, materialize(sS, "scol.sorted")),
                () -> cols.set(2, materialize(oS, "ocol.sorted")));
        RecordFile<TermRow> gSorted = cols.get(0);
        RecordFile<TermRow> sSorted = cols.get(1);
        RecordFile<TermRow> oSorted = cols.get(2);

        // ---- Interior triple-term components (absent for triple-term-free builds) ----
        RecordFile<TermRow> iEntSorted = null;
        RecordFile<TermRow> iLitSorted = null;
        if (iEntSorter != null) {
            RecordSorter<TermRow> ie = iEntSorter;
            RecordSorter<TermRow> il = iLitSorter;
            iEntSorter = null;
            iLitSorter = null;
            iEntSorted = materialize(ie, "icol-ent.sorted");
            iLitSorted = materialize(il, "icol-lit.sorted");
        }

        // ---- Distinct sorted dictionaries on disk ----
        RecordFile<Node> entFile = track(new RecordFile<>(workDir.resolve("entities.sorted"), NodeCodec.INSTANCE));
        mergeDistinctEntities(entFile, gSorted, sSorted, oSorted, iEntSorted);
        long numEntities = entFile.count();

        RecordFile<Node> litFile = track(new RecordFile<>(workDir.resolve("literals.sorted"), NodeCodec.INSTANCE));
        distinctLiterals(litFile, oSorted, iLitSorted);
        long numLiterals = litFile.count();
        if (iEntSorted != null) {
            iEntSorted.delete();
            iLitSorted.delete();
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
        final StreamingDictionaryWriter[] dicts = new StreamingDictionaryWriter[3];
        runStages("dictionary encode",
                () -> {
                    if (numEntities > 0) {
                        dicts[0] = track(new StreamingDictionaryWriter(workDir, "entities", numEntities, stats,
                                DictionarySection.ENTITIES, new TreeSet<>(), new TreeSet<>(), false, null));
                        try (var s = entFile.read()) {
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
                                dataTypes, langSet, langDirSeen, ttSupport));
                        try (var s = litFile.read()) {
                            dicts[2].encode(s);
                        }
                        // Reference join (CHANGELOG.md "Format v5 design notes"): entFile is safe to
                        // read concurrently with the entities encode stage - the
                        // id joins already read it from three stages at once.
                        if (ttSupport != null && ttSupport.count() > 0) {
                            dicts[2].setTripleTermsBuffer(
                                    ttSupport.resolve(entFile, litFile, numEntities, numEntities + numLiterals));
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

        // ---- Sort-merge id joins (independent columns; concurrent with a pool),
        // plus the predicate temp->final remap, which depends on nothing here ----
        logger.info("Assigning ids (sort-merge joins{})...", stagePool == null ? "" : ", 3 columns concurrently");
        RecordSorter<RowId> gIds = track(provider.rowIdSorter(workDir, "gid", rows, numObjects));
        RecordSorter<RowId> sIds = track(provider.rowIdSorter(workDir, "sid", rows, numObjects));
        RecordSorter<RowId> oIds = track(provider.rowIdSorter(workDir, "oid", rows, numObjects));
        RecordFile<Long> pFinal = track(new RecordFile<>(workDir.resolve("pcol.final"), HugeRecords.VAR_LONG_CODEC));
        runStages("id join",
                () -> joinColumn(gSorted, "Graph", entityCursor(entFile, null, numEntities), graphsList, gIds),
                () -> joinColumn(sSorted, "Subject", entityCursor(entFile, null, numEntities), subjectsList, sIds),
                () -> joinColumn(oSorted, "Object", entityCursor(entFile, litFile, numEntities), objectsList, oIds),
                () -> {
                    try (var s = pTempFile.read()) {
                        while (s.hasNext()) {
                            pFinal.append(tempToFinal[Math.toIntExact(s.next())]);
                        }
                    }
                    pFinal.finish();
                });
        gSorted.delete();
        sSorted.delete();
        oSorted.delete();
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
        try (var gs = gIds.sorted(); var ss = sIds.sorted(); var os = oIds.sorted(); var ps = pFinal.read()) {
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
            }
        }
        pFinal.delete();

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

        // ---- Assemble the HDF5 file (same names/attributes as HDF5Writer) ----
        logger.info("Writing HDF5 file {}", tmpH5);
        try (StreamingHdf5File hdf = StreamingHdf5.create(tmpH5)) {
            StreamingHdf5Group hdt = hdf.rootGroup().putGroup(Params.BG);
            hdt.putAttribute(Params.NUM_QUADS, parsedQuads);
            hdt.putAttribute(Params.FORMAT_VERSION_ATTR, Params.FORMAT_VERSION);
            StreamingHdf5Group dictionary = hdt.putGroup(Params.DICTIONARY);
            if (entitiesDict != null) entitiesDict.transferTo(dictionary);
            if (predicatesDict != null) predicatesDict.transferTo(dictionary);
            if (literalsDict != null) literalsDict.transferTo(dictionary);
            if (graphsList.getNumEntries() > 0) {
                graphsList.transferTo(dictionary, "graphs");
                subjectsList.transferTo(dictionary, "subjects");
                objectsList.transferTo(dictionary, "objects");
            }
            gspo.transferTo(hdt);
            gpos.transferTo(hdt);
        }
        logger.info("HDF5 assembly complete");
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
            // (mirrors the RAM writer; the model's namespace prefixes are
            // presentation-only and irrelevant to quads)
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
        if (n.isTripleTerm()) {
            // Blank nodes inside a triple term share the quad's document scope -
            // the same prefix keeps inside/outside co-reference intact.
            return TripleTerms.map(n, c -> scopeNode(c, scope));
        }
        return n.isBlank() ? NodeFactory.createBlankNode(scope + n.getBlankNodeLabel()) : n;
    }

    private void drainOne(ArrayDeque<Future<ArrayList<Quad>>> inFlight, File input) throws IOException {
        Future<ArrayList<Quad>> task = inFlight.poll();
        if (task == null) return;
        ArrayList<Quad> extraQuads;
        try {
            extraQuads = task.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while collecting spatial results: " + input, ex);
        } catch (ExecutionException ex) {
            throw new IOException("Spatial processing failed for " + input, ex.getCause());
        }
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
        } else {
            countEntityKind(o, "object");
        }
        com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder.requirePredicate(p);
        stats.numIRI++; // the predicate (RAM counts distinct; only >0 gates buffer allocation)

        long row = rows++;
        gSorter.add(new TermRow(g, row));
        sSorter.add(new TermRow(s, row));
        oSorter.add(new TermRow(o, row));
        pTempFile.append(predTempIds.computeIfAbsent(p, k -> {
            predByTempId.add(k);
            return (long) (predByTempId.size() - 1);
        }));
    }

    private void countEntityKind(Node n, String position) {
        if (n.isBlank()) {
            stats.numBlankNodes++;
        } else if (n.isURI()) {
            stats.numIRI++;
        } else {
            // Same guard as ProcessQuad: an unstorable node kind aborts the build.
            throw new IllegalStateException("Unexpected " + position + " node type: " + n);
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
        Node canon = o.isTripleTerm()
                ? TripleTerms.map(o, PositionalDictionaryWriterBuilder::canonicalizeNumericNode)
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

    private RecordFile<TermRow> materialize(RecordSorter<TermRow> sorter, String name) throws IOException {
        // Tracked: a failure elsewhere must close and delete this file even
        // when the exception escapes mid-write (BG-124).
        RecordFile<TermRow> rf = track(new RecordFile<>(workDir.resolve(name), HugeRecords.TERM_ROW_CODEC));
        try (RecordSorter.SortedCursor<TermRow> s = sorter.sorted()) {
            long n = 0;
            while (s.hasNext()) {
                rf.append(s.next());
                if ((++n & 0xFFFF) == 0) {
                    checkCancelled("materializing " + name);
                }
            }
        }
        rf.finish();
        sorter.close();
        return rf;
    }

    /**
     * Entities = distinct {G union S union non-literal O union triple-term
     * interior entities} in NodeComparator order: an n-way merge of the sorted
     * streams with consecutive-dedup. The object stream stops at its first
     * literal OR triple term - NodeComparator's macro order (bnode &lt; URI &lt;
     * literal &lt; triple term) makes literals-plus-triple-terms a contiguous
     * suffix, and the prefix must end at whichever of the two comes first:
     * stopping only at a literal let a store with NO top-level literal object
     * stream its triple terms into the entities file, where the writer (built
     * without triple-term support) threw, so valid RDF 1.2 that the in-memory
     * engines built failed on the disk engines. {@code interior} is null for
     * triple-term-free builds.
     */
    private void mergeDistinctEntities(RecordFile<Node> out, RecordFile<TermRow> g,
                                       RecordFile<TermRow> s, RecordFile<TermRow> o,
                                       RecordFile<TermRow> interior) throws IOException {
        try (var gs = g.read(); var ss = s.read(); var os = o.read();
             var is = (interior == null) ? null : interior.read()) {
            List<Iterator<Node>> streams = (is == null)
                    ? List.of(termsOf(gs), termsOf(ss), nonLiteralPrefix(termsOf(os)))
                    : List.of(termsOf(gs), termsOf(ss), nonLiteralPrefix(termsOf(os)), termsOf(is));
            PriorityQueue<PeekedIterator> heap = new PriorityQueue<>(
                    Comparator.comparing(pi -> pi.head, NodeComparator.INSTANCE));
            for (Iterator<Node> it : streams) {
                PeekedIterator pi = new PeekedIterator(it);
                if (pi.head != null) heap.add(pi);
            }
            Node last = null;
            while (!heap.isEmpty()) {
                PeekedIterator pi = heap.poll();
                Node term = pi.head;
                if (pi.advance()) heap.add(pi);
                // equals() is the cheap equivalent of compare()==0 here: the
                // comparator tie-breaks on the exact RDF term, so it returns 0
                // only for term-equal nodes.
                if (last == null || !last.equals(term)) {
                    out.append(term);
                    last = term;
                }
            }
        }
        out.finish();
    }

    /**
     * Distinct literals-section terms = dedup-merge of the literal + triple-term
     * suffix of the sorted object column with the interior literal/triple-term
     * stream ({@code interior} null for triple-term-free builds). Triple terms
     * macro-rank after every literal, so the merged stream stays sorted and the
     * section keeps them as its contiguous suffix.
     */
    private void distinctLiterals(RecordFile<Node> out, RecordFile<TermRow> o,
                                  RecordFile<TermRow> interior) throws IOException {
        try (var os = o.read(); var is = (interior == null) ? null : interior.read()) {
            Iterator<Node> suffix = new Iterator<>() {
                private Node next = advance();

                private Node advance() {
                    while (os.hasNext()) {
                        Node n = os.next().term();
                        if (n.isLiteral() || n.isTripleTerm()) {
                            return n;
                        }
                    }
                    return null;
                }

                @Override public boolean hasNext() { return next != null; }

                @Override public Node next() {
                    if (next == null) throw new NoSuchElementException();
                    Node n = next;
                    next = advance();
                    return n;
                }
            };
            List<Iterator<Node>> streams = (is == null)
                    ? List.of(suffix)
                    : List.of(suffix, termsOf(is));
            PriorityQueue<PeekedIterator> heap = new PriorityQueue<>(
                    Comparator.comparing(pi -> pi.head, NodeComparator.INSTANCE));
            for (Iterator<Node> it : streams) {
                PeekedIterator pi = new PeekedIterator(it);
                if (pi.head != null) heap.add(pi);
            }
            Node last = null;
            while (!heap.isEmpty()) {
                PeekedIterator pi = heap.poll();
                Node term = pi.head;
                if (pi.advance()) heap.add(pi);
                if (last == null || !last.equals(term)) {
                    out.append(term);
                    last = term;
                }
            }
        }
        out.finish();
    }

    private static Iterator<Node> termsOf(Iterator<TermRow> rows) {
        return new Iterator<>() {
            @Override public boolean hasNext() { return rows.hasNext(); }
            @Override public Node next() { return rows.next().term(); }
        };
    }

    private static Iterator<Node> nonLiteralPrefix(Iterator<Node> in) {
        return new Iterator<>() {
            private Node next = advance();

            private Node advance() {
                if (in.hasNext()) {
                    Node n = in.next();
                    // Sorted stream: the first literal or triple term ends the
                    // entity prefix (mirrors distinctLiterals, which takes the rest).
                    return (n.isLiteral() || n.isTripleTerm()) ? null : n;
                }
                return null;
            }

            @Override public boolean hasNext() { return next != null; }

            @Override public Node next() {
                if (next == null) throw new NoSuchElementException();
                Node n = next;
                next = advance();
                return n;
            }
        };
    }

    private static final class PeekedIterator {
        private final Iterator<Node> it;
        Node head;

        PeekedIterator(Iterator<Node> it) {
            this.it = it;
            advance();
        }

        /** Loads the next head; false at stream end. */
        final boolean advance() {
            head = it.hasNext() ? it.next() : null;
            return head != null;
        }
    }

    /**
     * A monotone cursor over the sorted dictionary (entities, optionally
     * followed by literals): the id of a term is its 1-based rank in the
     * concatenated stream - exactly locateGraph/locateSubject/locateObject
     * semantics, computed without random access.
     */
    private DictCursor entityCursor(RecordFile<Node> entities, RecordFile<Node> literals, long numEntities)
            throws IOException {
        var entStream = entities.read();
        var litStream = (literals == null) ? null : literals.read();
        Iterator<Node> concat = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return entStream.hasNext() || (litStream != null && litStream.hasNext());
            }

            @Override
            public Node next() {
                return entStream.hasNext() ? entStream.next() : litStream.next();
            }
        };
        return new DictCursor(concat, () -> {
            entStream.close();
            if (litStream != null) litStream.close();
        });
    }

    // Package-visible: HugeTripleTerms reuses the same monotone-locate shape for
    // the component-reference join.
    static final class DictCursor implements AutoCloseable {
        private final Iterator<Node> stream;
        private final Runnable onClose;
        private Node current = null;
        private long id = 0;

        DictCursor(Iterator<Node> stream, Runnable onClose) {
            this.stream = stream;
            this.onClose = onClose;
        }

        /** 1-based id of {@code term}; the cursor only moves forward. */
        long locate(Node term, String role) {
            // Cheap term-equality fast path before any comparator work.
            if (current != null && current.equals(term)) {
                return id;
            }
            while (current == null || NodeComparator.INSTANCE.compare(current, term) < 0) {
                if (!stream.hasNext()) {
                    throw new IllegalStateException("Cannot resolve " + role + " (not in dictionary): " + term);
                }
                current = stream.next();
                id++;
                if (current.equals(term)) {
                    return id;
                }
            }
            if (!current.equals(term)) {
                throw new IllegalStateException("Cannot resolve " + role + " (not in dictionary): " + term);
            }
            return id;
        }

        @Override
        public void close() {
            onClose.run();
        }
    }

    /**
     * Walks a sorted column against the dictionary cursor, emitting (row, id)
     * for every row and the id of each DISTINCT term - in term order, i.e.
     * ascending id order - into the columnar list.
     */
    private void joinColumn(RecordFile<TermRow> column, String role, DictCursor cursor,
                            SpillBitPackedBuffer columnar, RecordSorter<RowId> out) throws IOException {
        try (cursor; var col = column.read()) {
            Node prevTerm = null;
            long prevId = -1;
            while (col.hasNext()) {
                TermRow tr = col.next();
                long id;
                // equals() suffices for the repeat check (compare()==0 iff term-equal).
                if (prevTerm != null && prevTerm.equals(tr.term())) {
                    id = prevId;
                } else {
                    id = cursor.locate(tr.term(), role);
                    columnar.writeLong(id);
                    prevTerm = tr.term();
                    prevId = id;
                }
                out.add(new RowId(tr.row(), id));
            }
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
