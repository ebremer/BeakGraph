package com.ebremer.beakgraph.hdf5.writers.ultra;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.CdtTerms;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.core.lib.TripleTerms;
import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder;
import com.ebremer.beakgraph.sniff.SD;
import com.ebremer.beakgraph.utils.RdfSources;
import static com.ebremer.beakgraph.Params.BGVOID;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VOID;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ultra writer's ingest stage: the same per-quad pipeline as the
 * sequential builder (default-graph rewrite, relativize, bnode alignment,
 * numeric canonicalization, spatial/feature augmentation, VoID statistics) but
 * parallelized on two axes:
 *
 * <ul>
 * <li><b>Across documents</b> - each source file parses in its own pool task
 *     (gzip inflation, tokenization, and per-quad normalization are the real
 *     wall-clock cost of small-file merges). Blank-node alignment is
 *     per-document state anyway - labels are document-scoped in RDF - so the
 *     tasks share nothing but the (thread-safe) VoID accumulator. A
 *     single-document ingest uses the sequential builder's exact replacement
 *     labels ({@code b%020d} from 0), keeping single-source output
 *     structurally identical to the other in-memory writers; a multi-document
 *     merge prefixes labels with the document ordinal instead (isomorphic
 *     output, labels differ).</li>
 * <li><b>Across quads</b> - dictionary-set deduplication runs as one parallel
 *     pass over the collected quad array into concurrent sets, and literal
 *     statistics are computed chunk-parallel over the DEDUPED literal set with
 *     the shared {@code countLiteralStats} rules, merged at the end. The
 *     sequential builder interleaves both with parsing on one thread.</li>
 * </ul>
 *
 * Extends {@link PositionalDictionaryWriterBuilder} purely to inherit the
 * protected per-quad helpers and stay pinned to their single implementation;
 * {@code parse()}/{@code build()} are never called. All getters the downstream
 * stages use are overridden to expose this class's own collected state.
 */
public final class UltraIngest extends PositionalDictionaryWriterBuilder {

    private static final Logger logger = LoggerFactory.getLogger(UltraIngest.class);

    // Own copies of configuration the base class keeps private
    private File usrc;
    private final List<File> usources = new ArrayList<>();
    private boolean uspatial = false;

    // Collected state (replaces the base class's single-threaded collections)
    private final Set<Node> entities = ConcurrentHashMap.newKeySet();
    private final Set<Node> predicates = ConcurrentHashMap.newKeySet();
    private final Set<Node> literals = ConcurrentHashMap.newKeySet();
    private final Set<Node> uniqueGraphs = ConcurrentHashMap.newKeySet();
    private final Set<Node> uniqueSubjects = ConcurrentHashMap.newKeySet();
    private final Set<Node> uniqueObjects = ConcurrentHashMap.newKeySet();
    private final Set<String> dataTypes = ConcurrentHashMap.newKeySet();
    private final Stats ustats = new Stats();
    private com.ebremer.beakgraph.core.VoidMode uVoidMode = com.ebremer.beakgraph.core.VoidMode.NONE;
    private BGVoIDSD xvoid; // null when uVoidMode == NONE
    private Quad[] quads;
    private long numQuads;

    private record DocResult(ArrayList<Quad> main, ArrayList<Quad> extra) {}

    // ------------------------------------------------------------------
    // configuration (capture what the base class hides)
    // ------------------------------------------------------------------

    @Override
    public UltraIngest setSource(File src) {
        this.usrc = src;
        super.setSource(src);
        return this;
    }

    @Override
    public UltraIngest setSources(List<File> files) {
        this.usources.clear();
        this.usources.addAll(files);
        super.setSources(files);
        return this;
    }

    @Override
    public UltraIngest setSpatial(boolean flag) {
        this.uspatial = flag;
        super.setSpatial(flag);
        return this;
    }

    @Override
    public UltraIngest setVoidMode(com.ebremer.beakgraph.core.VoidMode mode) {
        this.uVoidMode = mode;
        super.setVoidMode(mode);
        return this;
    }

    // ------------------------------------------------------------------
    // overridden state getters
    // ------------------------------------------------------------------

    @Override public Set<Node> getEntities() { return entities; }
    @Override public Set<Node> getPredicates() { return predicates; }
    @Override public Set<Node> getLiterals() { return literals; }
    @Override public Set<Node> getUniqueGraphs() { return uniqueGraphs; }
    @Override public Set<Node> getUniqueSubjects() { return uniqueSubjects; }
    @Override public Set<Node> getUniqueObjects() { return uniqueObjects; }
    @Override public Set<String> getDataTypes() { return dataTypes; }
    @Override public Stats getStats() { return ustats; }
    @Override public Quad[] getQuads() { return quads; }
    @Override public long getNumberOfQuads() { return numQuads; }

    /** Drops the quad array once the index stage has packed its keys. */
    void releaseQuads() {
        this.quads = null;
    }

    /**
     * Empties the node sets once the dictionary has sorted them and its
     * column fills are joined: they hold the same Node keys as the rank maps
     * and were live through the HDF5 emission (BG-115).
     */
    void releaseNodeSets() {
        entities.clear();
        predicates.clear();
        literals.clear();
        uniqueGraphs.clear();
        uniqueSubjects.clear();
        uniqueObjects.clear();
    }

    // ------------------------------------------------------------------
    // ingest
    // ------------------------------------------------------------------

    /**
     * Runs the full ingest on {@code pool}: documents in parallel, then the
     * dedup/stats passes in parallel over the collected quads. On return,
     * every getter above reflects the complete parsed state.
     */
    public void ingest(ForkJoinPool pool) throws IOException {
        if (usources.isEmpty() && usrc == null) {
            throw new IllegalStateException("No source set: call setSource() or setSources()");
        }
        final List<File> inputs = usources.isEmpty() ? List.of(usrc) : List.copyOf(usources);
        final boolean multi = inputs.size() > 1;
        this.xvoid = BGVoIDSD.forMode(uVoidMode, "https://ebremer.com/void/");
        final long ingestStart = System.nanoTime();
        logger.info("Ultra ingest: parsing {} source document(s) on {} threads (spatial={})",
                inputs.size(), pool.getParallelism(), uspatial);

        // ---- documents, in parallel ----
        List<ForkJoinTask<DocResult>> tasks = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            final File input = inputs.get(i);
            final int docIndex = i;
            tasks.add(pool.submit(() -> parseDocument(input, docIndex, multi)));
        }
        List<DocResult> docs = new ArrayList<>(inputs.size());
        IOException failure = null;
        for (int i = 0; i < tasks.size(); i++) {
            try {
                docs.add(tasks.get(i).get());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                failure = new IOException("Interrupted while parsing " + inputs.get(i), ex);
                break;
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                failure = (cause instanceof IOException io)
                        ? io
                        : new IOException("Failed to parse " + inputs.get(i), cause);
                break;
            }
        }
        if (failure != null) {
            // Let the remaining document tasks finish/fail quietly, then abort.
            tasks.forEach(t -> t.cancel(true));
            throw failure;
        }
        logger.info("All {} document(s) parsed in {} ms", inputs.size(),
                (System.nanoTime() - ingestStart) / 1_000_000L);

        // ---- VoID/SD metadata quads over ALL sources (only when requested) ----
        final ArrayList<Quad> voidQuads = new ArrayList<>();
        if (xvoid != null) {
            Model xxx = xvoid.getModel();
            xxx.setNsPrefix("void", VOID.NS);
            xxx.setNsPrefix("sd", SD.getURI());
            xxx.setNsPrefix("xsd", XSD.getURI());
            xxx.setNsPrefix("rdfs", RDFS.getURI());
            xxx.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
            xxx.setNsPrefix("prov", "http://www.w3.org/ns/prov#");
            xxx.setNsPrefix("dct", "http://purl.org/dc/terms/");
            xxx.setNsPrefix("hal", "https://halcyon.is/ns/");
            xxx.setNsPrefix("exif", "http://www.w3.org/2003/12/exif/ns#");
            xxx.listStatements().forEach(s ->
                    voidQuads.add(canonicalizeNumericObject(Quad.create(BGVOID, s.asTriple()))));
            logger.info("VoID/SD metadata generated: {} statements", voidQuads.size());
        }

        // ---- assemble the quad array (documents in input order: main quads,
        // then that document's spatial/feature quads - the sequential layout) ----
        long parsedTotal = 0;
        long total = voidQuads.size();
        for (DocResult d : docs) {
            parsedTotal += d.main.size();
            total += d.main.size() + d.extra.size();
        }
        if (total > Integer.MAX_VALUE - 16) {
            throw new IllegalArgumentException(total + " quads is too large for the in-memory ultra writer");
        }
        this.numQuads = parsedTotal;
        this.quads = new Quad[(int) total];
        int at = 0;
        for (DocResult d : docs) {
            for (Quad q : d.main) quads[at++] = q;
            for (Quad q : d.extra) quads[at++] = q;
        }
        for (Quad q : voidQuads) quads[at++] = q;
        docs.clear();

        // ---- one parallel dedup pass over everything ----
        logger.info("Deduplicating {} quads into dictionary sets (parallel)...", quads.length);
        long phase = System.nanoTime();
        buildSets(pool);
        logger.info("Dictionary sets built in {} ms: {} entities, {} predicates, {} literals, {} datatypes",
                (System.nanoTime() - phase) / 1_000_000L,
                entities.size(), predicates.size(), literals.size(), dataTypes.size());

        // ---- statistics from the deduped sets ----
        logger.info("Computing statistics over {} unique literals (parallel)...", literals.size());
        phase = System.nanoTime();
        buildStats(pool);
        logger.info("Statistics computed in {} ms", (System.nanoTime() - phase) / 1_000_000L);
        logger.info("Ultra ingest complete in {} ms: {} source quads, {} total rows",
                (System.nanoTime() - ingestStart) / 1_000_000L, numQuads, quads.length);
    }

    /**
     * Parses one document. Everything called here is either pure
     * ({@code relativize}, {@code canonicalizeNumericObject}), per-document
     * ({@code bmap}/counter), or thread-safe ({@code addSpatial},
     * {@code xvoid.add}) - documents never contend.
     */
    private DocResult parseDocument(File input, int docIndex, boolean multi) throws IOException {
        final ArrayList<Quad> main = new ArrayList<>();
        final ArrayList<Quad> extra = new ArrayList<>();
        final HashMap<Node, Node> bmap = new HashMap<>();
        final long[] counter = {0};
        // Single document: the sequential builder's exact labels. Merge: the
        // document ordinal keeps labels unique across documents with no
        // cross-document coordination.
        final String labelPrefix = multi ? ("b" + docIndex + "_") : "b";
        final long docStart = System.nanoTime();
        try (RdfSources.OpenedSource opened = RdfSources.open(input)) {
            AsyncParserBuilder parserBuilder = RdfSources.parser(opened, parseBase(input), input);
            final List<Future<ArrayList<Quad>>> spatialTasks = new ArrayList<>();
            // The quad stream closes before the executor: that aborts and joins
            // the parser thread when the loop throws (BG-100).
            try (ExecutorService scope = Executors.newVirtualThreadPerTaskExecutor();
                 Stream<Quad> quads = parserBuilder.streamQuads()) {
                quads
                    .map(quad -> quad.isDefaultGraph()
                            ? new Quad(Quad.defaultGraphIRI, quad.getSubject(), quad.getPredicate(), quad.getObject())
                            : quad)
                    .map(this::relativize)
                    .map(quad -> alignBnodes(quad, bmap, counter, labelPrefix))
                    .map(this::canonicalizeNumericObject)
                    .forEach(quad -> {
                        main.add(quad);
                        if (main.size() % 100_000 == 0) {
                            logger.info("{}: loaded {} quads...", input.getName(), main.size());
                        }
                        if (xvoid != null) {
                            xvoid.add(quad);
                        }
                        if (uspatial && isGeoLiteral(quad)) {
                            spatialTasks.add(scope.submit(() -> addSpatial(quad)));
                        }
                    });
            } catch (Exception ex) {
                throw new IOException("Failed while parsing/processing RDF source: " + input, ex);
            }
            for (Future<ArrayList<Quad>> task : spatialTasks) {
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
                    extra.add(canonicalizeNumericObject(q));
                }
            }
        } catch (FileNotFoundException e) {
            throw new IOException("Source file not found: " + input, e);
        } catch (IOException e) {
            throw new IOException("I/O error while reading RDF source: " + input, e);
        }
        if (extra.isEmpty()) {
            logger.info("Parsed {} quads from {} in {} ms", main.size(), input,
                    (System.nanoTime() - docStart) / 1_000_000L);
        } else {
            logger.info("Parsed {} quads (+{} spatial/feature quads) from {} in {} ms",
                    main.size(), extra.size(), input, (System.nanoTime() - docStart) / 1_000_000L);
        }
        return new DocResult(main, extra);
    }

    /** The base AlignBnodes with per-document map, counter, and label format. */
    private static Quad alignBnodes(Quad quad, HashMap<Node, Node> bmap, long[] counter, String labelPrefix) {
        Node g = quad.getGraph();
        Node s = quad.getSubject();
        Node o = quad.getObject();
        boolean oTT = o.isTripleTerm();
        if (!(g.isBlank() || s.isBlank() || o.isBlank() || oTT)) {
            return quad;
        }
        java.util.function.UnaryOperator<Node> align = n -> n.isBlank()
                ? bmap.computeIfAbsent(n, k -> NodeFactory.createBlankNode(Params.blankNodeLabel(labelPrefix, counter[0]++)))
                : n;
        Node g2 = align.apply(g);
        Node s2 = align.apply(s);
        // Blank nodes INSIDE a triple term share the quad's document scope, so
        // they go through the same bmap - the co-reference invariant.
        Node o2 = oTT ? TripleTerms.map(o, align) : align.apply(o);
        if (g2 == g && s2 == s && o2 == o) {
            return quad;
        }
        return new Quad(g2, s2, quad.getPredicate(), o2);
    }

    /** One parallel pass: node-kind validation plus insertion into every dictionary set. */
    private void buildSets(ForkJoinPool pool) throws IOException {
        final Quad[] all = this.quads;
        try {
            pool.submit(() -> java.util.Arrays.stream(all).parallel().forEach(quad -> {
                Node g = quad.getGraph();
                Node s = quad.getSubject();
                Node p = quad.getPredicate();
                Node o = quad.getObject();
                // Same guards as the sequential ProcessQuad: any other node kind
                // (e.g. a triple term) would silently desynchronize the dictionary.
                if (!g.isBlank() && !g.isURI()) {
                    throw new IllegalStateException("Unexpected graph node type (not URI or blank): " + g);
                }
                if (!s.isBlank() && !s.isURI()) {
                    throw new IllegalStateException("Unexpected subject node type (not URI or blank): " + s);
                }
                uniqueGraphs.add(g);
                uniqueSubjects.add(s);
                uniqueObjects.add(o);
                entities.add(g);
                entities.add(s);
                predicates.add(p);
                if (o.isLiteral()) {
                    registerLiteralSet(o);
                } else if (o.isTripleTerm()) {
                    registerTripleTermSet(o);
                } else {
                    if (!o.isBlank() && !o.isURI()) {
                        throw new IllegalStateException("Unexpected object node type (not URI, blank, literal, or triple term): " + o);
                    }
                    entities.add(o);
                }
            })).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while building dictionary sets", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException("Failed to build dictionary sets", cause);
        }
    }

    /**
     * Set insertion for one literal object - top-level or inside a triple term.
     * Parallel-safe (the sets are concurrent); add() gates the composite-parsing
     * blank-node check to once per distinct term, matching the sequential
     * builder's first-encounter semantics.
     */
    private void registerLiteralSet(Node o) {
        if (literals.add(o)) {
            CdtTerms.requireStorable(o);
        }
        dataTypes.add(o.getLiteralDatatypeURI());
    }

    /**
     * Parallel mirror of the sequential builder's registerTripleTerm (PLAN
     * Part IV §IV.7): the term and every component join the dictionary sets -
     * interiors get dictionary entries, never role-list membership - with the
     * same loud component-kind guards. Counting happens later over the deduped
     * set (buildStats), so concurrent insertion needs no counters here.
     */
    private void registerTripleTermSet(Node tt) {
        if (!literals.add(tt)) {
            return; // components were registered when the term first appeared
        }
        TripleTerms.walk(tt, new TripleTerms.ComponentVisitor() {
            @Override
            public void component(TripleTerms.Position position, Node n) {
                switch (position) {
                    case SUBJECT -> {
                        if (!(n.isBlank() || n.isURI())) {
                            throw new IllegalStateException(
                                    "Unexpected triple-term subject (not URI or blank): " + n + " in " + tt);
                        }
                        entities.add(n);
                    }
                    case PREDICATE -> {
                        if (!n.isURI()) {
                            throw new IllegalStateException(
                                    "Unexpected triple-term predicate (not URI): " + n + " in " + tt);
                        }
                        predicates.add(n);
                    }
                    case OBJECT -> {
                        if (n.isLiteral()) {
                            registerLiteralSet(n);
                        } else if (n.isBlank() || n.isURI()) {
                            entities.add(n);
                        } else {
                            throw new IllegalStateException(
                                    "Unexpected triple-term object node type: " + n + " in " + tt);
                        }
                    }
                }
            }

            @Override
            public void nestedTripleTerm(Node nested) {
                // The ongoing walk covers its components; just register the term.
                literals.add(nested);
            }
        });
    }

    /**
     * Statistics over the DEDUPED sets - semantically identical to the
     * sequential builder, which counts each entity/predicate/literal only on
     * first encounter. Literal stats run chunk-parallel with partial
     * {@link Stats} merged at the end.
     */
    private void buildStats(ForkJoinPool pool) throws IOException {
        final Node[] ents = entities.toArray(Node[]::new);
        final Node[] lits = literals.toArray(Node[]::new);
        final int chunks = Math.max(1, Math.min(pool.getParallelism() * 4, lits.length));
        final Stats[] partial = new Stats[chunks];
        final long[] blanks = new long[Math.max(1, chunks)];
        try {
            pool.submit(() -> {
                java.util.stream.IntStream.range(0, chunks).parallel().forEach(c -> {
                    Stats st = new Stats();
                    int from = (int) ((long) lits.length * c / chunks);
                    int to = (int) ((long) lits.length * (c + 1) / chunks);
                    for (int i = from; i < to; i++) {
                        if (lits[i].isTripleTerm()) {
                            // Triple terms share the literals section but have no
                            // literal value spaces to count - only their tally,
                            // which sizes the tripleTerms component store.
                            st.numTripleTerms++;
                        } else {
                            countLiteralStats(lits[i], st);
                        }
                    }
                    int efrom = (int) ((long) ents.length * c / chunks);
                    int eto = (int) ((long) ents.length * (c + 1) / chunks);
                    long b = 0;
                    for (int i = efrom; i < eto; i++) {
                        if (ents[i].isBlank()) b++;
                    }
                    partial[c] = st;
                    blanks[c] = b;
                });
            }).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while computing statistics", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException("Failed to compute statistics", cause);
        }
        for (Stats st : partial) {
            if (st != null) {
                mergeLiteralStats(ustats, st);
            }
        }
        long numBlank = 0;
        for (long b : blanks) {
            numBlank += b;
        }
        ustats.numBlankNodes = numBlank;
        // Sequential accounting: every unique non-blank entity and every unique
        // predicate counts as one IRI.
        ustats.numIRI = (entities.size() - numBlank) + predicates.size();
        ustats.numGraphs = entities.size();
        ustats.numSubjects = entities.size();
        ustats.numPredicates = predicates.size();
        ustats.numObjects = entities.size() + literals.size();
    }

    private static void mergeLiteralStats(Stats into, Stats part) {
        into.maxLong = Math.max(into.maxLong, part.maxLong);
        into.minLong = Math.min(into.minLong, part.minLong);
        into.numLong += part.numLong;
        into.maxInteger = Math.max(into.maxInteger, part.maxInteger);
        into.minInteger = Math.min(into.minInteger, part.minInteger);
        into.numInteger += part.numInteger;
        into.maxFloat = Math.max(into.maxFloat, part.maxFloat);
        into.minFloat = Math.min(into.minFloat, part.minFloat);
        into.numFloat += part.numFloat;
        into.maxDouble = Math.max(into.maxDouble, part.maxDouble);
        into.minDouble = Math.min(into.minDouble, part.minDouble);
        into.numDouble += part.numDouble;
        into.numTripleTerms += part.numTripleTerms;
        into.numStrings += part.numStrings;
        into.longestStringLength = Math.max(into.longestStringLength, part.longestStringLength);
        into.shortestStringLength = Math.min(into.shortestStringLength, part.shortestStringLength);
    }
}
