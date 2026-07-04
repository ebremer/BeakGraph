package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.Types;
import com.ebremer.beakgraph.huge.HugeRecords.IdQuad;
import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import com.ebremer.beakgraph.utils.RdfSources;
import com.ebremer.ns.GEO;
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
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.irix.IRIx;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The disk-based BeakGraph build: parse once, spill per-column (term, row)
 * records, external-sort them, derive the dictionaries by merge-dedup, assign
 * ids by sort-merge join (no random lookups), zip the id columns into encoded
 * quads, external-sort those twice for the GSPO/GPOS indexes, and stream every
 * buffer into the HDF5 file. RAM stays bounded by the sorter batch sizes plus
 * the small in-memory populations (predicates, datatypes, language tags, VoID
 * statistics) regardless of quad count.
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
final class HugeBuildPipeline implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HugeBuildPipeline.class);

    // Same sentinel base as the RAM builder: relative references resolve against
    // it during parsing and are stripped back to relative form for storage.
    private static final String REL_BASE = "http://beakgraph.invalid/document";
    private static final String REL_BASE_PREFIX = "http://beakgraph.invalid/";
    private static final IRIx REL_BASE_IRIX = IRIx.create(REL_BASE);

    /** Source documents; more than one means a -merge build into a single store. */
    private final List<File> sources;
    private final boolean spatial;
    private final boolean features;
    private final Path workDir;
    private final int mergeFanIn;

    // ---- Pass A state ----
    private final Stats stats = new Stats();
    private final TreeSet<String> dataTypes = new TreeSet<>();
    private final TreeSet<String> langSet = new TreeSet<>();
    // Predicates are the one population kept in RAM (real-world predicate counts
    // are tiny next to entities/literals); they get temp ids during the parse
    // and final rank ids once the set is complete.
    private final HashMap<Node, Long> predTempIds = new HashMap<>();
    private final ArrayList<Node> predByTempId = new ArrayList<>();
    private final BGVoIDSD xvoid = new BGVoIDSD("https://ebremer.com/void/");
    private ExternalSorter<TermRow> gSorter;
    private ExternalSorter<TermRow> sSorter;
    private ExternalSorter<TermRow> oSorter;
    private RecordFile<Long> pTempFile;
    private final int idSpillBatch;
    private long rows = 0;
    private long parsedQuads = 0;

    // ---- Build products (owned; released in close()) ----
    private final List<AutoCloseable> resources = new ArrayList<>();

    HugeBuildPipeline(List<File> sources, boolean spatial, boolean features, Path workDir,
                      int termSpillBatch, int idSpillBatch, int mergeFanIn) {
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("At least one source document is required");
        }
        this.sources = List.copyOf(sources);
        this.spatial = spatial;
        this.features = features;
        this.workDir = workDir;
        this.mergeFanIn = mergeFanIn;
        this.gSorter = track(new ExternalSorter<>(workDir, "gcol", HugeRecords.TERM_ROW_CODEC,
                HugeRecords.TERM_ORDER, termSpillBatch, mergeFanIn));
        this.sSorter = track(new ExternalSorter<>(workDir, "scol", HugeRecords.TERM_ROW_CODEC,
                HugeRecords.TERM_ORDER, termSpillBatch, mergeFanIn));
        this.oSorter = track(new ExternalSorter<>(workDir, "ocol", HugeRecords.TERM_ROW_CODEC,
                HugeRecords.TERM_ORDER, termSpillBatch, mergeFanIn));
        this.idSpillBatch = idSpillBatch;
    }

    private <T extends AutoCloseable> T track(T resource) {
        resources.add(resource);
        return resource;
    }

    /** Runs the whole build, producing the finished HDF5 file at {@code tmpH5}. */
    void run(Path tmpH5) throws IOException {
        ingest();

        // ---- Predicates: final rank ids from the in-RAM population ----
        Node[] sortedPreds = predByTempId.toArray(Node[]::new);
        Arrays.parallelSort(sortedPreds, NodeComparator.INSTANCE);
        long numPredicates = sortedPreds.length;
        long[] tempToFinal = new long[(int) Math.min(Integer.MAX_VALUE, predByTempId.size())];
        {
            HashMap<Node, Long> finalIds = new HashMap<>();
            for (int i = 0; i < sortedPreds.length; i++) {
                finalIds.put(sortedPreds[i], (long) i + 1); // 1-based
            }
            for (int t = 0; t < predByTempId.size(); t++) {
                tempToFinal[t] = finalIds.get(predByTempId.get(t));
            }
        }

        // ---- Materialize the sorted columns (each is re-read several times) ----
        logger.info("Sorting {} rows per column (external merge sort)...", rows);
        RecordFile<TermRow> gSorted = materialize(gSorter, "gcol.sorted");
        gSorter = null;
        RecordFile<TermRow> sSorted = materialize(sSorter, "scol.sorted");
        sSorter = null;
        RecordFile<TermRow> oSorted = materialize(oSorter, "ocol.sorted");
        oSorter = null;

        // ---- Distinct sorted dictionaries on disk ----
        RecordFile<Node> entFile = new RecordFile<>(workDir.resolve("entities.sorted"), NodeCodec.INSTANCE);
        mergeDistinctEntities(entFile, gSorted, sSorted, oSorted);
        long numEntities = entFile.count();

        RecordFile<Node> litFile = new RecordFile<>(workDir.resolve("literals.sorted"), NodeCodec.INSTANCE);
        distinctLiterals(litFile, oSorted);
        long numLiterals = litFile.count();
        long numObjects = numEntities + numLiterals;
        logger.info("Dictionary populations: {} entities, {} predicates, {} literals",
                numEntities, numPredicates, numLiterals);

        // ---- Encode the three dictionary sections ----
        StreamingDictionaryWriter entitiesDict = null;
        StreamingDictionaryWriter predicatesDict = null;
        StreamingDictionaryWriter literalsDict = null;
        if (numEntities > 0) {
            entitiesDict = track(new StreamingDictionaryWriter(workDir, "entities", numEntities, stats,
                    Set.of(Types.IRI, Types.BNODE), new TreeSet<>(), new TreeSet<>()));
            try (var s = entFile.read()) {
                entitiesDict.encode(s);
            }
        }
        if (numPredicates > 0) {
            predicatesDict = track(new StreamingDictionaryWriter(workDir, "predicates", numPredicates, stats,
                    Set.of(Types.IRI), new TreeSet<>(), new TreeSet<>()));
            predicatesDict.encode(Arrays.asList(sortedPreds).iterator());
        }
        if (numLiterals > 0) {
            literalsDict = track(new StreamingDictionaryWriter(workDir, "literals", numLiterals, stats,
                    Set.of(Types.DOUBLE, Types.FLOAT, Types.LONG, Types.INTEGER, Types.STRING),
                    dataTypes, langSet));
            try (var s = litFile.read()) {
                literalsDict.encode(s);
            }
        }

        // ---- Columnar unique-id lists (same widths as PositionalDictionaryWriter) ----
        int gBits = (int) (Math.ceil(MinBits(numEntities + 1) / 8.0) * 8);
        int sBits = (int) (Math.ceil(MinBits(numEntities + 1) / 8.0) * 8);
        int oBits = (int) (Math.ceil(MinBits(numObjects + 1) / 8.0) * 8);
        SpillBitPackedBuffer graphsList = track(new SpillBitPackedBuffer(workDir.resolve("columnar.graphs"), gBits));
        SpillBitPackedBuffer subjectsList = track(new SpillBitPackedBuffer(workDir.resolve("columnar.subjects"), sBits));
        SpillBitPackedBuffer objectsList = track(new SpillBitPackedBuffer(workDir.resolve("columnar.objects"), oBits));

        // ---- Sort-merge id joins ----
        logger.info("Assigning ids (sort-merge joins)...");
        ExternalSorter<RowId> gIds = track(new ExternalSorter<>(workDir, "gid", HugeRecords.ROW_ID_CODEC,
                HugeRecords.ROW_ORDER, idSpillBatch, mergeFanIn));
        joinColumn(gSorted, "Graph", entityCursor(entFile, null, numEntities), graphsList, gIds);
        gSorted.delete();

        ExternalSorter<RowId> sIds = track(new ExternalSorter<>(workDir, "sid", HugeRecords.ROW_ID_CODEC,
                HugeRecords.ROW_ORDER, idSpillBatch, mergeFanIn));
        joinColumn(sSorted, "Subject", entityCursor(entFile, null, numEntities), subjectsList, sIds);
        sSorted.delete();

        ExternalSorter<RowId> oIds = track(new ExternalSorter<>(workDir, "oid", HugeRecords.ROW_ID_CODEC,
                HugeRecords.ROW_ORDER, idSpillBatch, mergeFanIn));
        joinColumn(oSorted, "Object", entityCursor(entFile, litFile, numEntities), objectsList, oIds);
        oSorted.delete();
        entFile.delete();
        litFile.delete();

        // ---- Predicate column: temp ids -> final ids, already in row order ----
        RecordFile<Long> pFinal = new RecordFile<>(workDir.resolve("pcol.final"), HugeRecords.VAR_LONG_CODEC);
        try (var s = pTempFile.read()) {
            while (s.hasNext()) {
                pFinal.append(tempToFinal[Math.toIntExact(s.next())]);
            }
        }
        pFinal.finish();
        pTempFile.delete();
        if (pFinal.count() != rows) {
            throw new IllegalStateException("Predicate column has " + pFinal.count() + " entries for " + rows + " rows");
        }

        // ---- Zip id columns into encoded quads, feeding both index sorters ----
        logger.info("Encoding {} quads...", rows);
        ExternalSorter<IdQuad> gspoSorter = track(new ExternalSorter<>(workDir, "gspo", HugeRecords.ID_QUAD_CODEC,
                HugeRecords.GSPO_ORDER, idSpillBatch, mergeFanIn));
        ExternalSorter<IdQuad> gposSorter = track(new ExternalSorter<>(workDir, "gpos", HugeRecords.ID_QUAD_CODEC,
                HugeRecords.GPOS_ORDER, idSpillBatch, mergeFanIn));
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
                IdQuad q = new IdQuad(g.id(), s.id(), p, o.id());
                gspoSorter.add(q);
                gposSorter.add(q);
            }
        }
        pFinal.delete();

        // ---- Indexes ----
        HugeIndexWriter gspo = track(new HugeIndexWriter(workDir, Index.GSPO,
                numEntities, numEntities, numPredicates, numObjects, rows));
        try (var it = gspoSorter.sorted()) {
            gspo.build(it);
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
            hdt.putAttribute("numQuads", parsedQuads);
            hdt.putAttribute("formatVersion", Params.FORMAT_VERSION);
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
        this.pTempFile = new RecordFile<>(workDir.resolve("pcol.tmpids"), HugeRecords.VAR_LONG_CODEC);
        for (int i = 0; i < sources.size(); i++) {
            // Blank-node labels are document-scoped in RDF: two merged sources
            // may both say _:b0 and mean different nodes (labels are parsed as
            // given). Prefixing every label with the source ordinal keeps
            // documents from colliding WITHOUT the unbounded RAM map the RAM
            // writer's AlignBnodes uses. The prefix never reaches the output:
            // bnodes are stored by dictionary rank and readers regenerate
            // labels from ids. Single-source builds stay untouched.
            ingestSource(sources.get(i), sources.size() > 1 ? (i + "/") : null);
        }
        // VoID/SD metadata quads over ALL sources (mirrors the RAM writer; the
        // model's namespace prefixes are presentation-only and irrelevant to quads).
        for (Iterator<org.apache.jena.rdf.model.Statement> it = xvoid.getModel().listStatements(); it.hasNext(); ) {
            Triple ff = it.next().asTriple();
            Quad qqq = canonicalizeNumericObject(Quad.create(Params.BGVOID, ff));
            acceptRow(qqq);
        }
        pTempFile.finish();
        logger.info("Parse complete: {} source quads, {} total rows", parsedQuads, rows);
    }

    private void ingestSource(File input, String bnodeScope) throws IOException {
        logger.info("Parsing {} (disk-based build)", input);
        // Syntax from the file name (TriG, N-Quads, N-Triples, RDF/XML, JSON-LD,
        // Turtle); .gz and .zip are decompressed transparently - one shared rule
        // with the RAM writer and the CLI filter (RdfSources).
        try (RdfSources.OpenedSource opened = RdfSources.open(input)) {
            AsyncParserBuilder parserBuilder = AsyncParser.of(opened.stream(), opened.lang(), REL_BASE);
            parserBuilder.mutateSources(rdfBuilder ->
                    rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven()));
            SpatialAugmenter augmenter = new SpatialAugmenter(features);
            // Spatial tasks run concurrently exactly like the RAM writer, but the
            // completion window is BOUNDED: results are drained and spilled as
            // soon as the window fills instead of accumulating until end of parse.
            final int maxInFlight = Math.max(8, Runtime.getRuntime().availableProcessors() * 4);
            final ArrayDeque<Future<ArrayList<Quad>>> inFlight = new ArrayDeque<>();
            try (ExecutorService scope = Executors.newVirtualThreadPerTaskExecutor()) {
                parserBuilder.streamQuads()
                    .map(quad -> quad.isDefaultGraph()
                            ? new Quad(Quad.defaultGraphIRI, quad.getSubject(), quad.getPredicate(), quad.getObject())
                            : quad)
                    .map(quad -> scopeBlankNodes(quad, bnodeScope))
                    .map(this::relativize)
                    .map(this::canonicalizeNumericObject)
                    .forEach(quad -> {
                        parsedQuads++;
                        if (parsedQuads % 1_000_000 == 0) {
                            logger.info("Loaded {} quads...", parsedQuads);
                        }
                        try {
                            acceptRow(quad);
                            xvoid.add(quad);
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
    private static Quad scopeBlankNodes(Quad q, String scope) {
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
        } else {
            countEntityKind(o, "object");
        }
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
     * Literal stats accounting, mirroring ProcessQuad's branches. RAM counts
     * distinct literals; per-occurrence counting yields identical min/max and
     * identical zero-ness of every count, which is all the dictionary widths
     * and buffer-allocation gates consume.
     */
    private void collectLiteralStats(Node o) {
        String dt = o.getLiteralDatatypeURI();
        dataTypes.add(dt);
        String lang = o.getLiteralLanguage();
        if (lang != null && !lang.isEmpty()) {
            langSet.add(lang);
        }
        if (dt.equals(XSD.xlong.getURI())) {
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxLong = Math.max(stats.maxLong, n.longValue());
                stats.minLong = Math.min(stats.minLong, n.longValue());
                stats.numLong++;
            } else {
                countStringStored(o.getLiteralLexicalForm());
            }
        } else if (dt.equals(XSD.xint.getURI())) {
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxInteger = Math.max(stats.maxInteger, n.intValue());
                stats.minInteger = Math.min(stats.minInteger, n.intValue());
                stats.numInteger++;
            } else {
                countStringStored(o.getLiteralLexicalForm());
            }
        } else if (dt.equals(XSD.xfloat.getURI())) {
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxFloat = Math.max(stats.maxFloat, n.floatValue());
                stats.minFloat = Math.min(stats.minFloat, n.floatValue());
                stats.numFloat++;
            } else {
                countStringStored(o.getLiteralLexicalForm());
            }
        } else if (dt.equals(XSD.xdouble.getURI())) {
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxDouble = Math.max(stats.maxDouble, n.doubleValue());
                stats.minDouble = Math.min(stats.minDouble, n.doubleValue());
                stats.numDouble++;
            } else {
                countStringStored(o.getLiteralLexicalForm());
            }
        } else if (dt.equals(XSD.xstring.getURI()) || dt.equals(GEO.wktLiteral.getURI())
                || dt.equals(XSD.xboolean.getURI()) || dt.equals(RDF.langString.getURI())) {
            countStringStored(o.getLiteralLexicalForm());
        } else if (dt.equals(XSD.dateTime.getURI())) {
            String lex = o.getLiteralLexicalForm();
            int t = lex.indexOf('T');
            countStringStored((t > 0) ? lex.substring(0, t) : lex);
        } else {
            countStringStored(o.getLiteralLexicalForm());
        }
    }

    private void countStringStored(String lex) {
        stats.longestStringLength = Math.max(stats.longestStringLength, lex.length());
        stats.shortestStringLength = Math.min(stats.shortestStringLength, lex.length());
        stats.numStrings++;
    }

    private static Object literalValueOrNull(Node o) {
        try {
            return o.getLiteralValue();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static boolean isGeoLiteral(Quad quad) {
        return SpatialAugmenter.isGeoLiteral(quad);
    }

    // ------------------------------------------------------------------
    // Quad transforms (mirrors of PositionalDictionaryWriterBuilder)
    // ------------------------------------------------------------------

    private Quad relativize(Quad q) {
        Node qg = q.getGraph();
        Node qs = q.getSubject();
        Node qp = q.getPredicate();
        Node qo = q.getObject();
        Node g = relativizeNode(qg);
        Node s = relativizeNode(qs);
        Node p = relativizeNode(qp);
        Node o = relativizeNode(qo);
        if (g == qg && s == qs && p == qp && o == qo) {
            return q;
        }
        return new Quad(g, s, p, o);
    }

    private Node relativizeNode(Node n) {
        if (n == null || !n.isURI() || !n.getURI().startsWith(REL_BASE_PREFIX)) {
            return n;
        }
        String u = n.getURI();
        try {
            IRIx rel = REL_BASE_IRIX.relativize(IRIx.create(u));
            if (rel != null && rel.isRelative()) {
                return NodeFactory.createURI(rel.str());
            }
        } catch (RuntimeException ignore) {
            // fall through to textual stripping
        }
        return NodeFactory.createURI(
                u.equals(REL_BASE) ? "" : u.substring(REL_BASE_PREFIX.length()));
    }

    private Quad canonicalizeNumericObject(Quad quad) {
        Node o = quad.getObject();
        if (!o.isLiteral()) return quad;
        String dt = o.getLiteralDatatypeURI();
        try {
            Node canonical = null;
            if (XSD.xint.getURI().equals(dt)) {
                if (o.getLiteralValue() instanceof Number n) canonical = NodeFactory.createLiteralByValue(n.intValue());
            } else if (XSD.xlong.getURI().equals(dt)) {
                if (o.getLiteralValue() instanceof Number n) canonical = NodeFactory.createLiteralByValue(n.longValue());
            } else if (XSD.xfloat.getURI().equals(dt)) {
                if (o.getLiteralValue() instanceof Number n) canonical = NodeFactory.createLiteralByValue(n.floatValue());
            } else if (XSD.xdouble.getURI().equals(dt)) {
                if (o.getLiteralValue() instanceof Number n) canonical = NodeFactory.createLiteralByValue(n.doubleValue());
            }
            if (canonical == null || canonical.equals(o)) return quad;
            return new Quad(quad.getGraph(), quad.getSubject(), quad.getPredicate(), canonical);
        } catch (RuntimeException e) {
            return quad;
        }
    }

    // ------------------------------------------------------------------
    // Phase helpers
    // ------------------------------------------------------------------

    private RecordFile<TermRow> materialize(ExternalSorter<TermRow> sorter, String name) throws IOException {
        RecordFile<TermRow> rf = new RecordFile<>(workDir.resolve(name), HugeRecords.TERM_ROW_CODEC);
        try (ExternalSorter.SortedStream<TermRow> s = sorter.sorted()) {
            while (s.hasNext()) {
                rf.append(s.next());
            }
        }
        rf.finish();
        sorter.close();
        return rf;
    }

    /**
     * Entities = distinct {G union S union non-literal O} in NodeComparator
     * order: a 3-way merge of the sorted columns with consecutive-dedup. The
     * object stream stops at its first literal - NodeComparator's macro order
     * (bnode &lt; URI &lt; literal) makes literals a contiguous suffix.
     */
    private void mergeDistinctEntities(RecordFile<Node> out, RecordFile<TermRow> g,
                                       RecordFile<TermRow> s, RecordFile<TermRow> o) throws IOException {
        try (var gs = g.read(); var ss = s.read(); var os = o.read()) {
            List<Iterator<Node>> streams = List.of(
                    termsOf(gs), termsOf(ss), nonLiteralPrefix(termsOf(os)));
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

    /** Distinct literals = dedup of the literal suffix of the sorted object column. */
    private void distinctLiterals(RecordFile<Node> out, RecordFile<TermRow> o) throws IOException {
        try (var os = o.read()) {
            Node last = null;
            while (os.hasNext()) {
                Node term = os.next().term();
                if (!term.isLiteral()) continue; // pre-literal prefix
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
                    // Sorted stream: the first literal ends the entity prefix.
                    return n.isLiteral() ? null : n;
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

    private static final class DictCursor implements AutoCloseable {
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
                            SpillBitPackedBuffer columnar, ExternalSorter<RowId> out) throws IOException {
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
