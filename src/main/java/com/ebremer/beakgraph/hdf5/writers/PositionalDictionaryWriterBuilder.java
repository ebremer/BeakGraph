package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.CdtTerms;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.core.lib.TripleTerms;
import com.ebremer.beakgraph.utils.RdfSources;
import com.ebremer.beakgraph.huge.SpatialAugmenter;
import com.ebremer.halcyon.hilbert.WKTDatatype;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import com.ebremer.beakgraph.core.lib.RelativeIris;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.ebremer.beakgraph.Params.BGVOID;
import com.ebremer.beakgraph.sniff.SD;
import com.ebremer.ns.GEO;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VOID;

public class PositionalDictionaryWriterBuilder {
    static {
        // Deterministic datatype registration before any document is parsed.
        com.ebremer.halcyon.hilbert.WKTDatatype.register();
    }

    private static final Logger logger = LoggerFactory.getLogger(PositionalDictionaryWriterBuilder.class);
    private File src;
    private File dest;
    /** When non-empty, ALL of these documents are parsed into ONE store (-merge); src is ignored. */
    private final List<File> sources = new ArrayList<>();
    /** Replacement-label counter for AlignBnodes; never resets, so labels stay unique across merged sources. */
    private long bnodeCounter = 0;
    private final HashSet<Node> entities = new HashSet<>();   // URIs & BNodes from G, S, O
    private final HashSet<Node> predicates = new HashSet<>(); // URIs from P
    private final HashSet<Node> literals = new HashSet<>();   // Literals from O
    private final HashSet<Node> uniqueGraphs = new HashSet<>();
    private final HashSet<Node> uniqueSubjects = new HashSet<>();
    private final HashSet<Node> uniqueObjects = new HashSet<>();

    private final HashSet<String> dataTypes = new HashSet<>();
    private final Stats stats = new Stats();
    private long numQuads;
    private String name;
    private final ArrayList<Quad> quadslist = new ArrayList<>();
    private Quad[] quads = null;
    private final HashMap<Node,Node> bmap = new HashMap<>();
    private boolean spatial = false;
    private boolean features = false;
    private int MaxX = Integer.MIN_VALUE;
    private int MaxY = Integer.MIN_VALUE;
    // Sentinel base: relative references in the source are parsed against this
    // stable, reserved (.invalid) host that survives IRI normalization, then
    // stripped back to relative form for storage (RelativeIris) and resolved
    // at query time against the URL the .h5 file is served from. Protected:
    // the ultra subclass parses documents itself and must use the identical
    // base. One constant for every engine, so they cannot drift apart.
    protected static final String REL_BASE = RelativeIris.SENTINEL_BASE;
    
    /**
     * Spatial index entry parameters. Each geometry part's bbox is covered by
     * whole Hilbert cells at the coarsest scale where the cover holds at most
     * {@link #MAX_INDEX_CELLS} cells, stored as hal:hilbertCell{scale} values.
     * The query side covers ITS bbox with ranges at every scale using the same
     * floor snapping (see SpatialIndexIterator), so any bbox overlap shares a
     * cell at the stored scale: candidates have no false negatives, and the
     * sfIntersects filter - which the rewrite no longer removes from the plan -
     * eliminates the false positives with real JTS geometry.
     */
    public static final String HILBERT_CELL_NS = "https://halcyon.is/ns/hilbertCell";
    public static final int MAX_INDEX_CELLS = 16;
    public static final int MAX_INDEX_SCALE = 30;


    // Null when voidMode == NONE (the default): no statistics are collected
    // and no urn:x-beakgraph:void graph is written.
    private BGVoIDSD xvoid;
    private com.ebremer.beakgraph.core.VoidMode voidMode = com.ebremer.beakgraph.core.VoidMode.NONE;

    /** VoID statistics mode (NONE default, EXACT = -void, SKETCH = -voidsketch). */
    public PositionalDictionaryWriterBuilder setVoidMode(com.ebremer.beakgraph.core.VoidMode mode) {
        this.voidMode = mode;
        return this;
    }
    
    public File getDestination() { return dest; }
    public Quad[] getQuads() { return quads; }
   
    public Set<Node> getEntities() { return entities; }
    public Set<Node> getPredicates() { return predicates; }
    public Set<Node> getLiterals() { return literals; }

    // GETTERS FOR THE NEW UNIQUE SETS
    public Set<Node> getUniqueGraphs() { return uniqueGraphs; }
    public Set<Node> getUniqueSubjects() { return uniqueSubjects; }
    public Set<Node> getUniqueObjects() { return uniqueObjects; }
    
    public PositionalDictionaryWriterBuilder setSource(File src) {
        this.src = src; return this;
    }

    /**
     * Merge mode: parse every given document into the one store being built.
     * Blank nodes stay distinct per document; everything else (dictionaries,
     * VoID statistics, indexes) is computed over the union.
     */
    public PositionalDictionaryWriterBuilder setSources(List<File> files) {
        this.sources.clear();
        this.sources.addAll(files);
        return this;
    }

    private File sourceRoot;

    /** Merge mode: root the documents' stored relative references are taken from (see {@link RelativeIris#parseBase}). */
    public PositionalDictionaryWriterBuilder setSourceRoot(File root) {
        this.sourceRoot = root;
        return this;
    }

    /** The base {@code input} is parsed against: the sentinel, or its per-document merge base. */
    protected String parseBase(File input) {
        List<File> inputs = sources.isEmpty() ? List.of(src) : sources;
        return RelativeIris.parseBase(input, inputs, sourceRoot);
    }

    public PositionalDictionaryWriterBuilder setSpatial(boolean flag) {
        this.spatial = flag; return this;
    }
    
    public PositionalDictionaryWriterBuilder setFeatures(boolean flag) {
        this.features = flag; return this;
    }
    
    public PositionalDictionaryWriterBuilder setDestination(File dest) {
        this.dest = dest; return this;
    }
    
    public long getNumberOfQuads() { return numQuads; }
    public Stats getStats() { return stats; }
    public String getName() { return name; }
    public Set<String> getDataTypes() { return dataTypes; }
    
    public PositionalDictionaryWriterBuilder setName(String name) {
        this.name = name; return this;
    }
    
    public void maxExtent(Polygon poly) {
        Envelope env = poly.getEnvelopeInternal();
        MaxX = Math.max(MaxX, (int) env.getMaxX());
        MaxY = Math.max(MaxY, (int) env.getMaxY());
    }
    
    /**
     * Spatial index + derived-feature quads for one geometry-carrying quad.
     * ONE implementation for every engine: {@link SpatialAugmenter}, the
     * disk pipeline's port of the ~150-line block that lived here, is now the
     * only copy - the two had to stay byte-identical to each other and to
     * SpatialIndexIterator's floor snapping (BG-296). Protected, thread-safe:
     * the ultra subclass's per-document parallel parse calls it too.
     */
    protected ArrayList<Quad> addSpatial(Quad quad) {
        return new SpatialAugmenter(features).addSpatial(quad);
    }


    // Not synchronized: called only from the sequential streamQuads().forEach pipeline
    // (one consumer thread), like the other per-quad steps; the concurrent addSpatial
    // tasks never touch bmap.
    private Quad AlignBnodes(Quad quad) {
        Node g = alignNode(quad.getGraph());
        Node s = alignNode(quad.getSubject());
        Node o = quad.getObject();
        // Blank nodes INSIDE a triple term share the quad's document scope, so
        // they go through the same bmap - that shared map is exactly what keeps
        // a label co-referring inside and outside the term (the property whose
        // absence forced the CDT blank-node rejection policy).
        o = o.isTripleTerm() ? TripleTerms.map(o, this::alignNode) : alignNode(o);
        return new Quad(g, s, quad.getPredicate(), o);
    }

    /**
     * Replaces a blank node with its store-scoped {@code b%020d} alias
     * (first-encounter order, never-reset counter); every other node kind
     * passes through unchanged.
     */
    private Node alignNode(Node n) {
        if (!n.isBlank()) {
            return n;
        }
        Node neo = bmap.get(n);
        if (neo == null) {
            neo = NodeFactory.createBlankNode(Params.blankNodeLabel(bnodeCounter++));
            bmap.put(n, neo);
        }
        return neo;
    }
    
    /**
     * Restores document-relative IRIs to their relative form for storage.
     * Relative references in the source were resolved against the sentinel base
     * during parsing; here that base is stripped so the empty reference
     * {@code <>} becomes "" and a sibling {@code <x.png>} becomes "x.png". They
     * are resolved against the serving URL at query time.
     */
    protected final Quad relativize(Quad q) {
        return RelativeIris.relativizeQuad(q);   // the one implementation (BG-432)
    }

    /**
     * Numeric literal canonicalization policy: xsd:int / xsd:long / xsd:float /
     * xsd:double objects are stored by VALUE and the reader regenerates the
     * canonical lexical form, so two lexical variants of one value ("01" vs
     * "1"^^xsd:int) would become two term-distinct dictionary entries that both
     * extract to the same canonical term - duplicate "equal" entries that break
     * the strict ordering the dictionary binary search relies on, leaving some
     * triples unreachable by term lookup. Rewriting the object to its canonical
     * term at ingest collapses the variants onto one entry and keeps locate()
     * and extract() symmetric. (The value-typed storage never preserved the
     * non-canonical lexical form anyway.)
     */
    protected final Quad canonicalizeNumericObject(Quad quad) {
        Node o = quad.getObject();
        // Inside a triple term the same duplicate-"equal"-entries hazard applies
        // to the term's OBJECT component (subjects/predicates cannot be
        // literals), so the node-level rule recurses through map().
        Node canon = o.isTripleTerm()
                ? TripleTerms.map(o, PositionalDictionaryWriterBuilder::canonicalizeNumericNode)
                : canonicalizeNumericNode(o);
        if (canon == o) return quad;
        return new Quad(quad.getGraph(), quad.getSubject(), quad.getPredicate(), canon);
    }

    /**
     * Node-level canonicalization: the canonical value term for a well-formed
     * xsd:int / xsd:long / xsd:float / xsd:double literal, the node itself
     * (same instance) otherwise - including ill-formed numerics, which stay
     * term-exact on the strings path. Public: the disk pipeline's quad-level
     * mirror delegates here instead of keeping a byte-identical copy.
     */
    public static Node canonicalizeNumericNode(Node o) {
        if (!o.isLiteral()) return o;
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
            if (canonical == null || canonical.equals(o)) return o;
            return canonical;
        } catch (RuntimeException e) {
            // Malformed numeric literal: leave it untouched; downstream handling decides.
            return o;
        }
    }

    /**
     * getLiteralValue(), or null for an ill-typed literal ("abc"^^xsd:int). RDF
     * permits such terms and parsers accept them with a warning; they are counted
     * toward (and stored via) the lexical strings path instead of aborting the
     * whole build on a DatatypeFormatException.
     */
    private static Object literalValueOrNull(Node o) {
        try {
            return o.getLiteralValue();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static void countStringStored(Stats stats, String lex) {
        stats.longestStringLength = Math.max(stats.longestStringLength, lex.length());
        stats.shortestStringLength = Math.min(stats.shortestStringLength, lex.length());
        stats.numStrings++;
    }

    /**
     * Counts one DISTINCT literal into {@code stats}, choosing the same storage
     * class (long/int/float/double/strings, with the ill-typed and dateTime
     * special cases) that {@link MultiTypeDictionaryWriter} will pick when it
     * encodes the node. Extracted from {@link #ProcessQuad} so the ultra
     * writer's post-dedup, chunk-parallel stats pass counts literals with
     * EXACTLY the sequential rules (a drifted copy here would corrupt buffer
     * allocation, not just reporting). Called once per unique literal here;
     * the disk pipeline calls it once per OCCURRENCE, which is safe because
     * only the min/max values and the zero-ness of the counts are consumed
     * there (BG-297: it used to keep a byte-identical copy).
     */
    public static void countLiteralStats(Node o, Stats stats) {
        String dt = o.getLiteralDatatypeURI();
        if (dt.equals(XSD.xlong.getURI())) {
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxLong = Math.max(stats.maxLong, n.longValue());
                stats.minLong = Math.min(stats.minLong, n.longValue());
                stats.numLong++;
            } else {
                countStringStored(stats, o.getLiteralLexicalForm()); // ill-typed: strings path
            }
        } else if (dt.equals(XSD.xint.getURI())) {
            // Only xsd:int (32-bit bounded) is bit-packed here. xsd:integer is
            // unbounded, so it is handled by the string fallback below instead;
            // bit-packing it would truncate large values and change the datatype
            // to xsd:int on read-back.
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxInteger = Math.max(stats.maxInteger, n.intValue());
                stats.minInteger = Math.min(stats.minInteger, n.intValue());
                stats.numInteger++;
            } else {
                countStringStored(stats, o.getLiteralLexicalForm()); // ill-typed: strings path
            }
        } else if (dt.equals(XSD.xfloat.getURI())) {
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxFloat = Math.max(stats.maxFloat, n.floatValue());
                stats.minFloat = Math.min(stats.minFloat, n.floatValue());
                stats.numFloat++;
            } else {
                countStringStored(stats, o.getLiteralLexicalForm()); // ill-typed: strings path
            }
        } else if (dt.equals(XSD.xdouble.getURI())) {
            if (literalValueOrNull(o) instanceof Number n) {
                stats.maxDouble = Math.max(stats.maxDouble, n.doubleValue());
                stats.minDouble = Math.min(stats.minDouble, n.doubleValue());
                stats.numDouble++;
            } else {
                countStringStored(stats, o.getLiteralLexicalForm()); // ill-typed: strings path
            }
        } else if (dt.equals(XSD.xstring.getURI()) || dt.equals(GEO.wktLiteral.getURI()) || dt.equals(XSD.xboolean.getURI()) || dt.equals(RDF.langString.getURI())) {
            // rdf:langString shares the strings buffer; its language tag is
            // stored separately by MultiTypeDictionaryWriter (langs/langTags).
            countStringStored(stats, o.getLiteralLexicalForm());
        } else if (dt.equals(XSD.dateTime.getURI())) {
            String lex = o.getLiteralLexicalForm();
            int t = lex.indexOf('T');
            countStringStored(stats, (t > 0) ? lex.substring(0, t) : lex);
        } else {
            // Any other datatype (xsd:integer, xsd:decimal, xsd:date, custom
            // datatypes, ...) is stored verbatim in the strings buffer by
            // MultiTypeDictionaryWriter, tagged with its datatype IRI. Count it
            // toward numStrings so that buffer is always allocated; otherwise the
            // writer would have nowhere to put it and would drop the node,
            // desynchronising the offset/datatype buffers and corrupting the dictionary.
            countStringStored(stats, o.getLiteralLexicalForm());
        }
    }

    private void ProcessQuad(Quad quad) {
        Node g = quad.getGraph();
        Node s = quad.getSubject();
        Node p = quad.getPredicate();
        Node o = quad.getObject();       
        uniqueGraphs.add(g);
        uniqueSubjects.add(s);
        uniqueObjects.add(o);
        if (!entities.contains(g)) {
            if (g.isBlank()) {
                stats.numBlankNodes++;
            } else if (g.isURI()) {
                stats.numIRI++;
            } else {
                throw new IllegalStateException("Unexpected graph node type (not URI or blank): " + g);
            }
            entities.add(g);
        }
        if (!entities.contains(s)) {
            if (s.isBlank()) {
                stats.numBlankNodes++;
            } else if (s.isURI()) {
                stats.numIRI++;
            } else {
                // Same guard as the graph and object positions: any other node kind
                // (e.g. a triple term) writes no dictionary entry and silently
                // desynchronizes every id after it. Fail the build loudly instead.
                throw new IllegalStateException("Unexpected subject node type (not URI or blank): " + s);
            }
            entities.add(s);
        }
        if (!predicates.contains(p)) {
            stats.numIRI++;
            predicates.add(p);
        }
        if (o.isLiteral()) {
            registerLiteral(o);
        } else if (o.isTripleTerm()) {
            registerTripleTerm(o);
        } else {
            if (!entities.contains(o)) {
                if (o.isBlank()) {
                    stats.numBlankNodes++;
                } else if (o.isURI()) {
                    stats.numIRI++;
                } else {
                    throw new IllegalStateException("Unexpected object node type (not URI, blank, literal, or triple term): " + o);
                }
                entities.add(o);
            }
        }
    }

    /**
     * Records one literal OBJECT - top-level or inside a triple term - into the
     * literals dictionary set with its stats accounting. Idempotent per term.
     */
    private void registerLiteral(Node o) {
        if (literals.contains(o)) {
            return;
        }
        // Blank nodes or relative IRIs inside a composite (cdt:) literal would
        // silently stop co-referring with the graph. Reject at ingest (checked
        // once per distinct literal; parses composite values only).
        CdtTerms.requireStorable(o);
        dataTypes.add(o.getLiteralDatatypeURI());
        countLiteralStats(o, stats);
        literals.add(o);
    }

    /** Records an IRI or blank node encountered inside a triple term into the entity dictionary set. */
    private void registerEntity(Node n) {
        if (!entities.contains(n)) {
            if (n.isBlank()) {
                stats.numBlankNodes++;
            } else {
                stats.numIRI++;
            }
            entities.add(n);
        }
    }

    /**
     * Records a triple term and, recursively, every component into the
     * dictionary sets. Interiors get DICTIONARY entries, not role-list
     * membership (CHANGELOG.md "Format v5 design notes"): an IRI appearing only inside a triple
     * term is an entity, but it is not a subject/object for the
     * uniqueSubjects/uniqueObjects role lists. The triple term itself joins the
     * literals section (it is macro-ranked after every literal, so the section
     * stays sorted with triple terms as a contiguous suffix). Component kinds
     * are guarded loudly, matching the top-level position guards.
     */
    private void registerTripleTerm(Node tt) {
        if (literals.contains(tt)) {
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
                        registerEntity(n);
                    }
                    case PREDICATE -> {
                        if (!n.isURI()) {
                            throw new IllegalStateException(
                                    "Unexpected triple-term predicate (not URI): " + n + " in " + tt);
                        }
                        if (!predicates.contains(n)) {
                            stats.numIRI++;
                            predicates.add(n);
                        }
                    }
                    case OBJECT -> {
                        if (n.isLiteral()) {
                            registerLiteral(n);
                        } else if (n.isBlank() || n.isURI()) {
                            registerEntity(n);
                        } else {
                            throw new IllegalStateException(
                                    "Unexpected triple-term object node type: " + n + " in " + tt);
                        }
                    }
                }
            }

            @Override
            public void nestedTripleTerm(Node nested) {
                if (!literals.contains(nested)) {
                    stats.numTripleTerms++;
                    literals.add(nested);
                }
            }
        });
        stats.numTripleTerms++;
        literals.add(tt);
    }
    
    public PositionalDictionaryWriter build() throws IOException {
        parse();
        return new PositionalDictionaryWriter(this);
    }

    /**
     * Runs the full ingest pipeline - parse, bnode alignment, numeric
     * canonicalization, spatial/feature augmentation, VoID statistics - leaving
     * the collected quads, node sets, and stats in this builder. Shared verbatim
     * by {@link #build()} and the parallel subclass
     * (com.ebremer.beakgraph.hdf5.writers.parallel), which differ only in which
     * dictionary writer they construct from the collected state.
     */
    protected final void parse() throws IOException {
        final AtomicLong quadcount = new AtomicLong();
        logger.trace("Creating dictionary...");
        if (sources.isEmpty() && src == null) {
            throw new IllegalStateException("No source set: call setSource() or setSources()");
        }
        final List<File> inputs = sources.isEmpty() ? List.of(src) : List.copyOf(sources);
        this.xvoid = BGVoIDSD.forMode(voidMode, "https://ebremer.com/void/");
        for (File input : inputs) {
            parseSource(input, quadcount);
            // Blank-node labels are document-scoped in RDF: two sources may both
            // say _:b0 and mean different nodes (the parser keeps labels as
            // given). Clearing the alignment map per document keeps them
            // distinct, while the never-reset bnodeCounter keeps the replacement
            // labels unique across the whole (possibly merged) store.
            bmap.clear();
        }
        // VoID/SD metadata accumulated over ALL sources (only when requested)
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
            xxx.listStatements().forEach(s -> {
                Triple ff = s.asTriple();
                Quad qqq = canonicalizeNumericObject(Quad.create(BGVOID, ff));
                ProcessQuad(qqq);
                quadslist.add(qqq);
            });
        }
        // Set sum logic for backward compatibility in Stats object
        stats.numGraphs = entities.size();
        stats.numSubjects = entities.size();
        stats.numPredicates = predicates.size();
        stats.numObjects = entities.size() + literals.size();

        this.numQuads = quadcount.get();
        this.quads = quadslist.toArray(Quad[]::new);
        quadslist.clear();
        logger.info("Dictionary created. Total quads: {}", this.numQuads);
    }

    /** Parses one source document into the shared collected state. */
    private void parseSource(File input, AtomicLong quadcount) throws IOException {
        // The syntax comes from the file name (TriG, N-Quads, N-Triples,
        // RDF/XML, JSON-LD, Turtle; .gz and .zip handled) instead of
        // hardcoding Turtle: this is a quad store, and named graphs can only
        // arrive through a quad-capable syntax.
        try (RdfSources.OpenedSource opened = RdfSources.open(input)) {
            // Parse relative references against a stable sentinel base so they
            // resolve deterministically (not against the process working
            // directory). The relativize() step below strips the sentinel back
            // off; the relative form is resolved at query time against the URL
            // the .h5 file is served from.
            AsyncParserBuilder parserBuilder = RdfSources.parser(opened, parseBase(input), input);
            final List<Future<ArrayList<Quad>>> spatialTasks = new ArrayList<>();
            // A per-task virtual-thread executor (final API since JDK 21). Its
            // try-with-resources close() blocks until every submitted spatial task finishes,
            // giving the same "join all forked work" guarantee as a structured task scope -
            // without depending on a preview API. The quad stream is a resource
            // too, declared after the executor so it closes FIRST: closing it
            // aborts and joins Jena's parser thread when the loop below throws
            // (a guard, an invalid term), which otherwise stayed parked on its
            // full queue for the life of the process (BG-100).
            try (ExecutorService scope = Executors.newVirtualThreadPerTaskExecutor();
                 Stream<Quad> quads = parserBuilder.streamQuads()) {
                quads
                    .map(quad -> quad.isDefaultGraph()
                            ? new Quad(Quad.defaultGraphIRI, quad.getSubject(), quad.getPredicate(), quad.getObject())
                            : quad)
                    .map(this::relativize)
                    .map(this::AlignBnodes)
                    .map(this::canonicalizeNumericObject)
                    .forEach(quad -> {
                        quadcount.incrementAndGet();
                        if (quadcount.get() % 100_000 == 0) {
                            logger.info("Loaded {} quads...", quadcount.get());
                        }
                        quadslist.add(quad);
                        // Let an invalid quad abort the write rather than silently skipping
                        // its dictionary accounting (the quad is already in quadslist, so a
                        // skip would only fail later, opaquely, when the index can't locate it).
                        ProcessQuad(quad);
                        if (xvoid != null) {
                            xvoid.add(quad);
                        }
                        if (spatial && isGeoLiteral(quad)) {
                            spatialTasks.add(scope.submit(() -> addSpatial(quad)));
                        }
                    });
            } catch (Exception ex) {
                // Don't swallow a parse/processing failure - that would leave a silently
                // truncated dictionary. Abort the write; Error/OOM still propagate.
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
                extraQuads.forEach(q -> {
                    Quad canon = canonicalizeNumericObject(q);
                    quadslist.add(canon);
                    ProcessQuad(canon);
                });
            }
        } catch (FileNotFoundException e) {
            throw new IOException("Source file not found: " + input, e);
        } catch (IOException e) {
            throw new IOException("I/O error while reading RDF source: " + input, e);
        }
    }

    protected final boolean isGeoLiteral(Quad quad) {
        Node o = quad.getObject();
        return o.isLiteral() && GEO.wktLiteral.getURI().equals(o.getLiteralDatatypeURI());
    }
}
