package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.utils.ImageTools;
import com.ebremer.halcyon.hilbert.HilbertSpace;
import com.ebremer.halcyon.hilbert.PolygonScaler;
import com.ebremer.halcyon.hilbert.WKTDatatype;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.irix.IRIx;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.ebremer.beakgraph.Params.BGVOID;
import com.ebremer.beakgraph.features.MajorMinor;
import com.ebremer.beakgraph.features.pyradiomics.Gen2DFeatures;
import com.ebremer.beakgraph.sniff.SD;
import com.ebremer.ns.GEO;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VOID;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

public class PositionalDictionaryWriterBuilder {
    private static final Logger logger = LoggerFactory.getLogger(PositionalDictionaryWriterBuilder.class);
    private File src;
    private File dest;
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
    // stripped back to relative form for storage and resolved at query time
    // against the URL the .h5 file is served from.
    private static final String REL_BASE = "http://beakgraph.invalid/document";
    private static final String REL_BASE_PREFIX = "http://beakgraph.invalid/";
    private static final IRIx REL_BASE_IRIX = IRIx.create(REL_BASE);
    
    private static final Node[] asHilbert = {
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert0"), NodeFactory.createURI("https://halcyon.is/ns/asHilbert1"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert2"), NodeFactory.createURI("https://halcyon.is/ns/asHilbert3"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert4"), NodeFactory.createURI("https://halcyon.is/ns/asHilbert5"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert6"), NodeFactory.createURI("https://halcyon.is/ns/asHilbert7"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert8"), NodeFactory.createURI("https://halcyon.is/ns/asHilbert9"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert10"), NodeFactory.createURI("https://halcyon.is/ns/asHilbert11")
    };
    private static final Node[] low = {
        NodeFactory.createURI("https://halcyon.is/ns/low0"), NodeFactory.createURI("https://halcyon.is/ns/low1"),
        NodeFactory.createURI("https://halcyon.is/ns/low2"), NodeFactory.createURI("https://halcyon.is/ns/low3"),
        NodeFactory.createURI("https://halcyon.is/ns/low4"), NodeFactory.createURI("https://halcyon.is/ns/low5"),
        NodeFactory.createURI("https://halcyon.is/ns/low6"), NodeFactory.createURI("https://halcyon.is/ns/low7"),
        NodeFactory.createURI("https://halcyon.is/ns/low8"), NodeFactory.createURI("https://halcyon.is/ns/low9"),
        NodeFactory.createURI("https://halcyon.is/ns/low10"), NodeFactory.createURI("https://halcyon.is/ns/low11")
    };
    private static final Node[] high = {
        NodeFactory.createURI("https://halcyon.is/ns/high0"), NodeFactory.createURI("https://halcyon.is/ns/high1"),
        NodeFactory.createURI("https://halcyon.is/ns/high2"), NodeFactory.createURI("https://halcyon.is/ns/high3"),
        NodeFactory.createURI("https://halcyon.is/ns/high4"), NodeFactory.createURI("https://halcyon.is/ns/high5"),
        NodeFactory.createURI("https://halcyon.is/ns/high6"), NodeFactory.createURI("https://halcyon.is/ns/high7"),
        NodeFactory.createURI("https://halcyon.is/ns/high8"), NodeFactory.createURI("https://halcyon.is/ns/high9"),
        NodeFactory.createURI("https://halcyon.is/ns/high10"), NodeFactory.createURI("https://halcyon.is/ns/high11")
    };
    private static final Node[] hasRange = {
        NodeFactory.createURI("https://halcyon.is/ns/hasRange0"), NodeFactory.createURI("https://halcyon.is/ns/hasRange1"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange2"), NodeFactory.createURI("https://halcyon.is/ns/hasRange3"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange4"), NodeFactory.createURI("https://halcyon.is/ns/hasRange5"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange6"), NodeFactory.createURI("https://halcyon.is/ns/hasRange7"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange8"), NodeFactory.createURI("https://halcyon.is/ns/hasRange9"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange10"), NodeFactory.createURI("https://halcyon.is/ns/hasRange11")
    };
    private static final Node[] asWKT = {
        NodeFactory.createURI("https://halcyon.is/ns/asWKT0"), NodeFactory.createURI("https://halcyon.is/ns/asWKT1"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT2"), NodeFactory.createURI("https://halcyon.is/ns/asWKT3"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT4"), NodeFactory.createURI("https://halcyon.is/ns/asWKT5"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT6"), NodeFactory.createURI("https://halcyon.is/ns/asWKT7"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT8"), NodeFactory.createURI("https://halcyon.is/ns/asWKT9"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT10"), NodeFactory.createURI("https://halcyon.is/ns/asWKT11"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT12"), NodeFactory.createURI("https://halcyon.is/ns/asWKT13"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT14")
    };
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


    private BGVoIDSD xvoid = new BGVoIDSD("https://ebremer.com/void/");
    
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
    
    private List<Node> generateGridURNs(Polygon polygon, int resolutionLevel) {
        List<Node> intersectingURNs = new ArrayList<>();
        Envelope env = polygon.getEnvelopeInternal();   
        double cellSize = Params.GRIDTILESIZE;   
        long minTileX = (long) Math.floor(env.getMinX() / cellSize);
        long maxTileX = (long) Math.floor(env.getMaxX() / cellSize);
        long minTileY = (long) Math.floor(env.getMinY() / cellSize);
        long maxTileY = (long) Math.floor(env.getMaxY() / cellSize);   
        GeometryFactory gf = polygon.getFactory();
        for (long x = minTileX; x <= maxTileX; x++) {
            double tileMinX = x * cellSize;
            double tileMaxX = tileMinX + cellSize;
            for (long y = minTileY; y <= maxTileY; y++) {
                double tileMinY = y * cellSize;
                double tileMaxY = tileMinY + cellSize;            
                Envelope tileEnv = new Envelope(tileMinX, tileMaxX, tileMinY, tileMaxY);
                if (env.intersects(tileEnv)) {
                    Polygon tilePoly = (Polygon) gf.toGeometry(tileEnv);
                    if (polygon.intersects(tilePoly)) {
                        intersectingURNs.add(NodeFactory.createURI(
                            String.format("urn:x-beakgraph:grid:%d:%d:%d", resolutionLevel, x, y)));
                    }
                }
            }
        }
        return intersectingURNs;
    }
    
    private ArrayList<Quad> addSpatial(Quad quad) {
        final ArrayList<Quad> qqq = new ArrayList<>();
        // The GeoSPARQL-standard "<crs-uri> WKT" form must be indexed too: strip the
        // prefix once here so the parser and the scaler both see plain WKT
        // (previously such geometries failed the parse and were silently dropped).
        String wkt = ImageTools.stripCrs(quad.getObject().getLiteralLexicalForm());
        if (features) {
            // Same containment as the geometry block below: feature generation runs
            // inside the spatial task, so anything escaping here feeds Future.get()
            // and fails the whole write over one bad geometry.
            try {
                addFeatures(qqq, quad);
            } catch (Exception ex) {
                logger.warn("Failed to generate features for {}: {}", quad.getSubject(), ex.toString());
            }
        }
        // Everything geometry-related sits inside the catch-all below: a single bad
        // geometry must never abort the build (these tasks feed Future.get(), whose
        // ExecutionException would otherwise fail the whole write).
        try {
            // Index EVERY polygonal part: MULTIPOLYGON members each get their own
            // pyramid and corner entries (indexing only the first part made the
            // others unfindable). Non-areal geometries (POINT, LINESTRING) are
            // indexed via their envelope rather than silently skipped.
            List<Polygon> parts = ImageTools.wktToPolygons(wkt);
            if (parts.isEmpty()) {
                Geometry g = new WKTReader().read(wkt);
                if (g.isEmpty()) {
                    return qqq;
                }
                Envelope env = g.getEnvelopeInternal();
                env.expandBy(0.5); // a point/degenerate envelope still needs area
                parts = List.of((Polygon) new GeometryFactory().toGeometry(env));
            }
            for (Polygon part : parts) {
                addSpatialIndexCells(qqq, quad, part);
                addSpatialScales(qqq, quad, wkt, PolygonScaler.toPolygons(part));
            }
        } catch (Exception ex) {
            // Expected data condition (pathology exports contain degenerate
            // geometries such as two-point rings): one line per skip, no stack -
            // a slide can contain thousands of these.
            logger.warn("Skipping spatial indexing for {}: {} ({})",
                    quad.getSubject(), ex.toString(), abbrevWkt(wkt));
        }
        return qqq;
    }

    private void addSpatialScales(ArrayList<Quad> qqq, Quad quad, String wkt, Polygon[] scales) {
        if (scales == null) {
            return;
        }
        final String[] wktScales = PolygonScaler.toWKT(scales);
        for (int s=0; s<Math.min(scales.length, asWKT.length); s++) {
            List<Node> tiles = generateGridURNs(scales[s],s);
            try {
                for (int ii=0; ii<tiles.size(); ii++) {
                    qqq.add( Quad.create(tiles.get(ii), quad.getSubject(), asWKT[s], NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)));
                }
            } catch (Exception ex) {
                logger.error("Failed to add spatial tile quads for {}", abbrevWkt(wkt), ex);
            }
            try {
                qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), asWKT[s], NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)) );
            } catch (Exception ex) {
                logger.error("Failed to add scaled WKT quad for {}", abbrevWkt(wkt), ex);
            }
        }
    }

    /**
     * Emits this part's recall-safe spatial index entries: the Hilbert indices of
     * every whole cell covering its bbox, at the coarsest scale where that cover
     * is at most {@link #MAX_INDEX_CELLS} cells. Cell coordinates use floor
     * snapping - the query side MUST snap identically or shared cells are missed.
     */
    private void addSpatialIndexCells(ArrayList<Quad> qqq, Quad quad, Polygon part) {
        Envelope env = part.getEnvelopeInternal();
        long minX = (long) Math.floor(env.getMinX());
        long maxX = (long) Math.floor(env.getMaxX());
        long minY = (long) Math.floor(env.getMinY());
        long maxY = (long) Math.floor(env.getMaxY());
        if (maxX < 0 || maxY < 0) {
            logger.warn("Geometry outside the indexable (non-negative) coordinate domain; not indexed: {}",
                    quad.getSubject());
            return;
        }
        minX = Math.max(0, minX);
        minY = Math.max(0, minY);
        int s = 0;
        while (s < MAX_INDEX_SCALE && cellCount(minX, maxX, minY, maxY, s) > MAX_INDEX_CELLS) {
            s++;
        }
        long cell = 1L << s;
        Node pred = NodeFactory.createURI(HILBERT_CELL_NS + s);
        HashSet<Long> cells = new HashSet<>();
        for (long x = Math.floorDiv(minX, cell); x <= Math.floorDiv(maxX, cell); x++) {
            for (long y = Math.floorDiv(minY, cell); y <= Math.floorDiv(maxY, cell); y++) {
                cells.add(HilbertSpace.hc.index(new long[]{x, y}));
            }
        }
        for (Long c : cells) {
            qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), pred, NodeFactory.createLiteralByValue((long) c)));
        }
    }

    private static long cellCount(long minX, long maxX, long minY, long maxY, int s) {
        long cell = 1L << s;
        long nx = Math.floorDiv(maxX, cell) - Math.floorDiv(minX, cell) + 1;
        long ny = Math.floorDiv(maxY, cell) - Math.floorDiv(minY, cell) + 1;
        return nx * ny;
    }

    private static String abbrevWkt(String wkt) {
        return (wkt != null && wkt.length() > 200) ? wkt.substring(0, 200) + "..." : wkt;
    }
    
    private void addFeatures(ArrayList<Quad> qqq, Quad quad) {
        Node geo = quad.getSubject();
        String wkt = quad.getObject().getLiteralLexicalForm();
        Gen2DFeatures.generate(qqq, geo, wkt);
        MajorMinor.add(qqq, geo, wkt);
    }
    
    // Not synchronized: called only from the sequential streamQuads().forEach pipeline
    // (one consumer thread), like the other per-quad steps; the concurrent addSpatial
    // tasks never touch bmap.
    private Quad AlignBnodes(Quad quad) {
        Node g = quad.getGraph();
        Node s = quad.getSubject();
        Node o = quad.getObject();
        if (g.isBlank()||s.isBlank()||o.isBlank()) {
            if (g.isBlank()) {
                if (!bmap.containsKey(g)) {
                    Node neo = NodeFactory.createBlankNode(String.format("b%020d", bmap.size()));
                    bmap.put(g, neo);
                    g = neo;
                } else {
                    g = bmap.get(g);
                }
            }
            if (s.isBlank()) {
                if (!bmap.containsKey(s)) {
                    Node neo = NodeFactory.createBlankNode(String.format("b%020d", bmap.size()));
                    bmap.put(s, neo);
                    s = neo;
                } else {
                    s = bmap.get(s);
                }
            }
            if (o.isBlank()) {
                if (!bmap.containsKey(o)) {
                    Node neo = NodeFactory.createBlankNode(String.format("b%020d", bmap.size()));
                    bmap.put(o, neo);
                    o = neo;
                } else {
                    o = bmap.get(o);
                }
            }
        }
        return new Quad(g,s,quad.getPredicate(),o);
    }
    
    /**
     * Restores document-relative IRIs to their relative form for storage.
     * Relative references in the source were resolved against the sentinel base
     * during parsing; here that base is stripped so the empty reference
     * {@code <>} becomes "" and a sibling {@code <x.png>} becomes "x.png". They
     * are resolved against the serving URL at query time.
     */
    private Quad relativize(Quad q) {
        Node qg = q.getGraph();
        Node qs = q.getSubject();
        Node qp = q.getPredicate();
        Node qo = q.getObject();
        Node g = relativizeNode(qg);
        Node s = relativizeNode(qs);
        Node p = relativizeNode(qp);
        Node o = relativizeNode(qo);
        // relativizeNode returns the same Node when nothing changed; if no
        // position held a document-relative IRI, reuse the quad as-is.
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
        // IRIx relativization failed or was incomplete. Strip the sentinel
        // textually so it can never leak into stored data. This loses correct
        // <#fragment> / <../> handling - an acceptable trade for a rare case.
        return NodeFactory.createURI(
                u.equals(REL_BASE) ? "" : u.substring(REL_BASE_PREFIX.length()));
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
            // Malformed numeric literal: leave it untouched; downstream handling decides.
            return quad;
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

    private void countStringStored(String lex) {
        this.stats.longestStringLength = Math.max(this.stats.longestStringLength, lex.length());
        this.stats.shortestStringLength = Math.min(this.stats.shortestStringLength, lex.length());
        this.stats.numStrings++;
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
            }
            entities.add(s);
        }
        if (!predicates.contains(p)) {
            stats.numIRI++;
            predicates.add(p);
        }
        if (o.isLiteral()) {
            if (!literals.contains(o)) {
                String dt = o.getLiteralDatatypeURI();
                dataTypes.add(dt);
                if (dt.equals(XSD.xlong.getURI())) {
                    if (literalValueOrNull(o) instanceof Number n) {
                        this.stats.maxLong = Math.max(this.stats.maxLong, n.longValue());
                        this.stats.minLong = Math.min(this.stats.minLong, n.longValue());
                        this.stats.numLong++;
                    } else {
                        countStringStored(o.getLiteralLexicalForm()); // ill-typed: strings path
                    }
                } else if (dt.equals(XSD.xint.getURI())) {
                    // Only xsd:int (32-bit bounded) is bit-packed here. xsd:integer is
                    // unbounded, so it is handled by the string fallback below instead;
                    // bit-packing it would truncate large values and change the datatype
                    // to xsd:int on read-back.
                    if (literalValueOrNull(o) instanceof Number n) {
                        this.stats.maxInteger = Math.max(this.stats.maxInteger, n.intValue());
                        this.stats.minInteger = Math.min(this.stats.minInteger, n.intValue());
                        this.stats.numInteger++;
                    } else {
                        countStringStored(o.getLiteralLexicalForm()); // ill-typed: strings path
                    }
                } else if (dt.equals(XSD.xfloat.getURI())) {
                    if (literalValueOrNull(o) instanceof Number n) {
                        this.stats.maxFloat = Math.max(this.stats.maxFloat, n.floatValue());
                        this.stats.minFloat = Math.min(this.stats.minFloat, n.floatValue());
                        this.stats.numFloat++;
                    } else {
                        countStringStored(o.getLiteralLexicalForm()); // ill-typed: strings path
                    }
                } else if (dt.equals(XSD.xdouble.getURI())) {
                    if (literalValueOrNull(o) instanceof Number n) {
                        this.stats.maxDouble = Math.max(this.stats.maxDouble, n.doubleValue());
                        this.stats.minDouble = Math.min(this.stats.minDouble, n.doubleValue());
                        this.stats.numDouble++;
                    } else {
                        countStringStored(o.getLiteralLexicalForm()); // ill-typed: strings path
                    }
                } else if (dt.equals(XSD.xstring.getURI()) || dt.equals(GEO.wktLiteral.getURI()) || dt.equals(XSD.xboolean.getURI()) || dt.equals(RDF.langString.getURI())) {
                    // rdf:langString shares the strings buffer; its language tag is
                    // stored separately by MultiTypeDictionaryWriter (langs/langTags).
                    countStringStored(o.getLiteralLexicalForm());
                } else if (dt.equals(XSD.dateTime.getURI())) {
                    String lex = o.getLiteralLexicalForm();
                    int t = lex.indexOf('T');
                    countStringStored((t > 0) ? lex.substring(0, t) : lex);
                } else {
                    // Any other datatype (xsd:integer, xsd:decimal, xsd:date, custom
                    // datatypes, ...) is stored verbatim in the strings buffer by
                    // MultiTypeDictionaryWriter, tagged with its datatype IRI. Count it
                    // toward numStrings so that buffer is always allocated; otherwise the
                    // writer would have nowhere to put it and would drop the node,
                    // desynchronising the offset/datatype buffers and corrupting the dictionary.
                    countStringStored(o.getLiteralLexicalForm());
                }
                literals.add(o);
            }
        } else {
            if (!entities.contains(o)) {
                if (o.isBlank()) {
                    stats.numBlankNodes++;
                } else if (o.isURI()) {
                    stats.numIRI++;
                } else {
                    throw new IllegalStateException("Unexpected object node type (not URI, blank, or literal): " + o);
                }
                entities.add(o);
            }
        }
    }
    
    public PositionalDictionaryWriter build() throws IOException {
        final AtomicLong quadcount = new AtomicLong();
        logger.trace("Creating dictionary...");        
        try (InputStream xis = src.toString().endsWith(".gz")
                ? new GZIPInputStream(new FileInputStream(src))
                : new FileInputStream(src)) {
            // Detect the syntax from the file name (TriG, N-Quads, N-Triples,
            // Turtle, ...; .gz handled) instead of hardcoding Turtle: this is a
            // quad store, and named graphs can only arrive through a quad-capable
            // syntax.
            String fname = src.getName();
            if (fname.endsWith(".gz")) {
                fname = fname.substring(0, fname.length() - 3);
            }
            Lang lang = RDFLanguages.filenameToLang(fname, Lang.TURTLE);
            // Parse relative references against a stable sentinel base so they
            // resolve deterministically (not against the process working
            // directory). The relativize() step below strips the sentinel back
            // off; the relative form is resolved at query time against the URL
            // the .h5 file is served from.
            AsyncParserBuilder parserBuilder = AsyncParser.of(xis, lang, REL_BASE);
            parserBuilder.mutateSources(rdfBuilder ->
                    rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven()));
            final List<Future<ArrayList<Quad>>> spatialTasks = new ArrayList<>();
            // A per-task virtual-thread executor (final API since JDK 21). Its
            // try-with-resources close() blocks until every submitted spatial task finishes,
            // giving the same "join all forked work" guarantee as a structured task scope -
            // without depending on a preview API.
            try (ExecutorService scope = Executors.newVirtualThreadPerTaskExecutor()) {
                parserBuilder.streamQuads()
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
                        xvoid.add(quad);
                        if (spatial && isGeoLiteral(quad)) {
                            spatialTasks.add(scope.submit(() -> addSpatial(quad)));
                        }
                    });
            } catch (Exception ex) {
                // Don't swallow a parse/processing failure - that would leave a silently
                // truncated dictionary. Abort the write; Error/OOM still propagate.
                throw new IOException("Failed while parsing/processing RDF source: " + src, ex);
            }
            for (Future<ArrayList<Quad>> task : spatialTasks) {
                ArrayList<Quad> extraQuads;
                try {
                    extraQuads = task.get();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while collecting spatial results: " + src, ex);
                } catch (ExecutionException ex) {
                    throw new IOException("Spatial processing failed for " + src, ex.getCause());
                }
                extraQuads.forEach(q -> {
                    Quad canon = canonicalizeNumericObject(q);
                    quadslist.add(canon);
                    ProcessQuad(canon);
                });
            }
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
            xvoid.getModel().listStatements().forEach(s->{
                Triple ff = s.asTriple();
                Quad qqq = canonicalizeNumericObject(Quad.create(BGVOID, ff));
                ProcessQuad(qqq);
                quadslist.add(qqq);
            });
            // Set sum logic for backward compatibility in Stats object
            stats.numGraphs = entities.size(); 
            stats.numSubjects = entities.size();
            stats.numPredicates = predicates.size();
            stats.numObjects = entities.size() + literals.size();
            
        } catch (FileNotFoundException e) {
            throw new IOException("Source file not found: " + src, e);
        } catch (IOException e) {
            throw new IOException("I/O error while reading RDF source", e);
        }
        this.numQuads = quadcount.get();
        this.quads = quadslist.toArray(Quad[]::new);
        quadslist.clear();
        logger.info("Dictionary created. Total quads: {}", this.numQuads);
        return new PositionalDictionaryWriter(this);
    }

    private boolean isGeoLiteral(Quad quad) {
        Node o = quad.getObject();
        return o.isLiteral() && GEO.wktLiteral.getURI().equals(o.getLiteralDatatypeURI());
    }
}
