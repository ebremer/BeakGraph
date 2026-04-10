package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.Stats;
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
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
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
    
    // THE NEW MONOLITHIC ARCHITECTURE SETS
    private final HashSet<Node> entities = new HashSet<>();   // URIs & BNodes from G, S, O
    private final HashSet<Node> predicates = new HashSet<>(); // URIs from P
    private final HashSet<Node> literals = new HashSet<>();   // Literals from O

    // NEW SETS: Tracking unique occurrences to prevent massive loops later
    private final HashSet<Node> uniqueGraphs = new HashSet<>();
    private final HashSet<Node> uniqueSubjects = new HashSet<>();
    private final HashSet<Node> uniqueObjects = new HashSet<>();

    private final HashSet<String> dataTypes = new HashSet<>();
    private final Stats stats = new Stats();
    private long numQuads;
    private String name;
    private final ArrayList<Quad> quadslist = new ArrayList<>(100_000_000);
    private Quad[] quads = null;
    private final HashMap<Node,Node> bmap = new HashMap<>(10_000_000);
    private boolean spatial = false;
    private boolean features = false;
    private int MaxX = Integer.MIN_VALUE;
    private int MaxY = Integer.MIN_VALUE;
    
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
    private static final Node[] hilbertCorner = {
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner0"), NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner1"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner2"), NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner3"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner4"), NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner5"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner6"), NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner7"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner8"), NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner9"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner10"), NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner11"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner12"), NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner13"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner14")
    };
    
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
    
    private boolean isDegeneratePolygon(String wkt) {
        try {
            Geometry g = new WKTReader().read(wkt);
            if (!(g instanceof Polygon p)) return false;
            return p.getExteriorRing().getNumPoints() < 4;
        } catch (Exception e) {
            return true;
        }
    }

    private ArrayList<Quad> AddSpatial(Quad quad) {
        final ArrayList<Quad> qqq = new ArrayList<>();
        String wkt = quad.getObject().getLiteralLexicalForm();
        if (isDegeneratePolygon(wkt)) {
            IO.println("Degenerate Polygon : "+wkt);
            return qqq;
        }
        final Polygon[] scales;        
        scales = PolygonScaler.toPolygons(wkt);
        if (features) {
            AddFeatures(qqq, quad);
        }
        try {
            if (scales == null) {
                return qqq;
            }
            final String[] wktScales = PolygonScaler.toWKT(scales);
            for (int s=0; s<scales.length; s++) {
                List<Node> tiles = generateGridURNs(scales[s],s);
                try {
                    for (int ii=0; ii<tiles.size(); ii++) {
                        qqq.add( Quad.create(tiles.get(ii), quad.getSubject(), asWKT[s], NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)));
                    }            
                } catch (Throwable ex) {
                    logger.error(ex.getMessage());
                }
                long[] corners = HilbertSpace.getBoundingBoxHilbertIndices(scales[s]);
                try {                
                    qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[0])));
                    qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[1])));
                    qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[2])));
                    qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[3])));          
                    qqq.add( Quad.create(Params.SPATIAL, quad.getSubject(), asWKT[s], NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)) );
                } catch (IllegalArgumentException ex) {
                    logger.error("Bad polygon bro2 : {}", wkt);
                    return qqq;
                }  catch (Throwable ex) {
                logger.error(ex.getMessage());
            }
            }
        } catch (Throwable ex) {
            logger.error(ex.getMessage());
        }
        return qqq;
    }
    
    private void AddFeatures(ArrayList<Quad> qqq, Quad quad) {
        Node geo = quad.getSubject();
        String wkt = quad.getObject().getLiteralLexicalForm();
        Gen2DFeatures.Generate(qqq, geo, wkt);
        MajorMinor.Add(qqq, geo, wkt);
    }
    
    private synchronized Quad AlignBnodes(Quad quad) {
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
    
    private void ProcessQuad(Quad quad) {
        Node g = quad.getGraph();
        Node s = quad.getSubject();
        Node p = quad.getPredicate();
        Node o = quad.getObject();

        // Populate unique entities explicitly for the writer to use efficiently later
        uniqueGraphs.add(g);
        uniqueSubjects.add(s);
        uniqueObjects.add(o);

        // 1. Graph is an Entity
        if (!entities.contains(g)) {
            if (g.isBlank()) {
                stats.numBlankNodes++;
            } else if (g.isURI()) {
                stats.numIRI++;
            } else {
                throw new Error("This shouldn't be in here : "+g);
            }
            entities.add(g);
        }

        // 2. Subject is an Entity
        if (!entities.contains(s)) {
            if (s.isBlank()) {
                stats.numBlankNodes++;
            } else if (s.isURI()) {
                stats.numIRI++;
            }
            entities.add(s);
        }

        // 3. Predicate
        if (!predicates.contains(p)) {
            stats.numIRI++;
            predicates.add(p);
        }

        // 4. Object is a Literal OR an Entity
        if (o.isLiteral()) {
            if (!literals.contains(o)) {
                String dt = o.getLiteralDatatypeURI();
                dataTypes.add(dt);
                if (dt.equals(XSD.xlong.getURI())) {
                    Number n = (Number) o.getLiteralValue();
                    this.stats.maxLong = Math.max(this.stats.maxLong, n.longValue());
                    this.stats.minLong = Math.min(this.stats.minLong, n.longValue());
                    this.stats.numLong++;
                } else if (dt.equals(XSD.xint.getURI()) || dt.equals(XSD.integer.getURI())) {
                    Number n = (Number) o.getLiteralValue();
                    this.stats.maxInteger = Math.max(this.stats.maxInteger, n.intValue());
                    this.stats.minInteger = Math.min(this.stats.minInteger, n.intValue());
                    this.stats.numInteger++;
                } else if (dt.equals(XSD.xfloat.getURI())) {
                    Number n = (Number) o.getLiteralValue();
                    this.stats.maxFloat = Math.max(this.stats.maxFloat, n.floatValue());
                    this.stats.minFloat = Math.min(this.stats.minFloat, n.floatValue());
                    this.stats.numFloat++;
                } else if (dt.equals(XSD.xdouble.getURI())) {
                    Number n = (Number) o.getLiteralValue();
                    this.stats.maxDouble = Math.max(this.stats.maxDouble, n.doubleValue());
                    this.stats.minDouble = Math.min(this.stats.minDouble, n.doubleValue());
                    this.stats.numDouble++;
                } else if (dt.equals(XSD.xstring.getURI()) || dt.equals(GEO.wktLiteral.getURI()) || dt.equals(XSD.xboolean.getURI())) {
                    String wow = (String) o.getLiteralLexicalForm();                            
                    this.stats.longestStringLength = Math.max(this.stats.longestStringLength, wow.length());
                    this.stats.shortestStringLength = Math.min(this.stats.shortestStringLength, wow.length());
                    this.stats.numStrings++;
                } else if (dt.equals(XSD.dateTime.getURI())) {
                    String lex = o.getLiteralLexicalForm(); 
                    int t = lex.indexOf('T');
                    String wow = (t > 0) ? lex.substring(0, t) : lex;
                    this.stats.longestStringLength = Math.max(this.stats.longestStringLength, wow.length());
                    this.stats.shortestStringLength = Math.min(this.stats.shortestStringLength, wow.length());
                    this.stats.numStrings++;
                } else {
                    logger.error("I DON'T KNOW WHAT TO DO WITH : {}", o);
                }
                //if (!o.getLiteral().toString().contains("POLYGON")) IO.println(o);
                literals.add(o);
            }                  
        } else {
            // Object is a URI or BNode -> Add to Entities
            if (!entities.contains(o)) {
                if (o.isBlank()) {
                    stats.numBlankNodes++;
                } else if (o.isURI()) {
                    stats.numIRI++;
                } else {
                    throw new Error("WHAT THE HELL IS THIS : "+o);
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
            AsyncParserBuilder parserBuilder = AsyncParser.of(xis, Lang.TURTLE, null);
            parserBuilder.mutateSources(rdfBuilder ->
                    rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven()));
            final List<StructuredTaskScope.Subtask<ArrayList<Quad>>> spatialTasks = new ArrayList<>();
            try (var scope = StructuredTaskScope.open()) {
                parserBuilder.streamQuads()
                    .map(quad -> quad.isDefaultGraph()
                            ? new Quad(Quad.defaultGraphIRI, quad.getSubject(), quad.getPredicate(), quad.getObject())
                            : quad)
                    .map(this::AlignBnodes)
                    .forEach(quad -> {
                        quadcount.incrementAndGet();
                        if (quadcount.get() % 100_000 == 0) {
                            System.out.println("Loaded " + quadcount.get() + " quads...");
                        }
                        quadslist.add(quad);
                        try {
                           ProcessQuad(quad);
                        } catch (Throwable ex) {
                            logger.error(ex.getMessage());
                        }
                        xvoid.add(quad);                    
                        if (spatial && isGeoLiteral(quad)) {
                            StructuredTaskScope.Subtask<ArrayList<Quad>> task = scope.fork(() -> AddSpatial(quad));
                            spatialTasks.add(task);
                        }
                    });
                scope.join();
            } catch (InterruptedException ex) {
                System.getLogger(PositionalDictionaryWriter.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
            } catch (Throwable ex) {
                logger.error(ex.getMessage());
            }
            for (var task : spatialTasks) {
                ArrayList<Quad> extraQuads = task.get();
                extraQuads.forEach(q -> {
                    quadslist.add(q);
                    ProcessQuad(q);
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
                Quad qqq = Quad.create(BGVOID, ff);
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
        System.out.println("Dictionary created. Total Quads: " + this.numQuads);
        return new PositionalDictionaryWriter(this);
    }

    private boolean isGeoLiteral(Quad quad) {
        Node o = quad.getObject();
        return o.isLiteral() && GEO.wktLiteral.getURI().equals(o.getLiteralDatatypeURI());
    }
}
