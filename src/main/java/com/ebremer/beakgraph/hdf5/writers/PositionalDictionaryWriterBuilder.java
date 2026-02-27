package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.core.lib.GEO;
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
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
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
import com.ebremer.beakgraph.sniff.SD;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VOID;

public class PositionalDictionaryWriterBuilder {
    private static final Logger logger = LoggerFactory.getLogger(PositionalDictionaryWriterBuilder.class);
    private File src;
    private File dest;
    //private final HashSet<Node> shared = new HashSet<>();
    private final HashSet<Node> graphs = new HashSet<>();
    private final HashSet<Node> subjects = new HashSet<>();
    private final HashSet<Node> predicates = new HashSet<>();
    private final HashSet<Node> objects = new HashSet<>();
    private final HashSet<String> dataTypes = new HashSet<>();
    private final Stats stats = new Stats();
    private long numQuads;
    private String name;
    private final ArrayList<Quad> quadslist = new ArrayList<>(100_000_000);
    private Quad[] quads = null;
    private final HashMap<Node,Node> bmap = new HashMap<>(10_000_000);
    private boolean spatial = false;
    private int MaxX = Integer.MIN_VALUE;
    private int MaxY = Integer.MIN_VALUE;
   
    private static final Node[] asHilbert = {
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert0"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert1"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert2"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert3"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert4"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert5"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert6"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert7"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert8"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert9"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert10"),
        NodeFactory.createURI("https://halcyon.is/ns/asHilbert11")
    };
    private static final Node[] low = {
        NodeFactory.createURI("https://halcyon.is/ns/low0"),
        NodeFactory.createURI("https://halcyon.is/ns/low1"),
        NodeFactory.createURI("https://halcyon.is/ns/low2"),
        NodeFactory.createURI("https://halcyon.is/ns/low3"),
        NodeFactory.createURI("https://halcyon.is/ns/low4"),
        NodeFactory.createURI("https://halcyon.is/ns/low5"),
        NodeFactory.createURI("https://halcyon.is/ns/low6"),
        NodeFactory.createURI("https://halcyon.is/ns/low7"),
        NodeFactory.createURI("https://halcyon.is/ns/low8"),
        NodeFactory.createURI("https://halcyon.is/ns/low9"),
        NodeFactory.createURI("https://halcyon.is/ns/low10"),
        NodeFactory.createURI("https://halcyon.is/ns/low11")
    };
    private static final Node[] high = {
        NodeFactory.createURI("https://halcyon.is/ns/high0"),
        NodeFactory.createURI("https://halcyon.is/ns/high1"),
        NodeFactory.createURI("https://halcyon.is/ns/high2"),
        NodeFactory.createURI("https://halcyon.is/ns/high3"),
        NodeFactory.createURI("https://halcyon.is/ns/high4"),
        NodeFactory.createURI("https://halcyon.is/ns/high5"),
        NodeFactory.createURI("https://halcyon.is/ns/high6"),
        NodeFactory.createURI("https://halcyon.is/ns/high7"),
        NodeFactory.createURI("https://halcyon.is/ns/high8"),
        NodeFactory.createURI("https://halcyon.is/ns/high9"),
        NodeFactory.createURI("https://halcyon.is/ns/high10"),
        NodeFactory.createURI("https://halcyon.is/ns/high11")
    };
    private static final Node[] hasRange = {
        NodeFactory.createURI("https://halcyon.is/ns/hasRange0"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange1"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange2"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange3"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange4"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange5"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange6"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange7"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange8"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange9"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange10"),
        NodeFactory.createURI("https://halcyon.is/ns/hasRange11")
    };
    private static final Node[] asWKT = {
        NodeFactory.createURI("https://halcyon.is/ns/asWKT0"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT1"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT2"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT3"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT4"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT5"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT6"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT7"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT8"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT9"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT10"),
        NodeFactory.createURI("https://halcyon.is/ns/asWKT11")
    };
    private static final Node[] hilbertCorner = {
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner0"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner1"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner2"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner3"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner4"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner5"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner6"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner7"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner8"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner9"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner10"),
        NodeFactory.createURI("https://halcyon.is/ns/hilbertCorner11")
    };
    
    //private static final Node low = HAL.low.asNode();
    //private static final Node high = HAL.high.asNode();
    //private static final Node hasRange = HAL.hasRange.asNode();
    //private static final Node member = RDFS.member.asNode();
    
    private BGVoIDSD xvoid = new BGVoIDSD("https://ebremer.com/void/");
   
    public File getDestination() {
        return dest;
    }
   
    public Quad[] getQuads() {
        return quads;
    }
  
    public Set<Node> getSubjects() {
        return subjects;
    }
   
    public Set<Node> getPredicates() {
        return predicates;
    }
   
    public Set<Node> getObjects() {
        return objects;
    }
   
    public Set<Node> getGraphs() {
        return graphs;
    }
   
    public PositionalDictionaryWriterBuilder setSource(File src) {
        this.src = src;
        return this;
    }
    public PositionalDictionaryWriterBuilder setSpatial(boolean flag) {
        this.spatial = flag;
        return this;
    }
   
    public PositionalDictionaryWriterBuilder setDestination(File dest) {
        this.dest = dest;
        return this;
    }
    public long getNumberOfQuads() {
        return numQuads;
    }
    public Stats getStats() {
        return stats;
    }
   
    public String getName() {
        return name;
    }
    
    public Set<String> getDataTypes() {
        return dataTypes;
    }
   
    public PositionalDictionaryWriterBuilder setName(String name) {
        this.name = name;
        return this;
    }
   
    public void maxExtent(Polygon poly) {
        Envelope env = poly.getEnvelopeInternal();
        MaxX = Math.max(MaxX, (int) env.getMaxX());
        MaxY = Math.max(MaxY, (int) env.getMaxY());
    }
    
    /*
    private synchronized Node getSafeBnode() {
        Node bnode = NodeFactory.createBlankNode(String.format("b%020d", bmap.size() ));
        bmap.put(bnode, bnode);
        return bnode;
    }*/
    
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

    private ArrayList<Quad> AddSpatial(Quad quad) {
        final Polygon[] scales = PolygonScaler.toPolygons(quad.getObject().getLiteralLexicalForm());
        final ArrayList<Quad> qqq = new ArrayList<>();
        if (scales == null) {
            return qqq;
        }
        final String[] wktScales = PolygonScaler.toWKT(scales);
        //List<GridCell> cells;
        /*
        if (scales.length>0) {
            cells = PolygonScaler.getGridCells(scales[0], scales.length);
        } else {
            logger.trace("NADA: {}", quad);
            cells = new ArrayList<>();
        }*/
        
        /*
        try {
            qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), HAL.hilbertCentroid.asNode(), NodeFactory.createLiteralByValue(HilbertSpace.getCentroidHilbertIndex(scales[0]))));
        } catch (IllegalArgumentException ex) {
            logger.error("Bad polygon bro1 : {}", quad.getObject().getLiteralLexicalForm());
            return qqq;
        }*/
        for (int s=0; s<scales.length; s++) {
            List<Node> tiles = generateGridURNs(scales[s],s);
            try {
                for (int ii=0; ii<tiles.size(); ii++) {
                    qqq.add( Quad.create(tiles.get(ii), quad.getSubject(), asWKT[s], NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)));
                }            
            } catch (Throwable ex) {
                logger.error(ex.getMessage());
            }
            try {
                long[] corners = HilbertSpace.getBoundingBoxHilbertIndices(scales[s]);
                qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[0])));
                qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[1])));
                qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[2])));
                qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), hilbertCorner[s], NodeFactory.createLiteralByValue(corners[3])));          
                qqq.add( Quad.create(Params.SPATIAL, quad.getSubject(), asWKT[s], NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)) );
                /*
                for (int ss=0; ss<cells.size(); ss++) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(Params.SPATIALSTRING).append("/").append(cells.get(ss));
                    Node collection = NodeFactory.createURI(sb.toString());
                    qqq.add( Quad.create(Params.SPATIAL, collection, member, quad.getSubject()));
                }*/
            } catch (IllegalArgumentException ex) {
                logger.error("Bad polygon bro2 : {}", quad.getObject().getLiteralLexicalForm());
                return qqq;
            } 
        }
        return qqq;
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
        if (!graphs.contains(g)) {
            if (g.isBlank()) {
                stats.numBlankNodes++;
            } else if (g.isURI()) {
                stats.numIRI++;
            } else {
                throw new Error("This shouldn't be in here : "+g);
            }
            graphs.add(g);
        }
        /*
        if (!shared.contains(s)) {
            if (objects.contains(s)) {
                subjects.remove(s);
                objects.remove(s);
                shared.add(s);
            } else {*/
                if (s.isBlank()) {
                    stats.numBlankNodes++;
                } else if (s.isURI()) {
                    stats.numIRI++;
                }
                subjects.add(s);
       //     }
        //}
        if (!predicates.contains(p)) {
            stats.numIRI++;
            predicates.add(p);
        }
        //if (!shared.contains(o)) {
         //   if (subjects.contains(o)) {
           //     subjects.remove(o);
             //   objects.remove(o);
              //  shared.add(o);
            //} else {
                if (o.isBlank()) {
                    stats.numBlankNodes++;
                } else if (o.isURI()) {
                    stats.numIRI++;
                } else if (o.isLiteral()) {
                    if (!objects.contains(o)) {
                        String dt = o.getLiteralDatatypeURI();
                        dataTypes.add(dt);
                        if (dt.equals(XSD.xlong.getURI())) {
                            Number n = (Number) o.getLiteralValue();
                            this.stats.maxLong = Math.max(this.stats.maxLong, n.longValue());
                            this.stats.minLong = Math.min(this.stats.minLong, n.longValue());
                            this.stats.numLong++;
                        } else if (dt.equals(XSD.xint.getURI())) {
                            Number n = (Number) o.getLiteralValue();
                            this.stats.maxInteger = Math.max(this.stats.maxInteger, n.intValue());
                            this.stats.minInteger = Math.min(this.stats.minInteger, n.intValue());
                            this.stats.numInteger++;
                        } else if (dt.equals(XSD.integer.getURI())) {
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
                        } else if (dt.equals(XSD.xstring.getURI())) {
                            String wow = (String) o.getLiteralValue();                           
                            this.stats.longestStringLength = Math.max(this.stats.longestStringLength, wow.length());
                            this.stats.shortestStringLength = Math.min(this.stats.shortestStringLength, wow.length());
                            this.stats.numStrings++;
                        } else if (dt.equals(GEO.wktLiteral.getURI())) {
                            String wow = o.getLiteralLexicalForm();
                            this.stats.longestStringLength = Math.max(this.stats.longestStringLength, wow.length());
                            this.stats.shortestStringLength = Math.min(this.stats.shortestStringLength, wow.length());
                            this.stats.numStrings++;
                        } else if (dt.equals(XSD.dateTime.getURI())) {
                            //String wow = (String) o.getLiteralValue();
                            String lex = o.getLiteralLexicalForm(); // e.g. "2024-11-23T15:17:39Z"
                            int t = lex.indexOf('T');
                            String wow = (t > 0) ? lex.substring(0, t) : lex;
                            this.stats.longestStringLength = Math.max(this.stats.longestStringLength, wow.length());
                            this.stats.shortestStringLength = Math.min(this.stats.shortestStringLength, wow.length());
                            this.stats.numStrings++;
                        } else if (dt.equals(XSD.xboolean.getURI())) {
                            String wow = (String) o.getLiteralValue();
                            this.stats.longestStringLength = Math.max(this.stats.longestStringLength, wow.length());
                            this.stats.shortestStringLength = Math.min(this.stats.shortestStringLength, wow.length());
                            this.stats.numStrings++;
                        } else {
                            logger.error("I DON'T KNOW WHAT TO DO WITH : {}", o);
                        }
                    }                    
                } else {
                    throw new Error("WHAT THE HELL IS THIS : "+o);
                }
                objects.add(o);
         //   }
       // }
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
                        ProcessQuad(quad);
                        xvoid.add(quad);
                        if (spatial && isGeoLiteral(quad)) {
                            StructuredTaskScope.Subtask<ArrayList<Quad>> task = scope.fork(() -> AddSpatial(quad));
                            spatialTasks.add(task);
                        }
                    });
                try {
                    scope.join();
                } catch (IllegalArgumentException ex) {
                    logger.error(ex.getMessage());
                }
            } catch (InterruptedException ex) {
                System.getLogger(PositionalDictionaryWriter.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
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
            stats.numGraphs = graphs.size();
            stats.numSubjects = subjects.size();
            stats.numPredicates = predicates.size();
            stats.numObjects = objects.size();
            //stats.numShared = shared.size();
        } catch (FileNotFoundException e) {
            throw new IOException("Source file not found: " + src, e);
        } catch (IOException e) {
            throw new IOException("I/O error while reading RDF source", e);
        } catch (Throwable ex) {
            java.util.logging.Logger.getLogger(PositionalDictionaryWriterBuilder.class.getName()).log(Level.SEVERE, null, ex);
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
