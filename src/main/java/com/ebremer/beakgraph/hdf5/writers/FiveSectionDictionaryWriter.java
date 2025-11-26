package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.lib.GEO;
import com.ebremer.beakgraph.core.lib.HAL;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import com.ebremer.halcyon.hilbert.HilbertSpace;
import com.ebremer.halcyon.hilbert.PolygonScaler;
import io.jhdf.api.WritableGroup;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.XSD;
import org.davidmoten.hilbert.Range;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;

/**
 *
 * @author Erich Bremer
 */
public class FiveSectionDictionaryWriter implements GSPODictionary, AutoCloseable, DictionaryWriter {
    private final DictionaryWriter shareddict;
    private final DictionaryWriter subjectsdict;
    private final DictionaryWriter predicatesdict;
    private final DictionaryWriter objectsdict;
    private final DictionaryWriter graphsdict;
    private final long numQuads;
    private final String name;
    private final Quad[] quads;
    
    public FiveSectionDictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        name = builder.getName();
        numQuads = builder.getNumberOfQuads();
        quads = builder.getQuads();
        MultiTypeDictionaryWriter.Builder shared = new MultiTypeDictionaryWriter.Builder();        
        MultiTypeDictionaryWriter.Builder subjects = new MultiTypeDictionaryWriter.Builder();
        MultiTypeDictionaryWriter.Builder predicates = new MultiTypeDictionaryWriter.Builder();
        MultiTypeDictionaryWriter.Builder objects = new MultiTypeDictionaryWriter.Builder();
        MultiTypeDictionaryWriter.Builder graphs = new MultiTypeDictionaryWriter.Builder();
        Stats stats = builder.getStats();
        IO.println(stats);
        shareddict = shared  
            .setName("shared")
            .setNodes(builder.getShared())
            .setStats(builder.getStats())
            .enable(Types.IRI)
            .build();
        graphsdict = graphs
            .setName("graphs")
            .setNodes(builder.getGraphs())
            .setStats(builder.getStats())
            .enable(Types.IRI)
            .build();        
        subjectsdict = subjects
            .setName( "subjects")
            .setNodes( builder.getSubjects() )
            .setStats( builder.getStats() )
            .enable( Types.IRI )
            .setOffset( shareddict.getNumberOfNodes() )
            .build();
        predicatesdict = predicates
            .setName("predicates")
            .setNodes(builder.getPredicates())
            .setStats(builder.getStats())
            .enable(Types.IRI)
            .build();
        objectsdict = objects
            .setName("objects")
            .setNodes(builder.getObjects())
            .setStats(builder.getStats())
            .enable( Types.IRI, Types.DOUBLE, Types.FLOAT, Types.LONG, Types.INTEGER, Types.STRING )
            .setOffset( shareddict.getNumberOfNodes() )
            .build();
    }
    
    public Quad[] getQuads() {
        return quads;
    }
    
    public long getNumberOfQuads() {
        return numQuads;
    }

    public long getNumberOfGraphs() {
        return graphsdict.getNumberOfNodes();
    }

    public long getNumberOfShared() {
        return shareddict.getNumberOfNodes();
    }
    
    public long getNumberOfSubjects() {
        return subjectsdict.getNumberOfNodes();
    }
    
    public long getNumberOfPredicates() {
        return predicatesdict.getNumberOfNodes();
    }
    
    public long getNumberOfObjects() {
        return objectsdict.getNumberOfNodes();
    }
    
    @Override
    public long locateGraph(Node element) {
        long c = ((Dictionary) graphsdict).locate(element);
        if (c > 0) {
            return c;
        }
        throw new Error("Cannot resolve Graph : "+element);
    }

    @Override
    public long locateSubject(Node element) {
        long c = ((Dictionary) shareddict).locate(element);
        if (c > 0) {
            return c;
        } else {
            c = ((Dictionary) subjectsdict).locate(element);
            if (c > 0) {
                return c + shareddict.getNumberOfNodes();
            }
        }
        throw new Error("Cannot resolve Subject : "+element);
    }
    
    @Override
    public long locatePredicate(Node element) {
        long c = ((Dictionary) predicatesdict).locate(element);
        if (c > 0) {
            return c;
        }
        throw new Error("Cannot resolve Predicate : "+element);
    }

    @Override
    public long locateObject(Node element) {
        long c;
        if (element.isLiteral()) {
            c = ((Dictionary) objectsdict).locate(element);
            if (c < 0) {
                return -1;
            }
            return c + shareddict.getNumberOfNodes();
        }
        c = ((Dictionary) shareddict).locate(element);
        if (c > 0) {
            return c;
        }
        c = ((Dictionary) objectsdict).locate(element);
        if (c < 0) {
            return -1;
        }
        return c + shareddict.getNumberOfNodes();    
    }

    @Override
    public Object extractGraph(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractSubject(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractPredicate(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
    @Override
    public Object extractObject(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        
    }

    @Override
    public long getNumberOfNodes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Node> getNodes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void Add(WritableGroup group) {
        WritableGroup dictionary = group.putGroup(name);
        if ( graphsdict.getNumberOfNodes() > 0 ) graphsdict.Add( dictionary );
        if ( shareddict.getNumberOfNodes() > 0 ) shareddict.Add( dictionary );
        if ( subjectsdict.getNumberOfNodes() > 0 ) subjectsdict.Add( dictionary );
        if ( predicatesdict.getNumberOfNodes() > 0 ) predicatesdict.Add( dictionary );
        if ( objectsdict.getNumberOfNodes() > 0 ) objectsdict.Add( dictionary );
    }

    @Override
    public Stream<Node> streamSubjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Stream<Node> streamPredicates() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Stream<Node> streamObjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Stream<Node> streamGraphs() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static class Builder {        
        private File src;
        private File dest;
        private final HashSet<Node> shared = new HashSet<>();
        private final LinkedHashSet<Node> graphs = new LinkedHashSet<>();
        private final LinkedHashSet<Node> subjects = new LinkedHashSet<>();
        private final HashSet<Node> predicates = new HashSet<>();
        private final LinkedHashSet<Node> objects = new LinkedHashSet<>();
        private final Stats stats = new Stats();
        private long numQuads;
        private String name;
        private final ArrayList<Quad> quadslist = new ArrayList<>();
        private Quad[] quads = null;
        private final HashMap<Node,Node> bmap = new HashMap<>();
        private boolean spatial = false;
        private final HilbertSpace hs = new HilbertSpace();
        private int MaxX = Integer.MIN_VALUE;
        private int MaxY = Integer.MIN_VALUE;

        
        public File getDestination() {
            return dest;
        }
        
        public Set<Node> getShared() {
            return shared;
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
        
        public Builder setSource(File src) {
            this.src = src;
            return this;
        }

        public Builder setSpatial(boolean flag) {
            this.spatial = flag;
            return this;
        }        
        
        public Builder setDestination(File dest) {
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
        
        public Builder setName(String name) {
            this.name = name;
            return this;
        }
        
        public void maxExtent(Polygon poly) {
            Envelope env = poly.getEnvelopeInternal();
            MaxX = Math.max(MaxX, (int) env.getMaxX());
            MaxY = Math.max(MaxY, (int) env.getMaxY());
        }        
        
        private synchronized Node getSafeBnode() {
            Node bnode = NodeFactory.createBlankNode(String.format("b%020d", bmap.size() ));
            bmap.put(NodeFactory.createBlankNode(), bnode);
            return bnode;
        }
        
        private ArrayList<Quad> AddSpatial(Quad quad, String wkt) {            
            Polygon[] scales = PolygonScaler.toHilbert(wkt);
            final ArrayList<Quad> qqq = new ArrayList<>();
            for (int s=0; s<scales.length; s++) {                
                ArrayList<Range> ranges = hs.Polygon2Hilbert(scales[s]);
                Node hp = getSafeBnode();                
                for (int c=0; c<ranges.size(); c++) {                        
                    Node range = getSafeBnode();
                    Quad qLow = Quad.create(quad.getGraph(), range, HAL.low.asNode(), NodeFactory.createLiteralByValue(ranges.get(c).low()));
                    Quad qHigh = Quad.create(quad.getGraph(), range, HAL.high.asNode(), NodeFactory.createLiteralByValue(ranges.get(c).high()));
                    Quad qrange = Quad.create(quad.getGraph(), hp, HAL.hasRange.asNode(), range);
                    qqq.add(qrange);
                    qqq.add(qLow);
                    qqq.add(qHigh);
                }
                Node pred = NodeFactory.createURI(String.format("https://halcyon.is/ns/asHilbert%d",s));
                Quad qHp = Quad.create(quad.getGraph(), quad.getSubject(), pred, hp);
                qqq.add(qHp);
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
                        Node neo = NodeFactory.createBlankNode(String.format("b%020d", bmap.size() ));
                        bmap.put(g, neo);
                        g = neo;
                    } else {
                        g = bmap.get(g);
                    }
                }
                if (s.isBlank()) {
                    if (!bmap.containsKey(s)) {
                        Node neo = NodeFactory.createBlankNode(String.format("b%020d", bmap.size() ));
                        bmap.put(s, neo);
                        s = neo;
                    } else {
                        s = bmap.get(s);
                    }
                }
                if (o.isBlank()) {
                    if (!bmap.containsKey(o)) {
                        Node neo = NodeFactory.createBlankNode(String.format("b%020d", bmap.size() ));
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
            if (!shared.contains(s)) {
                if (objects.contains(s)) {
                    subjects.remove(s);
                    objects.remove(s);
                    shared.add(s);
                } else {
                    if (s.isBlank()) {
                        stats.numBlankNodes++;
                    } else if (s.isURI()) {
                        stats.numIRI++;
                    }
                    subjects.add(s);
                }                            
            }
            if (!predicates.contains(p)) {
                stats.numIRI++;
                predicates.add(p);
            }
            if (!shared.contains(o)) {
                if (subjects.contains(o)) {
                    subjects.remove(o);
                    objects.remove(o);
                    shared.add(o);
                } else {
                    if (o.isBlank()) {
                        stats.numBlankNodes++;
                    } else if (o.isURI()) {
                        stats.numIRI++;
                    } else if (o.isLiteral()) {
                        if (!objects.contains(o)) {
                            String dt = o.getLiteralDatatypeURI();
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
                            }  else if (dt.equals(XSD.integer.getURI())) {
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
                                String lex = o.getLiteralLexicalForm();  // e.g. "2024-11-23T15:17:39Z"
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
                                IO.println("I DON'T KNOW WHAT TO DO WITH : "+o);
                                //throw new Error("I DON'T KNOW WHAT TO DO WITH : "+o);
                            }
                        }                                     
                    }
                    objects.add(o);
                }                     
            }      
        }
        
        public FiveSectionDictionaryWriter build() throws IOException {
            final AtomicLong quadcount = new AtomicLong();        
            System.out.print("Creating dictionary...");
            try ( InputStream xis = (src.toString().endsWith(".gz"))? new GZIPInputStream(new FileInputStream(src)): new FileInputStream(src)) {
                AsyncParserBuilder builder = AsyncParser.of(xis, Lang.TURTLE, null);
                builder.mutateSources(rdfBuilder->
                    rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven())
                );
                BlockingQueue<ArrayList<Quad>> completedResults = new LinkedBlockingQueue<>();
                ArrayList<ArrayList<Quad>> allResults = new ArrayList<>();
                try (var scope = StructuredTaskScope.open()) {
                builder
                    .streamQuads()
                    .map(quad -> quad.isDefaultGraph()?new Quad(Quad.defaultGraphIRI,quad.getSubject(),quad.getPredicate(),quad.getObject()):quad )
                    .map(q->AlignBnodes(q))
                    .forEach(quad->{
                        //IO.println(quad);
                        quadcount.incrementAndGet();
                        if (quadcount.get() % 1_000 == 0) {
                            System.out.println(completedResults.size()+" "+allResults.size()+"  Loaded " + quadcount.get() + " quads...");
                        }
                        quadslist.add(quad);
                        try {
                            ProcessQuad(quad);
                        } catch (Throwable ex) {
                            IO.println(ex.getMessage()+" "+quad);
                        }
                        if (spatial) {
                            Node o = quad.getObject();
                            if (o.isLiteral()) {
                                if (o.getLiteralDatatypeURI().equals(GEO.wktLiteral.toString())) {
                                    String wkt = o.getLiteralLexicalForm();
                                    scope.fork(() -> {
                                        ArrayList<Quad> result = AddSpatial(quad, wkt);
                                        try {
                                            completedResults.put(result);
                                        } catch (InterruptedException ex) {
                                            System.getLogger(FiveSectionDictionaryWriter.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
                                        }
                                    });
                                }
                            }
                            if (!completedResults.isEmpty()) {
                            try {
                                ArrayList<Quad> task = completedResults.take();
                                task.forEach(q->{
                                    quadslist.add(q);
                                    ProcessQuad(q);
                                });
                            } catch (InterruptedException ex) {
                                System.getLogger(FiveSectionDictionaryWriter.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
                            }
                            }
                        }
                    });
                    try {
                       scope.join();
                    } catch (StructuredTaskScope.FailedException e) {
                        System.err.println("A task failed: " + e.getCause());
                        throw new RuntimeException(e.getCause());
                    }
                    while (!completedResults.isEmpty()) {
                        try {
                            ArrayList<Quad> task = completedResults.take();
                            task.forEach(q->{
                                quadslist.add(q);
                                ProcessQuad(q);
                            });
                        } catch (InterruptedException ex) {
                            System.getLogger(FiveSectionDictionaryWriter.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
                        }
                    }                    
                    stats.numGraphs = graphs.size();
                    stats.numSubjects = subjects.size();
                    stats.numPredicates = predicates.size();
                    stats.numObjects = objects.size();
                    stats.numShared = shared.size();
                }
            } catch (FileNotFoundException ex) {
                throw new Error(ex.getMessage());
                //Logger.getLogger(FiveSectionDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                throw new Error(ex.getMessage());
                //Logger.getLogger(FiveSectionDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            }  catch (Throwable ex) {
                IO.println(ex.getMessage());
            }
            this.numQuads = quadcount.get();
            this.quads = quadslist.toArray(new Quad[0]);
            quadslist.clear();
            System.out.println("Dictionary created. Total Quads: " + this.numQuads);
            return new FiveSectionDictionaryWriter(this);
        }
    }
}
