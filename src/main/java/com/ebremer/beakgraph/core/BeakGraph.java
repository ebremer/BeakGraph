package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.jena.BGReader;
import com.ebremer.beakgraph.hdf5.jena.StageGeneratorDirectorBG;
import com.ebremer.beakgraph.turbo.Spatial;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderLib;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class BeakGraph extends GraphBase implements AutoCloseable {
    private static final Object initLock = new Object() ;
    private static volatile boolean initialized = false ;
    private final Node namedgraph;
    private final BGReader reader;
    // Whether this instance owns (and on close() must close) the reader. Named-graph
    // views handed out by BGDatasetGraph.getGraph share the dataset's reader; closing
    // such a view must not shut the storage under every other graph of the dataset.
    private final boolean ownsReader;
    private static final Logger logger = LoggerFactory.getLogger(BeakGraph.class);
    private final URI uri;
    // Lazily-computed triple count for this graph. -1 = not yet computed; the graph
    // is read-only so the value is stable once counted.
    private int cachedSize = -1;
    // Lazily-built join-reorder transform (read-only graph -> built once, then reused).
    private volatile ReorderTransformation reorderTransform;

    static {
        JenaSystem.init();
        Spatial.init();
        com.ebremer.halcyon.hilbert.WKTDatatype.register();
    }

    public BeakGraph(BGReader reader, URI uri) {
        this(reader, uri, uri);
    }

    public BeakGraph(BGReader reader, URI uri, URI base) {
        logger.trace("BeakGraph -> {}", uri.toString());
        init();
        this.uri = uri;
        this.reader = reader;
        this.namedgraph = Quad.defaultGraphIRI;
        this.ownsReader = true;
    }
    
    public BeakGraph(BGReader reader) {
        this( reader, reader.getURI(), null);
    }
    
    public URI getURI() {
        return uri;
    }
        
    public BeakGraph(Node namedgraph, BGReader reader) {
        logger.trace("Create a SubBeakGraph -> {}", namedgraph);
        init();
        this.uri = reader.getURI();
        this.reader = reader;
        this.namedgraph = namedgraph;
        this.ownsReader = false; // a view over a reader owned by the dataset
    }
    
    private static void init() {
        if ( initialized ) {
            return ;
        }
        synchronized(initLock) {
            if ( initialized ) {
                return ;
            }
            // The OpExecutor factory is wired per-dataset (BGDatasetGraph's own
            // context), NOT into the global ARQ context: a global factory would
            // change query execution for every other dataset in the JVM. Only the
            // stage-generator director is global - the standard Jena pattern -
            // because it dispatches on the active graph's type and delegates
            // everything that is not a BeakGraph.
            wireIntoExecution() ;
            // Pin CDT support on: BeakGraph stores cdt:List/cdt:Map literals and
            // its comparator/guards assume their datatypes behave as composites.
            // Jena defaults this to true, but ARQ.setStrictMode() anywhere in the
            // JVM silently flips it off; BGDatasetGraph re-pins per dataset for
            // the strictMode-after-init case.
            org.apache.jena.sparql.SystemARQ.EnableCDTs = true;
            // Publish only after wiring succeeded, so a failure here is retried by
            // the next caller instead of leaving the JVM half-wired forever.
            initialized = true ;
        }
    }
    
    public BGReader getReader() {
        return reader;
    }
    
    @Override
    public void close() {
        if (!ownsReader) {
            return; // closing a named-graph view must not close the shared reader
        }
        try {
            reader.close();
        } catch (Exception ex) {
            logger.error("Error closing BeakGraph reader for {}", uri, ex);
        }
    }
    
    public Node getNamedGraph() {
        return namedgraph;
    }
    
    public Dataset getDataset() {
        BGDatasetGraph dsg = new BGDatasetGraph(this);
        return DatasetFactory.wrap(dsg);
    }
    
    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple tp) {
        return reader.graphBaseFind(namedgraph, tp);
    }
    
    @Override
    public ExtendedIterator<Triple> find() {
        return graphBaseFind(Triple.create(Node.ANY, Node.ANY, Node.ANY));
    }

    @Override
    public void add(Node s, Node p, Node o) throws AddDeniedException {
        throw new UnsupportedOperationException("Not supported yet add."); 
    }

    @Override
    public void delete(Node s, Node p, Node o) throws DeleteDeniedException {
        throw new UnsupportedOperationException("Not supported yet. delete"); 
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        throw new UnsupportedOperationException("Not supported yet. Stream<Triple> stream(Node s, Node p, Node o)"); 
    }

    @Override
    public Stream<Triple> stream() {
        throw new UnsupportedOperationException("Not supported yet. stream"); 
    }
    
    @Override
    protected int graphBaseSize() {
        int size = cachedSize;
        if (size >= 0) {
            return size;
        }
        // Answered from index structure when possible (a few select1 calls);
        // otherwise count this graph's distinct triples by scanning it once.
        // Quads are de-duplicated in the index, so each triple is visited exactly
        // once. Cached because the graph is read-only.
        long count = reader.countTriples(namedgraph);
        if (count < 0) {
            count = 0;
            ExtendedIterator<Triple> it = graphBaseFind(Triple.create(Node.ANY, Node.ANY, Node.ANY));
            try {
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
            } finally {
                it.close();
            }
        }
        size = (count > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
        cachedSize = size;
        return size;
    }
    
    private static void wireIntoExecution() {
        Context cxt = ARQ.getContext() ;
        StageGenerator orig = StageBuilder.chooseStageGenerator(cxt) ;
        StageGenerator stageGenerator = new StageGeneratorDirectorBG(orig) ;
        StageBuilder.setGenerator(ARQ.getContext(), stageGenerator) ;
        // The engine that carries BG's per-execution wiring to every dataset
        // whose default graph is a BeakGraph - including the Model path, which
        // never sees a BGDatasetGraph context (see QueryEngineBG).
        QueryEngineBG.register();
    }

    /**
     * Join-reordering transform for BGP optimization. Built lazily from the persisted VoID stats
     * (BeakGraph-/index-aware selectivity) and cached, falling back to Jena's fixed heuristic when
     * no stats are available. Returns a usable transform rather than null so multi-pattern BGPs are
     * reordered most-selective-first.
     */
    public ReorderTransformation getReorderTransform() {
        ReorderTransformation r = reorderTransform;
        if (r == null) {
            synchronized (this) {
                r = reorderTransform;
                if (r == null) {
                    reorderTransform = r = buildReorderTransform();
                }
            }
        }
        return r;
    }

    private ReorderTransformation buildReorderTransform() {
        try {
            VoidStats stats = VoidStats.load(reader);
            if (stats.usable()) {
                return new BGReorderTransform(stats);
            }
            logger.debug("No VoID stats for {}; using fixed reorder heuristic", uri);
        } catch (RuntimeException ex) {
            logger.warn("Failed to load VoID stats for {}; using fixed reorder heuristic", uri, ex);
        }
        return ReorderLib.fixed();
    }
}
