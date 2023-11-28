package com.ebremer.beakgraph.ng;

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
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
public class BeakGraph extends GraphBase {
    private static final Object initLock = new Object() ;
    private static volatile boolean initialized = false ;
    public static String DICTIONARY = "dictionary.arrow";
    public static String NAMEDGRAPHS = "namedgraphs.arrow";
    private final int namedgraph;
    private final BeakReader reader;
    private static final Logger logger = LoggerFactory.getLogger(BeakGraph.class);
    
    static { JenaSystem.init(); }

    public BeakGraph(URI uri) throws IOException {
        logger.trace("Create a BeakGraph -> "+uri.toString());
        init();
        this.reader = new BeakReader(uri);
        this.namedgraph = 0;
    }
    
    public void warm(String predicate) {
        reader.warm(predicate, namedgraph);
    }
    
    public BeakGraph(int namedgraph, BeakReader reader) throws IOException {
        logger.trace("Create a SubBeakGraph -> "+reader.getURI().toString());
        init();
        this.reader = reader;
        this.namedgraph = namedgraph;
    }
    
    private static void init() {
        if ( initialized ) {
            return ;
        }
        synchronized(initLock) {
            if ( initialized ) {
                return ;
            }
            initialized = true ;
            QC.setFactory(ARQ.getContext(), OpExecutorBG.opExecFactoryBG);
            QueryEngineBeak.register();
            wireIntoExecution() ;
        }
    }
    
    public BeakReader getReader() {
        return reader;
    }
    
    @Override
    public void close() {
        reader.close();
    }
    
    public int getNamedGraph() {
        return namedgraph;
    }
    
    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple tp) {
        return new PredicateIterator(namedgraph, reader, tp);
    }
    
    @Override
    public ExtendedIterator<Triple> find() {
        return graphBaseFind(Triple.create(Node.ANY, Node.ANY, Node.ANY));
    }

    @Override
    public void add(Node s, Node p, Node o) throws AddDeniedException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public void delete(Node s, Node p, Node o) throws DeleteDeniedException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public Stream<Triple> stream() {
        throw new UnsupportedOperationException("Not supported yet."); 
    }
    
    @Override
    protected int graphBaseSize() {
        return reader.getNumberOfTriples();
    }
    
    private static void wireIntoExecution() {
        Context cxt = ARQ.getContext() ;
        StageGenerator orig = StageBuilder.chooseStageGenerator(cxt) ;
        StageGenerator stageGenerator = new StageGeneratorDirectorBG(orig) ;
        StageBuilder.setGenerator(ARQ.getContext(), stageGenerator) ;
    }

    public ReorderTransformation getReorderTransform() {
        return null;
    }
}
