package com.ebremer.beakgraph.rdf;

import com.ebremer.beakgraph.solver.OpExecutorBG;
import com.ebremer.beakgraph.solver.QueryEngineBeak;
import com.ebremer.beakgraph.solver.StageGeneratorDirectorBG;
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

/**
 *
 * @author erich
 */
public class BeakGraph extends GraphBase {
    private static final Object initLock = new Object() ;
    private static volatile boolean initialized = false ;
    
    static { JenaSystem.init(); }

    private final BeakReader reader;

    public BeakGraph(URI uri) throws IOException {
        init();
        this.reader = new BeakReader(uri);
    }
    
    private static void init() {
        if ( initialized ) {
            return ;
        }
        synchronized(initLock) {
            if ( initialized ) {
                System.out.println("BeakGraph Initialized.");
                return ;
            }
            System.out.println("BeakGraph Initializing...");
            initialized = true ;
            QC.setFactory(ARQ.getContext(), OpExecutorBG.opExecFactoryBG);
            QueryEngineBeak.register();
            wireIntoExecution() ;
            System.out.println("BeakGraph Initialized.");
        }
    }
    
    public BeakReader getReader() {
        return reader;
    }
    
    @Override
    public void close() {
        reader.close();
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple tp) {
        return new PredicateIterator(reader);
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
    public ExtendedIterator<Triple> find() {
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
