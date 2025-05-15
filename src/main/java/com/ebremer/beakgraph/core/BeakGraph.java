package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.jena.BGReader;
import com.ebremer.beakgraph.hdf5.jena.OpExecutorBG;
import com.ebremer.beakgraph.hdf5.jena.QueryEngineBeak;
import com.ebremer.beakgraph.hdf5.jena.StageGeneratorDirectorBG;
import java.io.IOException;
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
    private final Node namedgraph;
    private final BGReader reader;
    private static final Logger logger = LoggerFactory.getLogger(BeakGraph.class);
    
    static { JenaSystem.init(); }

    public BeakGraph(BGReader reader, URI uri) throws IOException {
        this(reader, uri, uri);
    }
    
    public BeakGraph(BGReader reader, URI uri, URI base) throws IOException {
        //logger.trace("Create a BeakGraph -> "+uri.toString());
        init();
        this.reader = reader;
        this.namedgraph = Quad.defaultGraphIRI;
    }
    
    public BeakGraph(BGReader reader) throws IOException {
        this( reader, null, null);
    }
    
    public BeakGraph(Node namedgraph, BGReader reader) throws IOException {
        logger.trace("Create a SubBeakGraph -> <need subgraph>");
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
    
    public BGReader getReader() {
        return reader;
    }
    
    @Override
    public void close() {
        try {
            reader.close();
        } catch (Exception ex) {
            System.getLogger(BeakGraph.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
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
        return ((HDF5Reader) reader).graphBaseFind(namedgraph, tp);
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
        return reader.getNumberOfTriples("");
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
