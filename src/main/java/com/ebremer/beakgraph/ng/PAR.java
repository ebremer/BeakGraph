package com.ebremer.beakgraph.ng;

import com.ebremer.beakgraph.control.AllocatorCore;
import com.ebremer.beakgraph.control.VectorCache;
import com.ebremer.beakgraph.control.VectorRequest;
import com.ebremer.beakgraph.control.VectorRequestEngine;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class PAR {
    private final String p;
    private final HashMap<DataType,ArrowFileReader> afrs;
    private final BeakReader reader;
    private static final Logger logger = LoggerFactory.getLogger(PAR.class);
    private final String uuid = UUID.randomUUID().toString();
    private final String name;
    private static final Object lock = new Object();
    
    public PAR(String p, BeakReader reader) {
        this.reader = reader;
        this.p = p;
        afrs = new HashMap<>();
        name = p+"/"+uuid;
    }
    
    public Node getPredicateNode() {
        return NodeFactory.createURI(p);
    }
    
    public BeakReader getReader() {
        return reader;
    }
    
    public void close() {
        if (!afrs.isEmpty()) {
            afrs.forEach((k,v)->{
                try {
                    v.close();
                } catch (IOException ex) {
                    logger.error(ex.toString());
                }
            });
        }
    }

    public void put(String dt, ArrowFileReader afr) {
        DataType datatype = DataType.getType(dt);
        afrs.putIfAbsent(datatype, afr);
    }
       
    private Future<ConcurrentHashMap<DataType,StructVector>> TTN(VectorRequest vr) {
        synchronized (lock) {
            var future = VectorCache.getInstance().getCache(reader.getURI()).get(vr);        
            if (future==null) {
                logger.trace("Requesting Vector Load ===== "+vr+" ======= "+uuid);
                future = VectorRequestEngine.getInstance().getExecutorService().submit(new Loader(vr.namedgraph()));
                VectorCache.getInstance().getCache(reader.getURI()).putIfAbsent(vr, future);
            }
            logger.trace("Vector Load Requested ===== "+vr+" ======= "+uuid);
            return future;
        }
    }
        
    public ConcurrentHashMap<DataType,StructVector> getAllTypes(int ng) {
        VectorRequest vr = new VectorRequest(p,ng);
        var future = VectorCache.getInstance().getCache(reader.getURI()).get(vr);
        if (future==null) {
            future = TTN(vr);
        }
        try {
            return future.get();
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(PAR.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            java.util.logging.Logger.getLogger(PAR.class.getName()).log(Level.SEVERE, null, ex);
        }
        logger.debug("==== FAILURE  ============ "+p+"  "+ng+"  "+vr+" =============");
        return new ConcurrentHashMap<>();
    }
    
    public VectorSchemaRoot cloneRoot(VectorSchemaRoot srcRoot) {
        VectorSchemaRoot cloneRoot = VectorSchemaRoot.create(srcRoot.getSchema(), AllocatorCore.getInstance().getChildAllocator(reader.getURI(),name));
        VectorLoader loader = new VectorLoader(cloneRoot);
        VectorUnloader unloader = new VectorUnloader(srcRoot);
        try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
            loader.load(recordBatch);
        }
        return cloneRoot;
    }
    
    class Loader implements Callable<ConcurrentHashMap<DataType,StructVector>> {
        private final int ng;
             
        Loader(int ng) {
            this.ng = ng;
        }
        
        @Override
        public ConcurrentHashMap<DataType,StructVector> call() throws Exception {
            ConcurrentHashMap<DataType,StructVector> ha = new ConcurrentHashMap<>();
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                afrs.forEach((dt,afr)->{
                    executor.submit(() -> {
                        logger.debug("LoadVectors : "+p+"  "+ng+"  "+dt+"  "+uuid);
                        try {
                            ArrowBlock block = afr.getRecordBlocks().get(ng);
                            afr.loadRecordBatch(block);
                            StructVector v = (StructVector) cloneRoot(afr.getVectorSchemaRoot()).getVector(0);
                            //StructVector v = (StructVector) (afr.getVectorSchemaRoot()).getVector(0);
                            ha.put(dt, v);
                        } catch (IOException ex) {
                            logger.error(ex.toString());
                        }
                    });
                });
            }
            logger.debug("Vectors Loaded: "+p+"  "+ng+"  "+uuid);
            return ha;
        }
    }
}
