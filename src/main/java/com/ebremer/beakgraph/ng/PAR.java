package com.ebremer.beakgraph.ng;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 *
 * @author erich
 */
public class PAR {
    private final String p;
    private final HashMap<DataType,ArrowFileReader> afrs;
    private final BeakReader reader;
    private final HashMap<Integer,HashMap<DataType,StructVector>> ngraphs;
    
    public PAR(String p, BeakReader reader) {
        this.reader = reader;
        this.p = p;
        afrs = new HashMap<>();
        ngraphs = new HashMap<>();
    }
    
    public Node getPredicateNode() {
        return NodeFactory.createURI(p);
    }
    
    public void close() {
        if (!afrs.isEmpty()) {
            afrs.forEach((k,v)->{
                try {
                    v.close();
                } catch (IOException ex) {
                    Logger.getLogger(PAR.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        }
        ngraphs.forEach((k,v)->{
            v.forEach((dt,vector)->{
                vector.close();
            });
        });
    }

    public void put(String dt, ArrowFileReader afr) {
        DataType datatype = DataType.getType(dt);
        if (!afrs.containsKey(datatype)) {
            afrs.put(datatype, afr);
        } else {
            throw new Error("YY DUPLCATE VECTOR TYPE FOR "+p);
        }
    }
    
    private VectorSchemaRoot cloneRoot(VectorSchemaRoot originalRoot) {
        VectorSchemaRoot theRoot = VectorSchemaRoot.create(originalRoot.getSchema(), reader.getBufferAllocator());
        VectorLoader loader = new VectorLoader(theRoot);
        VectorUnloader unloader = new VectorUnloader(originalRoot);
        try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
            loader.load(recordBatch);
        }
        return theRoot;
    }    
    
    public HashMap<DataType,StructVector> getAllTypes(int ng) {
        LoadVectors(ng);
        return ngraphs.get(ng);        
    }
    
    private void LoadVectors(int ng) {
        if (!ngraphs.containsKey(ng)) {
            HashMap<DataType,StructVector> ha = new HashMap<>();
            afrs.forEach((dt,afr)->{
                try {
                    ArrowBlock block = afr.getRecordBlocks().get(ng);
                    afr.loadRecordBatch(block);
                    StructVector v = (StructVector) cloneRoot(afr.getVectorSchemaRoot()).getVector(0);
                    ha.put(dt, v);
                } catch (IOException ex) {
                    Logger.getLogger(PAR.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
            ngraphs.put(ng, ha);
        }
    }
}
