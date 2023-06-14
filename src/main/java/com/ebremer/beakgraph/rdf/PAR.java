package com.ebremer.beakgraph.rdf;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 *
 * @author erich
 */
public class PAR {
    private final String p;
    private final HashMap<DataType,StructVector> cs;
    private final HashMap<DataType,ArrowFileReader> afrs;
    
    public PAR(String p) {
        this.p = p;
        cs = new HashMap<>();
        afrs = new HashMap<>();
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
        if (!cs.isEmpty()) {
            cs.forEach((k,v)->{
                v.close();
            });
        }
    }

    public void put(String dt, ArrowFileReader afr) {
        DataType datatype = DataType.getType(dt);
        if (!afrs.containsKey(datatype)) {
            afrs.put(datatype, afr);
        } else {
            throw new Error("YY DUPLCATE VECTOR TYPE FOR "+p);
        }
    }
    
    private void put(DataType datatype, StructVector v) {
        if (!cs.containsKey(datatype)) {
            cs.put(datatype, v);
        } else {
            throw new Error("XX DUPLCATE VECTOR TYPE FOR "+p);
        }
    }
    
    private synchronized void LoadVectors() {
        if (cs.isEmpty()) {
            afrs.forEach((dt,afr)->{
                try {
                    VectorSchemaRoot za = afr.getVectorSchemaRoot();
                    afr.loadNextBatch();
                    StructVector v = (StructVector) za.getVector(0);
                    put(dt,v);
                } catch (IOException ex) {
                    Logger.getLogger(PAR.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        }    
    }
    
    public HashMap<DataType,StructVector> getAllTypes() {
        LoadVectors();
        return cs;
    }
}
