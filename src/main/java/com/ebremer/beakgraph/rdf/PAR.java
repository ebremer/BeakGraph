package com.ebremer.beakgraph.rdf;

import java.util.HashMap;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 *
 * @author erich
 */
public class PAR {
    private final String p;
    private final HashMap<DataType,StructVector> cs;
    
    public PAR(String p) {
        this.p = p;
        cs = new HashMap<>();
    }
    
    public Node getPredicateNode() {
        return NodeFactory.createURI(p);
    }
    
    public void close() {
        cs.forEach((k,v)->{
            v.close();
        });
    }
    
    public void put(String dt, StructVector v) {
        DataType datatype = DataType.getType(dt);
        if (!cs.containsKey(datatype)) {
            cs.put(datatype, v);
        } else {
            throw new Error("DUPLCATE VECTOR TYPE FOR "+p);
        }
    }
    
    public HashMap<DataType,StructVector> getAllTypes() {
        return cs;
    }
}
