package com.ebremer.beakgraph.ng;

import org.apache.jena.query.Dataset;

/**
 *
 * @author erich
 */
public interface AbstractProcess {
    
    public void Process(BeakWriter bw, Dataset ds);
    
}
