package com.ebremer.beakgraph.ng;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;

/**
 *
 * @author erich
 */
public interface SpecialProcess {
    
    public void Execute(BeakWriter bw, Resource ng, Model m);
    
}
