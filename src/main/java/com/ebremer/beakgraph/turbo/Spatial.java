package com.ebremer.beakgraph.turbo;

import com.ebremer.beakgraph.core.lib.GEOF;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.jena.sparql.pfunction.PropertyFunctionRegistry;
import org.apache.jena.vocabulary.RDFS;


/**
 *
 * @author erich
 */
public final class Spatial {
    private static Spatial spatial = null;

    private Spatial() {
        PropertyFunctionRegistry.get().remove(RDFS.member.getURI());
        FunctionRegistry.get().put(GEOF.sfIntersects.getURI(), Intersects.class);                    
    }
    
    public synchronized static void init() {
        if (spatial == null) {
            spatial = new Spatial();
        }
    }
}
