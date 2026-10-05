package com.ebremer.beakgraph.turbo;

import com.ebremer.ns.GEOF;
import org.apache.jena.sparql.function.FunctionRegistry;


/**
 *
 * @author erich
 */
public final class Spatial {
    private static Spatial spatial = null;

    private Spatial() {
        // Register a real, JTS-backed geof:sfIntersects so spatial filters that the
        // index rewrite does not capture still evaluate correctly. Registering a
        // filter function globally is the normal Jena pattern; what this must NOT
        // do is alter standard semantics for other datasets - the old code here
        // remove()d the rdfs:member property function JVM-wide. BG datasets opt out
        // of rdfs:member rewriting via their own dataset-scoped registry instead
        // (see BGDatasetGraph).
        // Global registration is a courtesy for plain Jena models: it must not
        // overwrite an implementation the embedding application registered
        // (jena-geosparql's CRS-aware one, say). BeakGraph datasets get this
        // evaluator through their own dataset-scoped registry regardless
        // (BGDatasetGraph.wire), so BG answers never depend on load order.
        if (!FunctionRegistry.get().isRegistered(GEOF.sfIntersects.getURI())) {
            FunctionRegistry.get().put(GEOF.sfIntersects.getURI(), Intersects.class);
        }
    }

    public synchronized static void init() {
        if (spatial == null) {
            spatial = new Spatial();
        }
    }
}
