package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.junit.jupiter.api.Test;

/** BG-318: the third "scale" argument of geof:sfIntersects was parsed and ignored; it is refused now. */
class SpatialFunctionArityTest {

    private static final String PREFIXES = """
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        """;

    @Test
    void aThirdArgumentIsRefusedAtBuildTime() {
        com.ebremer.beakgraph.turbo.Spatial.init();
        Dataset ds = DatasetFactory.create();
        ds.getDefaultModel().add(ds.getDefaultModel().createResource("http://ex.org/f"),
                ds.getDefaultModel().createProperty("http://www.opengis.net/ont/geosparql#asWKT"),
                ds.getDefaultModel().createTypedLiteral("POLYGON((0 0,10 0,10 10,0 10,0 0))", "http://www.opengis.net/ont/geosparql#wktLiteral"));
        String three = PREFIXES + "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"POLYGON((1 1,2 1,2 2,1 2,1 1))\"^^geo:wktLiteral, 2)) }";
        Exception ex = assertThrows(Exception.class, () -> {
            try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(three)).build()) {
                qe.execSelect().hasNext();
            }
        });
        boolean named = false;
        for (Throwable c = ex; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains("takes two arguments")) named = true;
        }
        assertTrue(named, "the arity error must name the rule: " + ex);
        String two = PREFIXES + "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"POLYGON((1 1,2 1,2 2,1 2,1 1))\"^^geo:wktLiteral)) }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(two)).build()) {
            assertTrue(qe.execSelect().hasNext(), "the two-argument form still answers");
        }
    }
}
