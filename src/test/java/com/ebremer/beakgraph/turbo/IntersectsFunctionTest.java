package com.ebremer.beakgraph.turbo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.ns.GEOF;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase2;
import org.apache.jena.sparql.function.FunctionFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-69 (per-row re-parsing of the query constant),
 * BG-70 (CRS ignored silently) and BG-74 (the global sfIntersects
 * registration clobbered an embedder's implementation and BG answers
 * depended on load order).
 */
class IntersectsFunctionTest {

    private static final String WKT = "http://www.opengis.net/ont/geosparql#wktLiteral";
    private static final String SQUARE = "POLYGON((0 0,10 0,10 10,0 10,0 0))";
    private static final String INSIDE = "POINT(5 5)";
    private static final String OUTSIDE = "POINT(50 50)";

    private static NodeValue wkt(String lex) {
        return NodeValue.makeNode(NodeFactory.createLiteralDT(lex, NodeFactory.getType(WKT)));
    }

    @TempDir
    Path dir;

    @Test
    void answersIntersectsAndCachesThePreparedConstant() throws Exception {
        Intersects fn = new Intersects();
        assertEquals(NodeValue.TRUE, fn.exec(List.of(wkt(INSIDE), wkt(SQUARE))));
        assertEquals(NodeValue.FALSE, fn.exec(List.of(wkt(OUTSIDE), wkt(SQUARE))));
        assertEquals(NodeValue.TRUE, fn.exec(List.of(wkt(SQUARE), wkt(INSIDE))), "argument order does not matter");
        // Invalid (bowtie) geometry is repaired, not an error.
        assertEquals(NodeValue.TRUE, fn.exec(List.of(wkt("POLYGON((0 0,10 10,10 0,0 10,0 0))"), wkt(INSIDE))));
        // Many rows against one constant: the constant is parsed and prepared once
        // per thread, so this stays fast (it re-parsed and re-validated per row).
        long t0 = System.nanoTime();
        StringBuilder big = new StringBuilder("POLYGON((");
        int n = 4000;
        for (int i = 0; i <= n; i++) {
            double a = 2 * Math.PI * (i % n) / n;
            big.append(String.format(java.util.Locale.ROOT, "%.4f %.4f", 500 + 400 * Math.cos(a), 500 + 400 * Math.sin(a)));
            if (i < n) big.append(',');
        }
        big.append("))");
        NodeValue region = wkt(big.toString());
        int hits = 0;
        for (int i = 0; i < 20_000; i++) {
            if (fn.exec(List.of(wkt("POINT(" + (i % 1000) + " " + (i / 20) + ")"), region)) == NodeValue.TRUE) hits++;
        }
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(hits > 0);
        assertTrue(ms < 20_000, "20k rows against a 4000-vertex constant took " + ms + " ms");
    }

    @Test
    void differentCrsIsAnEvaluationErrorNotARawComparison() {
        Intersects fn = new Intersects();
        String epsg = "<http://www.opengis.net/def/crs/EPSG/0/4326> ";
        String crs84 = "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> ";
        // Same CRS, prefixed or default: compared.
        assertEquals(NodeValue.TRUE, fn.exec(List.of(wkt(epsg + INSIDE), wkt(epsg + SQUARE))));
        assertEquals(NodeValue.TRUE, fn.exec(List.of(wkt(crs84 + INSIDE), wkt(SQUARE))), "an unprefixed literal adopts the other operand's CRS");
        assertEquals(NodeValue.TRUE, fn.exec(List.of(wkt(epsg + INSIDE), wkt(SQUARE))), "prefixed data is routinely queried unprefixed");
        // Two explicit, different CRSs: an error (the FILTER drops the row), not
        // a silent comparison of raw coordinates in different axis orders.
        ExprEvalException ex = assertThrows(ExprEvalException.class, () -> fn.exec(List.of(wkt(epsg + INSIDE), wkt(crs84 + SQUARE))));
        assertTrue(ex.getMessage().contains("CRS mismatch"), ex.getMessage());
    }

    @Test
    void beakGraphDatasetsUseTheirOwnEvaluatorRegardlessOfTheGlobalRegistration() throws Exception {
        File src = dir.resolve("s.ttl").toFile();
        Files.writeString(src.toPath(), "@prefix ex: <http://ex.org/> .\n@prefix geo: <http://www.opengis.net/ont/geosparql#> .\n"
                + "ex:a geo:asWKT \"" + SQUARE + "\"^^geo:wktLiteral .\n", StandardCharsets.UTF_8);
        File h5 = dir.resolve("s.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        String q = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
                + "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"" + INSIDE + "\"^^geo:wktLiteral)) }";
        FunctionRegistry global = FunctionRegistry.get();
        FunctionFactory original = global.get(GEOF.sfIntersects.getURI());
        assertNotNull(original);
        try {
            // An embedder's own (here: always-false) implementation takes the
            // global slot; BG datasets must still answer with theirs.
            global.put(GEOF.sfIntersects.getURI(), uri -> new FunctionBase2() {
                @Override public NodeValue exec(NodeValue a, NodeValue b) { return NodeValue.FALSE; }
            });
            Spatial.init();   // must NOT overwrite what the embedder registered
            assertFalse(global.get(GEOF.sfIntersects.getURI()) instanceof FunctionFactory f && f == original,
                    "Spatial.init must leave a pre-existing global registration alone");
            try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
                Dataset ds = bg.getDataset();
                assertSame(Intersects.class, FunctionRegistry.get(ds.asDatasetGraph().getContext())
                        .get(GEOF.sfIntersects.getURI()).create(GEOF.sfIntersects.getURI()).getClass(),
                        "the dataset-scoped registry carries BeakGraph's evaluator");
                try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
                    ResultSet rs = qe.execSelect();
                    assertTrue(rs.hasNext(), "BG's evaluator answers, not the embedder's always-false one");
                    assertEquals("http://ex.org/a", rs.next().getResource("f").getURI());
                }
            }
        } finally {
            global.put(GEOF.sfIntersects.getURI(), original);
        }
    }
}
