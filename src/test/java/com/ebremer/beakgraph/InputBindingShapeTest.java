package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Bindings flowing INTO a BeakGraph BGP (VALUES, BIND, joins) must come back
 * out well-formed. SolverLibBeak.convert copies every input var into the
 * BindingNodeId for id-level solving, and BindingBG also mounts the original
 * binding as its BindingBase parent - before the fix each input var was
 * therefore exposed twice: size() was inflated, vars() yielded duplicates, and
 * DISTINCT could not merge such a row with an equal-valued row of normal shape
 * (two identical rows came back).
 */
class InputBindingShapeTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:s1 ex:p ex:o1 .
        """;

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("shape.ttl").toFile();
        File h5 = dir.resolve("shape.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static int countRows(String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void valuesSeededRowHasWellFormedShape() {
        try (QueryExec qe = QueryExec.dataset(ds.asDatasetGraph())
                .query(PREFIX + "SELECT * WHERE { VALUES ?x { ex:s1 } ?x ex:p ?o }").build()) {
            RowSet rows = qe.select();
            assertTrue(rows.hasNext());
            Binding b = rows.next();
            assertFalse(rows.hasNext(), "exactly one row expected");
            assertEquals(2, b.size(), "two variables must count as two, not re-count the input var");
            List<Var> vars = new ArrayList<>();
            b.vars().forEachRemaining(vars::add);
            Set<Var> distinct = new HashSet<>(vars);
            assertEquals(distinct.size(), vars.size(), "vars() must not yield duplicates: " + vars);
            assertEquals(Set.of(Var.alloc("x"), Var.alloc("o")), distinct);
            assertEquals(NodeFactory.createURI("http://ex.org/s1"), b.get(Var.alloc("x")));
            assertEquals(NodeFactory.createURI("http://ex.org/o1"), b.get(Var.alloc("o")));
        }
    }

    @Test
    void distinctMergesValuesSeededRowsWithScannedRows() {
        assertEquals(1, countRows("SELECT DISTINCT ?x WHERE { { ?x ex:p ex:o1 } "
                + "UNION { VALUES ?x { ex:s1 } ?x ex:p ex:o1 } }"),
                "the seeded row and the scanned row bind the same term and must merge");
    }

    @Test
    void distinctMergesBindSeededRowsWithScannedRows() {
        assertEquals(1, countRows("SELECT DISTINCT ?x WHERE { { ?x ex:p ex:o1 } "
                + "UNION { BIND(ex:s1 AS ?x) ?x ex:p ex:o1 } }"));
    }

    @Test
    void termNotInStoreStillRidesThroughToOutput() {
        // ?y is not used by the pattern; its term does not exist in this store
        // (a "does not exist" id) and must still come back bound to the
        // original term via the parent binding.
        try (QueryExec qe = QueryExec.dataset(ds.asDatasetGraph())
                .query(PREFIX + "SELECT * WHERE { VALUES ?y { ex:missing } ?s ex:p ?o }").build()) {
            RowSet rows = qe.select();
            assertTrue(rows.hasNext());
            Binding b = rows.next();
            assertEquals(NodeFactory.createURI("http://ex.org/missing"), b.get(Var.alloc("y")));
            assertEquals(3, b.size());
        }
    }
}
