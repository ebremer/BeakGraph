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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-76: a BeakGraph row binding computes its own variables once. Its
 * size(), vars(), contains(), equals() and hashCode() must match a plain
 * binding of the same terms for a many-variable row whose id chain spans
 * several layers and copies parent variables.
 */
class BindingBGShapeTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:a ex:p1 ex:b . ex:b ex:p2 ex:c . ex:c ex:p3 ex:d . ex:d ex:p4 ex:e . ex:e ex:p5 ex:f . ex:f ex:p6 "end" .
        ex:a2 ex:p1 ex:b2 . ex:b2 ex:p2 ex:c2 . ex:c2 ex:p3 ex:d2 . ex:d2 ex:p4 ex:e2 . ex:e2 ex:p5 ex:f2 . ex:f2 ex:p6 "end2" .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("chain.ttl").toFile();
        Files.writeString(ttl.toPath(), TTL, StandardCharsets.UTF_8);
        File h5 = dir.resolve("chain.ttl.h5").toFile();
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    @Test
    void rowBindingsHaveThePlainBindingsShape() {
        String q = "PREFIX ex: <http://ex.org/> SELECT * WHERE { VALUES ?start { ex:a ex:a2 } "
                + "?start ex:p1 ?b . ?b ex:p2 ?c . ?c ex:p3 ?d . ?d ex:p4 ?e . ?e ex:p5 ?f . ?f ex:p6 ?end }";
        int rows = 0;
        try (QueryExec exec = QueryExec.dataset(bg.getDataset().asDatasetGraph()).query(QueryFactory.create(q)).build()) {
            RowSet rs = exec.select();
            while (rs.hasNext()) {
                Binding row = rs.next();
                BindingBuilder plain = Binding.builder();
                Set<Var> seen = new HashSet<>();
                Iterator<Var> vars = row.vars();
                while (vars.hasNext()) {
                    Var v = vars.next();
                    assertTrue(seen.add(v), "vars() must not repeat " + v);
                    Node n = row.get(v);
                    assertTrue(n != null, v + " must resolve");
                    plain.add(v, n);
                }
                Binding copy = plain.build();
                assertEquals(7, row.size(), "start, b, c, d, e, f, end");
                assertEquals(copy.size(), row.size());
                assertFalse(row.isEmpty());
                assertTrue(row.contains(Var.alloc("start")) && row.contains(Var.alloc("end")));
                assertFalse(row.contains(Var.alloc("nope")));
                assertEquals(copy, row);
                assertEquals(row, copy);
                assertEquals(copy.hashCode(), row.hashCode());
                rows++;
            }
        }
        assertEquals(2, rows);
    }
}
