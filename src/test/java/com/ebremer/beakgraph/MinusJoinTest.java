package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.MinusJoin;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpMinus;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MinusJoin}: {@code (join (minus A B) C)} run as {@code (minus (join A C) B)} only where the answers are the
 * same. Every query is compared with Jena's own engine over the same data; the counter says whether it was rewritten.
 */
class MinusJoinTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/>\n";
    private static final String TRIG = """
            @prefix ex: <http://ex.org/> .
            ex:s1 ex:p ex:o1 ; ex:val "1" ; ex:tag ex:t1 ; ex:bad ex:x .
            ex:s2 ex:p ex:o2 ; ex:val "non-detect" ; ex:tag ex:t2 .
            ex:s3 ex:p ex:o3 ; ex:val "3" ; ex:bad ex:y .
            ex:s4 ex:p ex:o1 ; ex:val "4" ; ex:tag ex:t1 .
            ex:o1 ex:label "one" . ex:o2 ex:label "two" . ex:o3 ex:label "three" .
            ex:t1 ex:label "tag one" . ex:t2 ex:label "tag two" .
            ex:g { ex:s1 ex:p ex:o1 . ex:s5 ex:p ex:o2 . ex:s5 ex:bad ex:z . ex:o1 ex:label "one" . ex:o2 ex:label "two" . }
            """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("minus.trig");
        Files.write(src, TRIG.getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve("minus.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        store = bg.getDataset();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        reference = DatasetFactory.wrap(dsg);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    /** Asserts Jena's answer; returns how many joins the store rewrote for it. */
    private static long rewrites(String query) {
        long before = MinusJoin.HITS.get();
        List<String> got = rows(store, query);
        long n = MinusJoin.HITS.get() - before;
        assertEquals(rows(reference, query), got, query);
        return n;
    }

    /** The patterns after the MINUS join the rows before it: rewritten, and Jena's answer. */
    @ParameterizedTest
    @ValueSource(strings = {
        // shares nothing with the MINUS
        "SELECT * { ?s ex:p ?o MINUS { ?s ex:bad ?z } ?o ex:label ?l }",
        // shares ?o, bound in every row before the MINUS
        "SELECT * { ?s ex:p ?o MINUS { ?o ex:label \"two\" } ?o ex:label ?l }",
        // samples_longchain_biota's shape: a VALUES list taken away, then unconnected patterns
        "SELECT * { ?s ex:val ?v MINUS { VALUES ?v { \"non-detect\" } } ?s ex:p ?o . ?o ex:label ?l . ?t ex:label \"tag one\" }",
        // 60day's shape: counts from a subquery, a filter on them, the MINUS, then patterns on ?s
        "SELECT * { ?s ex:p ?o { SELECT ?s (COUNT(*) AS ?n) { ?s ?pp ?oo } GROUP BY ?s } FILTER(?n > 2) MINUS { ?s ex:bad ?z } ?o ex:label ?l }",
        // inside GRAPH
        "SELECT * { GRAPH ex:g { ?s ex:p ?o MINUS { ?s ex:bad ?z } ?o ex:label ?l } }",
    })
    void rewrittenWithJenasAnswer(String query) {
        assertTrue(rewrites(query) >= 1, "rewritten: " + query);
    }

    /** A rewrite would change the answer, or can't be shown not to: left alone, and Jena's answer. */
    @ParameterizedTest
    @ValueSource(strings = {
        // ?t is shared by the MINUS and the patterns after it, but unbound before it: rewritten, every row would go
        "SELECT * { ?s ex:p ?o MINUS { ?x ex:tag ?t } ?s ex:tag ?t }",
        // ?t from BIND, which an error leaves unbound
        "SELECT * { ?s ex:p ?o BIND(IF(?o = ex:o1, ex:t1, 1/0) AS ?t) MINUS { ?x ex:tag ?t } ?s ex:tag ?t }",
        // ?t from a VALUES row with UNDEF
        "SELECT * { VALUES ?t { ex:t1 UNDEF } ?s ex:p ?o MINUS { ?x ex:tag ?t } ?s ex:tag ?t }",
        // ?t from an OPTIONAL
        "SELECT * { ?s ex:p ?o OPTIONAL { ?s ex:tag ?t } MINUS { ?x ex:tag ?t } ?t ex:label ?tl }",
    })
    void leftAloneWithJenasAnswer(String query) {
        assertEquals(0, rewrites(query), "not rewritten: " + query);
    }

    /** Other shapes around a MINUS: whichever way they go, Jena's answer. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * { MINUS { ?s ex:bad ?z } ?s ex:p ?o }",
        "SELECT * { ?s ex:p ?o FILTER(?o != ex:o3) MINUS { ?s ex:bad ?z } ?o ex:label ?l }",
        "SELECT * { ?s ex:p ?o MINUS { ?s ex:bad ?z } FILTER EXISTS { ?o ex:label ?x } ?o ex:label ?l }",
        "SELECT * { ?s ex:p ?o MINUS { ?s ex:bad ?z } FILTER(!BOUND(?l)) OPTIONAL { ?o ex:label ?l } }",
        "SELECT * { ?s ex:p ?o OPTIONAL { ?s ex:tag ?t MINUS { ?t ex:label \"tag two\" } ?t ex:label ?tl } }",
        "SELECT * { { ?s ex:p ?o MINUS { ?s ex:bad ?z } } UNION { ?s ex:tag ?o } ?o ex:label ?l }",
        "SELECT * { ?s ex:p ?o MINUS { ?s ex:bad ?z } MINUS { ?s ex:tag ex:t2 } ?o ex:label ?l }",
        "SELECT (COUNT(*) AS ?n) { ?s ex:p ?o MINUS { ?s ex:bad ?z } ?o ex:label ?l }",
        "SELECT * FROM <urn:x-arq:UnionGraph> { ?s ex:p ?o MINUS { ?s ex:bad ?z } ?o ex:label ?l }",
    })
    void otherShapesGiveJenasAnswer(String query) {
        rewrites(query);
    }

    @Test
    void sixtyDaysShapeBecomesAMinusOverASequence() {
        String q = PREFIX + "SELECT * { ?s ex:p ?o { SELECT ?s (COUNT(*) AS ?n) { ?s ?pp ?oo } GROUP BY ?s } FILTER(?n > 2) "
                + "MINUS { ?s ex:bad ?z } ?o ex:label ?l FILTER(?l != \"x\") ?o ex:label ?l2 }";
        Op op = MinusJoin.transform(Algebra.optimize(Algebra.compile(QueryFactory.create(q))));
        Op body = (op instanceof OpProject p) ? p.getSubOp() : op;
        assertTrue(body instanceof OpMinus, "a MINUS on top: " + op);
        assertTrue(((OpMinus) body).getLeft() instanceof OpSequence, "the patterns after it run per row: " + op);
    }

    @Test
    void offWhenTurnedOff() {
        String q = "SELECT * { ?s ex:p ?o MINUS { ?s ex:bad ?z } ?o ex:label ?l }";
        System.setProperty("beakgraph.minus.join", "false");
        try {
            assertEquals(0, rewrites(q));
        } finally {
            System.clearProperty("beakgraph.minus.join");
        }
        assertTrue(rewrites(q) >= 1);
    }

    @Test
    void boundVarsCountsOnlyWhatEveryRowBinds() {
        assertEquals(java.util.Set.of(Var.alloc("s"), Var.alloc("o")),
                bound("SELECT * { ?s ex:p ?o OPTIONAL { ?s ex:tag ?t } BIND(1/0 AS ?b) }"));
        assertEquals(java.util.Set.of(Var.alloc("s")), bound("SELECT * { { ?s ex:p ?o } UNION { ?s ex:tag ?t } }"));
        assertEquals(java.util.Set.of(Var.alloc("s"), Var.alloc("o")), bound("SELECT * { VALUES ?v { 1 UNDEF } ?s ex:p ?o }"));
        assertEquals(java.util.Set.of(Var.alloc("v"), Var.alloc("s"), Var.alloc("o")), bound("SELECT * { VALUES ?v { 1 2 } ?s ex:p ?o }"));
        assertEquals(java.util.Set.of(Var.alloc("s")), bound("SELECT ?s (COUNT(*) AS ?n) { ?s ex:p ?o } GROUP BY ?s"));
        assertEquals(java.util.Set.of(Var.alloc("s"), Var.alloc("o"), Var.alloc("g")), bound("SELECT * { GRAPH ?g { ?s ex:p ?o } }"));
        assertFalse(bound("SELECT * { ?s ex:p ?o MINUS { ?s ex:bad ?z } }").contains(Var.alloc("z")));
    }

    private static java.util.Set<Var> bound(String q) {
        return MinusJoin.boundVars(Algebra.optimize(Algebra.compile(QueryFactory.create(PREFIX + q))));
    }
}
