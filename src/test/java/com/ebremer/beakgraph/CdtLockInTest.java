package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SPARQL-CDT lock-in: composite datatype literals (cdt:List / cdt:Map) already
 * work over a BeakGraph store with zero BeakGraph code - they ride the
 * term-exact strings path, and Jena 6.x registers the datatypes, all 16 cdt:
 * functions, the FOLD aggregate, and the UNFOLD operator by default. This test
 * converts that accidental behavior into defended behavior: if a Jena upgrade,
 * a parser-profile change, or a storage change breaks any of it, this is what
 * notices.
 *
 * <p>The 16 functions exercised (namespace http://w3id.org/awslabs/neptune/SPARQL-CDTs/):
 * List, Map, concat, contains, containsKey, containsTerm, get, head, keys,
 * merge, put, remove, reverse, size, subseq, tail.
 */
class CdtLockInTest {

    private static final String TTL = """
        @prefix : <http://ex.org/> .
        @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
        :s :list   "[1, 2, 3]"^^cdt:List .
        :s :map    "{\\"k1\\": 5, \\"k2\\": 7}"^^cdt:Map .
        :s :nested "[[1, 2], [3]]"^^cdt:List .
        :s :mixed  "[<http://ex.org/x>, \\"tag\\"@en, null]"^^cdt:List .
        :g :v 1 .
        :g :v 2 .
        :g :v 3 .
        """;

    private static final String PRE =
        "PREFIX cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> PREFIX : <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("cdt.ttl");
        Files.writeString(src, TTL);
        File dest = dir.resolve("cdt.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).build().write();
        bg = BG.getBeakGraph(dest);
        ds = bg.getDataset();
    }

    @AfterAll
    static void close() throws Exception {
        if (bg != null) {
            bg.close();
        }
    }

    private static QuerySolution one(String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PRE + query)).build()) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext(), "no solution for: " + query);
            QuerySolution qs = rs.nextSolution();
            assertTrue(!rs.hasNext(), "more than one solution for: " + query);
            return qs;
        }
    }

    private static List<QuerySolution> all(String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PRE + query)).build()) {
            List<QuerySolution> out = new ArrayList<>();
            qe.execSelect().forEachRemaining(out::add);
            return out;
        }
    }

    @Test
    void compositeLiteralsRoundTripTermExactly() {
        Set<String> stored = new TreeSet<>();
        ds.getDefaultModel().listStatements().forEachRemaining(st -> {
            if (st.getObject().isLiteral()
                    && st.getObject().asNode().getLiteralDatatypeURI()
                            .startsWith("http://w3id.org/awslabs/neptune/SPARQL-CDTs/")) {
                stored.add(st.getObject().asNode().getLiteralLexicalForm()
                        + " ^^ " + st.getObject().asNode().getLiteralDatatypeURI());
            }
        });
        Set<String> expected = new TreeSet<>(Set.of(
            "[1, 2, 3] ^^ http://w3id.org/awslabs/neptune/SPARQL-CDTs/List",
            "{\"k1\": 5, \"k2\": 7} ^^ http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map",
            "[[1, 2], [3]] ^^ http://w3id.org/awslabs/neptune/SPARQL-CDTs/List",
            "[<http://ex.org/x>, \"tag\"@en, null] ^^ http://w3id.org/awslabs/neptune/SPARQL-CDTs/List"));
        assertEquals(expected, stored, "CDT literals must round-trip lexical-form- and datatype-exact");
    }

    @Test
    void listFunctionsOverTheStore() {
        QuerySolution s = one("""
            SELECT * WHERE {
              :s :list ?l .
              :s :nested ?nl .
              BIND(cdt:size(?l) AS ?sz)
              BIND(cdt:get(?l, 1) AS ?first)
              BIND(cdt:head(?l) AS ?hd)
              BIND(cdt:contains(?l, 2) AS ?has2)
              BIND(cdt:containsTerm(?l, 3) AS ?hasT3)
              BIND(cdt:size(cdt:tail(?l)) AS ?tsz)
              BIND(cdt:get(cdt:reverse(?l), 1) AS ?rfirst)
              BIND(cdt:get(cdt:subseq(?l, 2, 2), 1) AS ?sub1)
              BIND(cdt:size(cdt:concat(?l, ?l)) AS ?csz)
              BIND(cdt:size(cdt:List(9, 8)) AS ?nlsz)
              BIND(cdt:get(cdt:get(?nl, 1), 2) AS ?nested12)
            }""");
        assertEquals(3, s.getLiteral("sz").getInt());
        assertEquals(1, s.getLiteral("first").getInt());
        assertEquals(1, s.getLiteral("hd").getInt());
        assertTrue(s.getLiteral("has2").getBoolean());
        assertTrue(s.getLiteral("hasT3").getBoolean());
        assertEquals(2, s.getLiteral("tsz").getInt());
        assertEquals(3, s.getLiteral("rfirst").getInt());
        assertEquals(2, s.getLiteral("sub1").getInt());
        assertEquals(6, s.getLiteral("csz").getInt());
        assertEquals(2, s.getLiteral("nlsz").getInt());
        assertEquals(2, s.getLiteral("nested12").getInt());
    }

    @Test
    void mapFunctionsOverTheStore() {
        QuerySolution s = one("""
            SELECT * WHERE {
              :s :map ?m .
              BIND(cdt:get(?m, "k1") AS ?g)
              BIND(cdt:size(?m) AS ?sz)
              BIND(cdt:containsKey(?m, "k1") AS ?ck)
              BIND(cdt:size(cdt:keys(?m)) AS ?ksz)
              BIND(cdt:size(cdt:merge(?m, ?m)) AS ?msz)
              BIND(cdt:size(cdt:put(?m, "k3", 9)) AS ?psz)
              BIND(cdt:size(cdt:remove(?m, "k1")) AS ?rsz)
              BIND(cdt:containsKey(cdt:Map("a", 1), "a") AS ?cm)
            }""");
        assertEquals(5, s.getLiteral("g").getInt());
        assertEquals(2, s.getLiteral("sz").getInt());
        assertTrue(s.getLiteral("ck").getBoolean());
        assertEquals(2, s.getLiteral("ksz").getInt());
        assertEquals(2, s.getLiteral("msz").getInt());
        assertEquals(3, s.getLiteral("psz").getInt());
        assertEquals(1, s.getLiteral("rsz").getInt());
        assertTrue(s.getLiteral("cm").getBoolean());
    }

    @Test
    void foldAggregatesOverTheStore() {
        QuerySolution list = one("""
            SELECT (cdt:size(?l) AS ?n) (cdt:get(?l, 1) AS ?first) (DATATYPE(?l) AS ?dt) WHERE {
              { SELECT (FOLD(?v ORDER BY ?v) AS ?l) WHERE { :g :v ?v } }
            }""");
        assertEquals(3, list.getLiteral("n").getInt());
        assertEquals(1, list.getLiteral("first").getInt());
        assertEquals("http://w3id.org/awslabs/neptune/SPARQL-CDTs/List",
                list.getResource("dt").getURI());

        QuerySolution map = one("""
            SELECT (cdt:containsKey(?mp, "a") AS ?ca) (cdt:get(?mp, "b") AS ?gb) WHERE {
              { SELECT (FOLD(?k, ?v) AS ?mp) WHERE { VALUES (?k ?v) { ("a" 1) ("b" 2) } } }
            }""");
        assertTrue(map.getLiteral("ca").getBoolean());
        assertEquals(2, map.getLiteral("gb").getInt());
    }

    @Test
    void unfoldOverTheStore() {
        List<QuerySolution> rows = all(
            "SELECT ?e WHERE { :s :list ?l . UNFOLD(?l AS ?e) } ORDER BY ?e");
        assertEquals(3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, rows.get(i).getLiteral("e").getInt());
        }

        List<QuerySolution> indexed = all(
            "SELECT ?e ?i WHERE { :s :list ?l . UNFOLD(?l AS ?e, ?i) } ORDER BY ?i");
        assertEquals(3, indexed.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, indexed.get(i).getLiteral("i").getInt(), "1-based position");
            assertEquals(i + 1, indexed.get(i).getLiteral("e").getInt(), "element at that position");
        }

        List<QuerySolution> entries = all(
            "SELECT ?k ?v WHERE { :s :map ?m . UNFOLD(?m AS ?k, ?v) } ORDER BY ?k");
        assertEquals(2, entries.size());
        assertEquals("k1", entries.get(0).getLiteral("k").getString());
        assertEquals(5, entries.get(0).getLiteral("v").getInt());
        assertEquals("k2", entries.get(1).getLiteral("k").getString());
        assertEquals(7, entries.get(1).getLiteral("v").getInt());
    }
}
