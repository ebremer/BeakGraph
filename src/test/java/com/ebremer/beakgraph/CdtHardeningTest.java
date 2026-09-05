package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.SystemARQ;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CDT hardening over a real store, plus the
 * empty-store regression the SPARQL-CDTs suite exposed.
 */
class CdtHardeningTest {

    private static final String PRE =
        "PREFIX cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> PREFIX : <http://ex.org/> ";

    private static final String TTL = """
        @prefix : <http://ex.org/> .
        @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        :s :list "[9]"^^cdt:List .
        :s :list "[10]"^^cdt:List .
        :s :list "[2]"^^cdt:List .
        :a :val "1"^^xsd:int .
        :b :val "7"^^xsd:int .
        :c :val "9"^^xsd:int .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("hardening.ttl");
        Files.writeString(src, TTL);
        File dest = dir.resolve("hardening.h5").toFile();
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

    /**
     * C.5: ORDER BY over composite literals must use ARQ's value-based relative
     * order, not the dictionary's. The two disagree by construction here: the
     * dictionary ranks lexically ("[10]" < "[2]" < "[9]"), ARQ ranks by value
     * ([2] < [9] < [10]) - if index order leaked through ORDER BY, the result
     * order would be lexical.
     */
    @Test
    void orderByUsesValueOrderNotDictionaryOrder() {
        List<String> got = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PRE + "SELECT ?l WHERE { :s :list ?l } ORDER BY ?l")).build()) {
            qe.execSelect().forEachRemaining(sol ->
                    got.add(sol.getLiteral("l").getLexicalForm()));
        }
        assertEquals(List.of("[2]", "[9]", "[10]"), got,
                "ORDER BY must follow ARQ's value order, not the dictionary's lexical order");
    }

    /**
     * C.8: numeric range FILTERs must stay exact in a store whose literals
     * dictionary ALSO holds composite literals (the ValueCluster pushdown walks
     * that dictionary; a composite literal mis-entering a numeric cluster would
     * add or drop rows).
     */
    @Test
    void rangeFiltersStayExactWithCompositeLiteralsPresent() {
        assertEquals(Set.of("http://ex.org/b", "http://ex.org/c"),
                selectSubjects("SELECT ?s WHERE { ?s :val ?v FILTER(?v > 5) }"));
        assertEquals(Set.of("http://ex.org/a"),
                selectSubjects("SELECT ?s WHERE { ?s :val ?v FILTER(?v < 5) }"));
        assertEquals(Set.of("http://ex.org/s"),
                selectSubjects("SELECT ?s WHERE { ?s :list ?l FILTER(?l = \"[9]\"^^cdt:List) }"));
    }

    /**
     * C.10: {@code ARQ.setStrictMode()} anywhere in the JVM silently flips
     * {@code SystemARQ.EnableCDTs} off; opening a BeakGraph dataset must re-pin
     * it, because stored cdt: literals need composite semantics to query.
     */
    @Test
    void enableCdtsIsRePinnedByDatasetCreation() {
        try {
            ARQ.setStrictMode();
            assertFalse(SystemARQ.EnableCDTs, "strict mode should have flipped the flag off");
            Dataset fresh = bg.getDataset(); // BGDatasetGraph ctor re-pins
            assertTrue(SystemARQ.EnableCDTs, "opening a BeakGraph dataset must re-pin EnableCDTs");
            try (QueryExecution qe = QueryExecution.dataset(fresh)
                    .query(QueryFactory.create(PRE
                            + "SELECT (cdt:size(\"[1, 2]\"^^cdt:List) AS ?n) WHERE {}")).build()) {
                assertEquals(2, qe.execSelect().next().getLiteral("n").getInt());
            }
        } finally {
            ARQ.setNormalMode();
        }
        assertTrue(SystemARQ.EnableCDTs);
    }

    /**
     * The empty-store regression found by the SPARQL-CDTs suite: a store built
     * from an empty source has no dictionary groups at all, and scan-shaped
     * queries NPE'd in ScanChunks/SimpleNodeTable on the null dictionaries.
     * Every query shape must answer empty instead.
     */
    @Test
    void emptyStoreAnswersEmptyNotNpe() throws Exception {
        Path src = dir.resolve("empty.ttl");
        Files.writeString(src, "");
        File dest = dir.resolve("empty.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).build().write();
        try (BeakGraph empty = BG.getBeakGraph(dest)) {
            Dataset eds = empty.getDataset();
            for (String q : new String[]{
                    "SELECT * WHERE { ?s ?p ?o }",
                    "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }",
                    "SELECT * WHERE { <http://ex.org/x> ?p ?o }",
                    "ASK { ?s ?p ?o }"}) {
                try (QueryExecution qe = QueryExecution.dataset(eds)
                        .query(QueryFactory.create(q)).build()) {
                    if (qe.getQuery().isAskType()) {
                        assertFalse(qe.execAsk(), q);
                    } else {
                        assertFalse(qe.execSelect().hasNext(), q + " must answer zero rows");
                    }
                }
            }
        }
    }

    private Set<String> selectSubjects(String query) {
        Set<String> out = new TreeSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PRE + query)).build()) {
            qe.execSelect().forEachRemaining(sol -> out.add(sol.getResource("s").getURI()));
        }
        return out;
    }
}
