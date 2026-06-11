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
import java.util.Set;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for the high-severity correctness fixes:
 * <ul>
 *   <li>C3 - a concrete object term absent from the dictionary must yield no
 *       solutions (BGIteratorSO previously treated "not in dictionary" like an
 *       unbound variable and scanned the whole object range, so
 *       {@code ASK { <s> <p> <missing> }} answered true).</li>
 *   <li>C6a - {@code FILTER(?o <= X)} range pushdown must keep the boundary row
 *       (BGIteratorPOS subtracted 1 from an already-inclusive upperBound).</li>
 *   <li>H4 - {@code GRAPH <x>} where x is an entity but not a graph must return
 *       empty instead of leaking id-0 padding rows (which crashed extract), and
 *       containsGraph must only report actual graphs.</li>
 *   <li>H1 - xsd:long values needing 58-63 bits (>= 2^56) must build instead of
 *       crashing on an unsupported bit width.</li>
 *   <li>H2 - sorted-adjacent strings sharing a high surrogate (emoji) must not
 *       be corrupted by the front-coded dictionary splitting the pair.</li>
 * </ul>
 */
class HighSeverityFixesRoundTripTest {

    // p/q/r emoji pairs: each pair is sorted-adjacent and shares its prefix up to
    // (and including) the high surrogate of the emoji, so front-coding would split
    // the surrogate pair. Three pairs so at least two are front-coded within a
    // block regardless of where the FCD block boundary (blockSize 16) falls.
    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:s1 ex:p ex:o1 .
        ex:s1 ex:q "alpha" .
        ex:v1 ex:value 10 .
        ex:v2 ex:value 20 .
        ex:v3 ex:value 30 .
        ex:v4 ex:value 40 .
        ex:t1 ex:stamp "100000000000000000"^^xsd:long .
        ex:t2 ex:stamp "5"^^xsd:long .
        ex:e1 ex:label "p😀" .
        ex:e2 ex:label "p😁" .
        ex:e3 ex:label "q😀" .
        ex:e4 ex:label "q😁" .
        ex:e5 ex:label "r😀" .
        ex:e6 ex:label "r😁" .
        """;

    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("highsev.ttl").toFile();
        File h5 = dir.resolve("highsev.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static boolean ask(String pattern) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "ASK { " + pattern + " }")).build()) {
            return qe.execAsk();
        }
    }

    private static int count(String whereBody) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT * WHERE { " + whereBody + " }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    private static Set<String> selectURIs(String var, String query) {
        Set<String> uris = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get(var);
                if (n != null && n.isURIResource()) uris.add(n.asResource().getURI());
            }
        }
        return uris;
    }

    // --- C3: concrete object absent from the dictionary -------------------

    @Test
    void askWithAbsentConcreteObjectIsFalse() {
        assertTrue(ask("ex:s1 ex:p ex:o1"), "control: existing triple must match");
        assertFalse(ask("ex:s1 ex:p ex:notInStore"), "URI object absent from the store");
        assertFalse(ask("ex:s1 ex:q \"not in store\""), "literal object absent from the store");
        assertFalse(ask("ex:s1 ex:p \"42\"^^xsd:int"), "literal absent from the store");
    }

    @Test
    void selectWithAbsentConcreteObjectIsEmpty() {
        assertEquals(0, count("ex:s1 ex:p ex:notInStore"));
        assertEquals(0, count("ex:s1 ex:q \"not in store\""));
    }

    // --- C6a: FILTER upper-bound boundary row ------------------------------

    @Test
    void filterLessOrEqualKeepsBoundaryRow() {
        assertEquals(Set.of("http://ex.org/v1", "http://ex.org/v2", "http://ex.org/v3"),
            selectURIs("s", "SELECT ?s WHERE { ?s ex:value ?o FILTER(?o <= 30) }"));
    }

    @Test
    void filterLessThanKeepsLastIncludedRow() {
        assertEquals(Set.of("http://ex.org/v1", "http://ex.org/v2", "http://ex.org/v3"),
            selectURIs("s", "SELECT ?s WHERE { ?s ex:value ?o FILTER(?o < 40) }"));
    }

    // --- H4: GRAPH on a non-graph entity -----------------------------------

    @Test
    void graphPatternOnNonGraphEntityIsEmpty() {
        // ex:o1 exists in the entity dictionary (as an object) but is not a graph.
        // Previously the GSPO padding rows (S=0) leaked through and extract(0) threw.
        assertEquals(0, count("GRAPH ex:o1 { ?s ?p ?o }"));
    }

    @Test
    void graphPatternOnRealNamedGraphStillWorks() {
        // The writer always embeds VoID metadata in its own named graph; the id-0
        // padding guard must not suppress real rows.
        assertTrue(count("GRAPH <" + Params.VOIDSTRING + "> { ?s ?p ?o }") > 0,
            "VoID named graph must still be queryable");
    }

    @Test
    void containsGraphReportsOnlyActualGraphs() {
        assertFalse(ds.asDatasetGraph().containsGraph(NodeFactory.createURI("http://ex.org/o1")),
            "an entity that never occurs as a graph is not a graph");
        assertTrue(ds.asDatasetGraph().containsGraph(NodeFactory.createURI(Params.VOIDSTRING)),
            "the VoID metadata graph is a real named graph");
    }

    // --- H1: xsd:long needing 58-63 bits ------------------------------------

    @Test
    void largeLongRoundTrips() {
        // 1e17 > 2^56, so the value-derived width is 58 - previously rejected by
        // BitPackedUnSignedLongBuffer (which supports 1..57 and 64 only), crashing
        // the whole build in @BeforeAll.
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?v WHERE { ex:t1 ex:stamp ?v }")).build()) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext());
            Literal v = rs.next().getLiteral("v");
            assertEquals("100000000000000000", v.getLexicalForm());
            assertEquals(XSD.xlong.getURI(), v.getDatatypeURI());
            assertFalse(rs.hasNext());
        }
    }

    // --- H2: surrogate-pair-safe front coding -------------------------------

    @Test
    void supplementaryPlaneStringsRoundTrip() {
        String[][] cases = {
            {"http://ex.org/e1", "p😀"},
            {"http://ex.org/e2", "p😁"},
            {"http://ex.org/e3", "q😀"},
            {"http://ex.org/e4", "q😁"},
            {"http://ex.org/e5", "r😀"},
            {"http://ex.org/e6", "r😁"},
        };
        for (String[] c : cases) {
            // Extraction: the stored value must come back byte-identical.
            try (QueryExecution qe = QueryExecution.dataset(ds)
                    .query(QueryFactory.create(PREFIX + "SELECT ?v WHERE { <" + c[0] + "> ex:label ?v }")).build()) {
                ResultSet rs = qe.execSelect();
                assertTrue(rs.hasNext(), "no label for " + c[0]);
                assertEquals(c[1], rs.next().getLiteral("v").getLexicalForm(),
                    "label for " + c[0] + " must survive front coding");
            }
            // Lookup: the exact term must be locatable again.
            assertEquals(Set.of(c[0]),
                selectURIs("s", "SELECT ?s WHERE { ?s ex:label \"" + c[1] + "\" }"),
                "term lookup for label of " + c[0]);
        }
    }
}
