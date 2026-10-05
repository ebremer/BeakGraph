package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for H3: numeric literals (xsd:int / xsd:long / xsd:float /
 * xsd:double) are stored by VALUE, and the reader regenerates the canonical
 * lexical form. Two lexical variants of one value ("01" vs "1"^^xsd:int) used
 * to become two term-distinct dictionary entries that both extract to the same
 * canonical term - duplicate "equal" entries that break the strict ordering
 * the dictionary binary search relies on, leaving some triples unreachable by
 * term lookup.
 * <p>
 * Policy: such literals are canonicalized at ingest (the value-typed storage
 * never preserved their lexical form anyway), so all variants collapse onto
 * one term and every triple is findable via the canonical form.
 */
class NumericCanonicalizationTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:v "01"^^xsd:int .
        ex:b ex:v "1"^^xsd:int .
        ex:c ex:v "007"^^xsd:long .
        ex:d ex:v "7"^^xsd:long .
        ex:e ex:v "2.50"^^xsd:float .
        ex:f ex:v "2.5"^^xsd:float .
        ex:g ex:v "1.0E1"^^xsd:double .
        ex:h ex:v "10.0"^^xsd:double .
        ex:z ex:v "2"^^xsd:int .
        """;

    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("canon.ttl").toFile();
        File h5 = dir.resolve("canon.ttl.h5").toFile();
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

    private static Set<String> subjects(String objectTerm) {
        return subjects(ds, objectTerm);
    }

    private static Set<String> subjects(Dataset d, String objectTerm) {
        Set<String> uris = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(d)
                .query(QueryFactory.create(PREFIX + "SELECT ?s WHERE { ?s ex:v " + objectTerm + " }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get("s");
                if (n != null && n.isURIResource()) uris.add(n.asResource().getURI());
            }
        }
        return uris;
    }

    @Test
    void lexicalVariantsCollapseAndAllTriplesStayFindable() {
        assertEquals(Set.of("http://ex.org/a", "http://ex.org/b"), subjects("\"1\"^^xsd:int"));
        assertEquals(Set.of("http://ex.org/c", "http://ex.org/d"), subjects("\"7\"^^xsd:long"));
        assertEquals(Set.of("http://ex.org/e", "http://ex.org/f"), subjects("\"2.5\"^^xsd:float"));
        assertEquals(Set.of("http://ex.org/g", "http://ex.org/h"), subjects("\"10.0\"^^xsd:double"));
        // Neighbouring entries are still findable (ordering is intact).
        assertEquals(Set.of("http://ex.org/z"), subjects("\"2\"^^xsd:int"));
    }

    @Test
    void nonCanonicalQueryConstantMatchesNothing() {
        // Documented policy: the store contains the canonical term only; "01"^^xsd:int
        // is a different RDF term and (correctly, under term semantics) matches nothing.
        assertEquals(Set.of(), subjects("\"01\"^^xsd:int"));
    }

    @Test
    void extractionReturnsCanonicalForm() {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?v WHERE { ex:a ex:v ?v }")).build()) {
            ResultSet rs = qe.execSelect();
            Literal v = rs.next().getLiteral("v");
            assertEquals("1", v.getLexicalForm());
        }
    }

    @Test
    void distinctValuesCollapseToCanonicalTerms() {
        Set<String> values = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT DISTINCT ?v WHERE { ?s ex:v ?v }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) values.add(rs.next().getLiteral("v").getLexicalForm());
        }
        assertEquals(Set.of("1", "7", "2.5", "10.0", "2"), values);
    }

    static java.util.stream.Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    /** BG-182: the canonicalization policy holds on every engine, not only method 0. */
    @org.junit.jupiter.params.ParameterizedTest(name = "{0}")
    @org.junit.jupiter.params.provider.MethodSource("engines")
    void everyEngineCanonicalizesByValue(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        File src = dir.resolve(engine.name() + ".ttl").toFile();
        File h5 = dir.resolve(engine.name() + ".h5").toFile();
        Files.write(src.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        engine.buildStore(src, h5);
        try (BeakGraph b = new BeakGraph(new HDF5Reader(h5))) {
            Dataset d = b.getDataset();
            assertEquals(Set.of("http://ex.org/a", "http://ex.org/b"), subjects(d, "\"1\"^^xsd:int"));
            assertEquals(Set.of("http://ex.org/c", "http://ex.org/d"), subjects(d, "\"7\"^^xsd:long"));
            assertEquals(Set.of("http://ex.org/e", "http://ex.org/f"), subjects(d, "\"2.5\"^^xsd:float"));
            assertEquals(Set.of("http://ex.org/g", "http://ex.org/h"), subjects(d, "\"10.0\"^^xsd:double"));
            assertEquals(Set.of(), subjects(d, "\"01\"^^xsd:int"));
            Set<String> values = new HashSet<>();
            try (QueryExecution qe = QueryExecution.dataset(d)
                    .query(QueryFactory.create(PREFIX + "SELECT DISTINCT ?v WHERE { ?s ex:v ?v }")).build()) {
                ResultSet rs = qe.execSelect();
                while (rs.hasNext()) values.add(rs.next().getLiteral("v").getLexicalForm());
            }
            assertEquals(Set.of("1", "7", "2.5", "10.0", "2"), values, "one canonical term per value");
        }
    }
}
