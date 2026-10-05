package com.ebremer.beakgraph;

import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that {@code rdf:langString} literals survive the Turtle -> HDF5 ->
 * read round-trip with their language tags intact, and that matching follows
 * RDF term semantics: a plain {@code xsd:string} and a language-tagged literal
 * with the same lexical form are distinct terms.
 */
class LanguageTagRoundTripTest {

    private static final String TTL = """
        @prefix sdo: <https://schema.org/> .
        @prefix ex:  <http://ex.org/> .
        ex:a sdo:name "hello"@en .
        ex:b sdo:name "hello"@fr .
        ex:c sdo:name "hello" .
        ex:d sdo:name "bonjour"@fr .
        ex:e sdo:name "plain" .
        """;

    private static final String PREFIX = "PREFIX sdo: <https://schema.org/> PREFIX ex: <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("langs.ttl").toFile();
        File h5 = dir.resolve("langs.ttl.h5").toFile();
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
        // Release the file: a leaked reader makes @TempDir cleanup flaky on Windows (BG-176).
        if (bg != null) bg.close();
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

    @Test
    void languageTaggedTermMatchesExactly() {
        // "hello"@en must match only ex:a, not the @fr or plain "hello".
        assertEquals(1, count("?s sdo:name \"hello\"@en"));
        assertEquals(1, count("?s sdo:name \"hello\"@fr"));
    }

    @Test
    void plainStringDoesNotMatchLanguageTagged() {
        // RDF term semantics: plain "hello" (xsd:string) is a different term
        // from "hello"@en / "hello"@fr, so it matches only ex:c.
        assertEquals(1, count("?s sdo:name \"hello\""));
    }

    @Test
    void langFunctionReconstructsTags() {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX
                        + "SELECT ?lang WHERE { ex:a sdo:name ?o BIND(LANG(?o) AS ?lang) }")).build()) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext());
            assertEquals("en", rs.next().getLiteral("lang").getString());
        }
    }

    @Test
    void filterByLanguageTag() {
        // Two @fr literals (ex:b, ex:d); two with no tag (ex:c, ex:e).
        assertEquals(2, count("?s sdo:name ?o FILTER(LANG(?o) = \"fr\")"));
        assertEquals(2, count("?s sdo:name ?o FILTER(LANG(?o) = \"\")"));
    }

    @Test
    void allDistinctTagsSurvive() {
        Set<String> tags = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX
                        + "SELECT ?o WHERE { ?s sdo:name ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                tags.add(qs.getLiteral("o").getLanguage()); // "" when no tag
            }
        }
        assertTrue(tags.contains("en"));
        assertTrue(tags.contains("fr"));
        assertTrue(tags.contains(""));
    }
}
