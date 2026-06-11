package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that a concrete subject in a "{@code <s> ?p ?o}" pattern restricts
 * the scan to that subject - exercising the binary-search seek into the GSPO
 * subject list at the first, middle, last and absent positions.
 */
class BGIteratorConcreteSubjectTest {

    private static final String TTL = """
        @prefix sdo: <https://schema.org/> .
        <http://ex.org/s/a> a sdo:Thing ; sdo:name "a" .
        <http://ex.org/s/b> a sdo:Thing .
        <http://ex.org/s/c> a sdo:Thing ; sdo:name "c" ; sdo:url "u" .
        <http://ex.org/s/d> a sdo:Thing .
        <http://ex.org/s/e> a sdo:Thing .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("subjects.ttl").toFile();
        File h5 = dir.resolve("subjects.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @org.junit.jupiter.api.AfterAll
    static void closeReader() {
        // Release the mapped file: a leaked reader makes @TempDir cleanup flaky on Windows.
        if (bg != null) bg.close();
    }

    private static int count(String iri) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("SELECT * WHERE { <" + iri + "> ?p ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) {
                rs.next();
                n++;
            }
            return n;
        }
    }

    @Test
    void firstSubject() {
        assertEquals(2, count("http://ex.org/s/a"));
    }

    @Test
    void middleSubject() {
        assertEquals(3, count("http://ex.org/s/c"));
    }

    @Test
    void lastSubject() {
        assertEquals(1, count("http://ex.org/s/e"));
    }

    @Test
    void entityThatIsNeverASubject() {
        // sdo:Thing is in the dictionary but only ever an object - the binary
        // search must return "not found", yielding zero rows.
        assertEquals(0, count("https://schema.org/Thing"));
    }

    @Test
    void subjectNotInDictionary() {
        assertEquals(0, count("http://ex.org/s/missing"));
    }

    @Test
    void fullScanUnaffected() {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("SELECT * WHERE { ?s ?p ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) {
                rs.next();
                n++;
            }
            assertEquals(8, n);
        }
    }
}
