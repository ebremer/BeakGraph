package com.ebremer.beakgraph.io;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end proof that the whole reader stack works over FFM-mapped segments:
 * the FFM threshold is forced to 0 so EVERY dataset - bit-packed ids and
 * bitmaps, rank/select directories, FCD string buffers (compressed and raw
 * fragments), float/double value stores, language tags - is read through
 * {@link MemorySegmentBytes} instead of jHDF's ByteBuffer, and every query
 * answer must match the default (ByteBuffer) path over the same file exactly
 * (same file, same ids, so even blank node labels must agree).
 */
class FfmReaderPathTest {

    // NEVER + manual delete: auto-arena mappings unmap at GC, which on Windows
    // can outlive JUnit's cleanup and fail the run over a locked temp file.
    @TempDir(cleanup = CleanupMode.NEVER)
    static Path dir;

    @AfterAll
    static void tryCleanup() {
        try (var files = Files.walk(dir)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                p.toFile().deleteOnExit();
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        } catch (Exception ignored) {}
    }

    private static final String[] PROBES = {
        "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
        "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
        "SELECT ?s ?o WHERE { GRAPH <http://ex.org/g1> { ?s ?p ?o } }",
        "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/f> ?o }",
        "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/d> ?o }",
        "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/name> ?o }",
        "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }",
        "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
    };

    @Test
    void allDatasetsReadIdenticallyThroughFfmMappings() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:s0 ex:p0 ex:o0 .
            ex:s0 ex:name "Alice" .
            ex:s0 ex:name "Alice"@en .
            ex:s0 ex:count "42"^^xsd:int .
            ex:s0 ex:big "9007199254740993"^^xsd:long .
            ex:s0 ex:f "1.5"^^xsd:float .
            ex:s0 ex:d "-2.25E8"^^xsd:double .
            ex:s0 ex:when "2024-05-06T07:08:09Z"^^xsd:dateTime .
            ex:s0 ex:longstr "%s" .
            _:b1 ex:p0 _:b2 .
            ex:g1 { ex:s1 ex:p0 ex:o0 . _:b1 ex:inGraph ex:g1 . }
            """.formatted("z".repeat(200));
        File src = dir.resolve("ffm.trig").toFile();
        File h5 = dir.resolve("ffm.trig.h5").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        // Baseline: today's ByteBuffer path.
        Set<String>[] expected = runProbes(h5);

        // Forced-FFM: every dataset larger than 0 bytes goes through MemorySegmentBytes.
        long previous = DatasetBytes.setFfmThreshold(0);
        try {
            Set<String>[] actual = runProbes(h5);
            for (int i = 0; i < PROBES.length; i++) {
                assertFalse(expected[i].isEmpty(), "probe must return rows: " + PROBES[i]);
                assertEquals(expected[i], actual[i],
                        "FFM path must answer identically: " + PROBES[i]);
            }
        } finally {
            DatasetBytes.setFfmThreshold(previous);
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String>[] runProbes(File h5) throws Exception {
        Set<String>[] results = new Set[PROBES.length];
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            for (int i = 0; i < PROBES.length; i++) {
                Set<String> rows = new HashSet<>();
                try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                        .query(QueryFactory.create(PROBES[i])).build()) {
                    ResultSet rs = qe.execSelect();
                    while (rs.hasNext()) {
                        QuerySolution qs = rs.next();
                        StringBuilder sb = new StringBuilder();
                        rs.getResultVars().forEach(v -> sb.append(v).append('=').append(qs.get(v)).append('|'));
                        rows.add(sb.toString());
                    }
                }
                results[i] = rows;
            }
        }
        return results;
    }
}
