package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.ParallelScan;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Chunked parallel scans (ScanChunks + ParallelScan) must return exactly the
 * sequential results - rows arrive interleaved across chunks, so comparisons
 * are order-insensitive (index rows are distinct, so set+size equality is a
 * multiset check) - and workers must stop when the consumer stops (LIMIT).
 * The activation threshold is lowered for this class so a small store chunks.
 */
class ParallelScanTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + ">\n";
    private static final int SUBJECTS = 3000;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset truth;
    static String oldThreshold;

    @BeforeAll
    static void build() throws Exception {
        oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        StringBuilder trig = new StringBuilder("@prefix ex: <" + NS + "> .\n");
        for (int i = 0; i < SUBJECTS; i++) {
            trig.append("ex:s").append(i)
                .append(" ex:value ").append(i % 500)
                .append(" ; ex:name \"n").append(i)
                .append("\" ; ex:link ex:s").append((i * 7 + 13) % SUBJECTS)
                .append(" .\n");
        }
        trig.append("ex:s0 ex:refl ex:s0 .\n");
        trig.append("ex:g1 { ex:a ex:p2 ex:b . }\n");
        trig.append("ex:g2 { ex:d ex:p3 \"x\" . }\n");
        File src = dir.resolve("ps.trig").toFile();
        File h5 = dir.resolve("ps.trig.h5").toFile();
        Files.write(src.toPath(), trig.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
        truth = RDFDataMgr.loadDataset(src.getAbsolutePath());
    }

    @AfterAll
    static void close() {
        if (oldThreshold == null) {
            System.clearProperty("beakgraph.scan.parallel.threshold");
        } else {
            System.setProperty("beakgraph.scan.parallel.threshold", oldThreshold);
        }
        if (bg != null) bg.close();
    }

    /** All rows as var=term strings - order-insensitive, distinct by construction. */
    private static Set<String> rows(Dataset dataset, String queryBody) {
        Set<String> out = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(dataset)
                .query(QueryFactory.create(PREFIX + queryBody)).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = rs.getResultVars();
            long n = 0;
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : vars) {
                    sb.append(v).append('=').append(row.get(v)).append('|');
                }
                out.add(sb.toString());
                n++;
            }
            assertEquals(n, out.size(), "scan rows must be distinct");
        }
        return out;
    }

    /** Runs on BeakGraph and the in-memory truth dataset; asserts identical rows and the expected path. */
    private static void check(String queryBody, boolean expectParallel) {
        long before = ParallelScan.HITS.get();
        Set<String> got = rows(ds, queryBody);
        long used = ParallelScan.HITS.get() - before;
        assertEquals(rows(truth, queryBody), got, "results for: " + queryBody);
        if (expectParallel) {
            assertTrue(used >= 1, "expected a parallel scan for: " + queryBody);
        } else {
            assertEquals(0, used, "expected sequential execution for: " + queryBody);
        }
    }

    private static void awaitWorkersDone() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (ParallelScan.ACTIVE_WORKERS.get() != 0) {
            if (System.currentTimeMillis() > deadline) {
                assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "scan workers must stop");
            }
            Thread.sleep(20);
        }
    }

    @Test
    void fullScanMatchesSequential() throws Exception {
        check("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", true);
        awaitWorkersDone();
    }

    @Test
    void predicateScanMatchesSequential() throws Exception {
        check("SELECT ?s ?v WHERE { ?s ex:value ?v }", true);
        awaitWorkersDone();
    }

    @Test
    void rangeFilterMatchesSequential() throws Exception {
        check("SELECT ?s ?v WHERE { ?s ex:value ?v FILTER(?v >= 400) }", true);
        awaitWorkersDone();
    }

    @Test
    void joinWithScanFirstMatchesSequential() throws Exception {
        check("SELECT ?s ?v ?n WHERE { ?s ex:value ?v . ?s ex:name ?n }", true);
        awaitWorkersDone();
    }

    @Test
    void limitClosesScanAndStopsWorkers() throws Exception {
        long before = ParallelScan.HITS.get();
        Set<String> got = rows(ds, "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 3");
        assertEquals(3, got.size());
        assertTrue(ParallelScan.HITS.get() - before >= 1, "LIMIT scan should still parallelize");
        // The close cascade (slice -> ... -> ParallelScan.close) must stop the
        // producers even though the scan was abandoned almost immediately.
        awaitWorkersDone();
    }

    @Test
    void concreteSubjectFallsBack() {
        check("SELECT ?p ?o WHERE { ex:s5 ?p ?o }", false);
    }

    @Test
    void repeatedVariableFallsBack() {
        check("SELECT ?s WHERE { ?s ex:refl ?s }", false);
        check("SELECT ?s ?p WHERE { ?s ?p ?s }", false);
    }

    @Test
    void unionGraphFallsBack() {
        check("SELECT ?s ?p ?o WHERE { GRAPH <" + Quad.unionGraph.getURI() + "> { ?s ?p ?o } }", false);
    }

    @Test
    void belowThresholdStaysSequential() {
        String old = System.setProperty("beakgraph.scan.parallel.threshold", "1000000");
        try {
            check("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", false);
        } finally {
            System.setProperty("beakgraph.scan.parallel.threshold", old);
        }
    }

    @Test
    void disabledStaysSequential() {
        String old = System.setProperty("beakgraph.scan.parallel.threshold", "0");
        try {
            check("SELECT ?s ?v WHERE { ?s ex:value ?v }", false);
        } finally {
            System.setProperty("beakgraph.scan.parallel.threshold", old);
        }
    }
}
