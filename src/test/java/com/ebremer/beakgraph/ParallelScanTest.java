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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    static String oldMinChunk;

    @BeforeAll
    static void build() throws Exception {
        oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        // BG-217: several chunks per scan, so two-scan shapes (MINUS, OPTIONAL,
        // UNION) and concurrent queries really share and contend for workers.
        oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
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
        if (oldMinChunk == null) {
            System.clearProperty("beakgraph.scan.parallel.minchunk");
        } else {
            System.setProperty("beakgraph.scan.parallel.minchunk", oldMinChunk);
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

    /** All rows as a sorted multiset (shapes below legitimately repeat rows). */
    private static List<String> rowsList(Dataset dataset, String queryBody) {
        List<String> out = new java.util.ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(dataset)
                .query(QueryFactory.create(PREFIX + queryBody)).timeout(30, java.util.concurrent.TimeUnit.SECONDS).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = rs.getResultVars();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : vars) sb.append(v).append('=').append(row.get(v)).append('|');
                out.add(sb.toString());
            }
        }
        java.util.Collections.sort(out);
        return out;
    }

    private static void checkList(String queryBody, int minParallelScans) {
        long before = ParallelScan.HITS.get();
        List<String> got = rowsList(ds, queryBody);
        assertEquals(rowsList(truth, queryBody), got, "results for: " + queryBody);
        assertTrue(ParallelScan.HITS.get() - before >= minParallelScans, "expected " + minParallelScans + " parallel scans for: " + queryBody);
        assertFalse(got.isEmpty(), "not vacuous: " + queryBody);
    }

    // --- BG-217: two eligible scans in one query, and many queries at once ---

    @Test
    @org.junit.jupiter.api.Timeout(60)
    void minusOverTwoScansMatchesSequential() throws Exception {
        // LEFT built first and consumed last: the shape that starved a fixed pool.
        checkList("SELECT ?s WHERE { ?s ex:name ?n MINUS { ?s ex:value ?v FILTER(?v < 250) } }", 2);
        checkList("SELECT ?s WHERE { ?s ?p ?o MINUS { ?s ex:link ?t FILTER(STRSTARTS(STR(?t), \"http://ex.org/s1\")) } }", 2);
        awaitWorkersDone();
    }

    @Test
    @org.junit.jupiter.api.Timeout(60)
    void optionalOverTwoScansMatchesSequential() throws Exception {
        checkList("SELECT * WHERE { ?s ex:value ?v OPTIONAL { ?s ex:name ?n } }", 1);
        checkList("SELECT * WHERE { ?s ex:value ?v OPTIONAL { ?s ex:link ?t FILTER(?v > 400) } }", 1);
        awaitWorkersDone();
    }

    @Test
    @org.junit.jupiter.api.Timeout(60)
    void unionAndNotExistsOverScansMatchSequential() throws Exception {
        checkList("SELECT ?s WHERE { { ?s ex:value ?v } UNION { ?s ex:name ?n } }", 2);
        checkList("SELECT ?s WHERE { ?s ex:value ?v FILTER NOT EXISTS { ?s ex:refl ?s } }", 1);
        awaitWorkersDone();
    }

    @Test
    @org.junit.jupiter.api.Timeout(120)
    void concurrentFullScansAllCompleteAndMatchTruth() throws Exception {
        int threads = 2 * Runtime.getRuntime().availableProcessors();
        Set<String> expected = rows(truth, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        try {
            List<java.util.concurrent.Future<Set<String>>> futures = new java.util.ArrayList<>();
            long before = ParallelScan.HITS.get();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> rows(ds, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")));
            }
            for (java.util.concurrent.Future<Set<String>> f : futures) {
                assertEquals(expected, f.get(90, java.util.concurrent.TimeUnit.SECONDS));
            }
            assertTrue(ParallelScan.HITS.get() - before >= threads, "every query ran as a parallel scan");
        } finally {
            pool.shutdownNow();
        }
        awaitWorkersDone();
    }

    @Test
    void subPatternsReExecutedPerOuterRowAreNotReplannedInParallel() throws Exception {
        // BG-337: EXISTS re-executes its (uncorrelated, scan-shaped) pattern once
        // per outer row with a singleton input. One binding used to be enough
        // for a parallel scan, so every outer row built a worker set, a queue
        // and a Cleaner registration. Only the root-level outer scan may plan
        // one; the inner scans stay sequential.
        long before = ParallelScan.HITS.get();
        Set<String> got = rows(ds, "SELECT ?s WHERE { ?s ex:link ?t FILTER EXISTS { ?x ex:value ?v } }");
        assertEquals(SUBJECTS, got.size());
        assertEquals(1, ParallelScan.HITS.get() - before,
                "exactly the outer scan parallelizes; the EXISTS sub-pattern (one execution per outer row) must not");
        awaitWorkersDone();
    }

    @Test
    void rootLikeInputsStillParallelize() throws Exception {
        // A top-level GRAPH <g> and UNION branch feed the pattern one EMPTY
        // binding (a copy of the root) - those are fresh top-level scans, not
        // per-row re-executions, and keep their parallel plan.
        long before = ParallelScan.HITS.get();
        Set<String> got = rows(ds, "SELECT ?s ?v WHERE { GRAPH <" + Quad.defaultGraphIRI.getURI() + "> { ?s ex:value ?v } }");
        assertEquals(SUBJECTS, got.size());
        assertEquals(1, ParallelScan.HITS.get() - before, "GRAPH <default> at the root is a top-level scan");
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
