package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Concurrent queries against a single shared reader must not corrupt each other.
 * Exercises the front-coded dictionary ({@code FCDReader}) - including its compressed
 * (long) and uncompressed (short) string paths - and the index cache from many threads.
 */
class ConcurrentReadTest {

    private static final int N = 150;

    @TempDir
    static Path dir;
    static File h5;

    @BeforeAll
    static void build() throws Exception {
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n");
        for (int i = 0; i < N; i++) {
            ttl.append("ex:s").append(i).append(" ex:link ex:target").append(i).append(" .\n");
            ttl.append("ex:s").append(i).append(" ex:short \"v").append(i).append("\" .\n");
            // > 64 bytes -> stored compressed, so the zstd decompressor is exercised too.
            ttl.append("ex:s").append(i).append(" ex:long \"long-").append(i).append('-')
               .append("x".repeat(80)).append("\" .\n");
        }
        File t = dir.resolve("conc.ttl").toFile();
        h5 = dir.resolve("conc.ttl.h5").toFile();
        Files.write(t.toPath(), ttl.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(t).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
    }

    private static void runConcurrently(int threads, java.util.function.IntConsumer body) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    body.accept(id);
                } catch (Throwable th) {
                    failure.compareAndSet(null, th);
                }
            }));
        }
        for (Future<?> f : futures) f.get();
        pool.shutdownNow();
        if (failure.get() != null) {
            throw new AssertionError("concurrent reads corrupted each other", failure.get());
        }
    }

    @Test
    void concurrentDictionaryExtractIsThreadSafe() throws Exception {
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            Dictionary objects = reader.getDictionary().getObjects();
            int n = (int) objects.getNumberOfNodes();
            String[] golden = new String[n + 1];
            for (int id = 1; id <= n; id++) golden[id] = objects.extract(id).toString();

            runConcurrently(8, seed -> {
                Random rnd = new Random(seed);
                for (int it = 0; it < 4000; it++) {
                    int id = 1 + rnd.nextInt(n);
                    String got = objects.extract(id).toString();
                    if (!golden[id].equals(got)) {
                        throw new AssertionError("id " + id + " expected <" + golden[id] + "> but got <" + got + ">");
                    }
                }
            });
        }
    }

    @Test
    void concurrentSparqlQueriesAreThreadSafe() throws Exception {
        Dataset ds = new BeakGraph(new HDF5Reader(h5)).getDataset();
        try {
            runConcurrently(8, seed -> {
                Random rnd = new Random(seed);
                for (int it = 0; it < 600; it++) {
                    int i = rnd.nextInt(N);
                    String q = "PREFIX ex: <http://ex.org/> SELECT ?o WHERE { ex:s" + i + " ex:long ?o }";
                    try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
                        ResultSet rs = qe.execSelect();
                        String got = rs.hasNext() ? rs.next().getLiteral("o").getLexicalForm() : null;
                        String want = "long-" + i + "-" + "x".repeat(80);
                        if (!want.equals(got)) {
                            throw new AssertionError("ex:s" + i + " ex:long expected <" + want + "> but got <" + got + ">");
                        }
                    }
                }
            });
        } finally {
            ds.close();
        }
    }
}
