package com.ebremer.beakgraph.benchmarks;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Deterministic synthetic BeakGraph store shared by the benchmarks.
 *
 * <p>The store is built once per {@code subjects} size into
 * {@code target/jmh-data/store-&lt;subjects&gt;.h5} (override the directory with
 * {@code -Dbeakgraph.bench.data=...}) and reused by later runs and forks: the
 * generator is fully deterministic, so an existing file is always equivalent to
 * a fresh build. Delete the directory to force a rebuild.
 *
 * <p>Data shape, chosen to exercise each read-path index:
 * <ul>
 *   <li>Default graph, per subject {@code s&lt;i&gt;} (4 triples each):
 *     {@code rdf:type ex:Widget} (one low-selectivity predicate),
 *     {@code ex:name "widget-&lt;i&gt;"} (distinct string literals),
 *     {@code ex:value &lt;i mod 1000&gt;} (shared integer literals - range-filter fodder),
 *     {@code ex:link s&lt;(7i+13) mod subjects&gt;} (URI objects forming join chains).</li>
 *   <li>{@code subjects/4} extra quads spread round-robin over {@value #NAMED_GRAPHS}
 *       named graphs {@code ex:g&lt;k&gt;}: {@code s&lt;i&gt; ex:tag &lt;k&gt;} - so
 *       GRAPH-variable and union scans have real multi-graph work to do.</li>
 * </ul>
 */
public final class SyntheticStore {

    public static final String NS = "http://bench.beakgraph.org/";
    public static final int NAMED_GRAPHS = 8;

    private SyntheticStore() {}

    /** Index of the subject that {@code s<i>}'s ex:link points at. */
    public static int linkTarget(int i, int subjects) {
        return (int) ((7L * i + 13) % subjects);
    }

    /** Path of the store for {@code subjects}, building it first if absent. */
    public static synchronized Path get(int subjects) {
        Path dir = Paths.get(System.getProperty("beakgraph.bench.data", "target/jmh-data"));
        Path h5 = dir.resolve("store-" + subjects + ".h5");
        if (Files.exists(h5)) {
            return h5;
        }
        try {
            Files.createDirectories(dir);
            Path trig = dir.resolve("store-" + subjects + ".trig");
            long t0 = System.nanoTime();
            System.err.printf("[SyntheticStore] generating %,d subjects -> %s%n", subjects, trig);
            generate(trig, subjects);
            System.err.printf("[SyntheticStore] building store -> %s%n", h5);
            HDF5Writer.Builder()
                    .setSource(trig.toFile())
                    .setDestination(h5.toFile())
                    .setSpatial(false)
                    .setFeatures(false)
                    .build()
                    .write();
            System.err.printf("[SyntheticStore] ready in %.1f s (%,d bytes)%n",
                    (System.nanoTime() - t0) / 1e9, Files.size(h5));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build benchmark store " + h5, e);
        }
        return h5;
    }

    private static void generate(Path trig, int subjects) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(trig, StandardCharsets.UTF_8)) {
            w.write("@prefix ex: <" + NS + "> .\n");
            w.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\n");
            for (int i = 0; i < subjects; i++) {
                w.write("ex:s" + i
                        + " a ex:Widget ; ex:name \"widget-" + i + "\" ; ex:value " + (i % 1000)
                        + " ; ex:link ex:s" + linkTarget(i, subjects) + " .\n");
            }
            int tagged = subjects / 4;
            for (int k = 0; k < NAMED_GRAPHS; k++) {
                w.write("\nex:g" + k + " {\n");
                for (int i = k; i < tagged; i += NAMED_GRAPHS) {
                    w.write("  ex:s" + i + " ex:tag " + k + " .\n");
                }
                w.write("}\n");
            }
        }
    }
}
