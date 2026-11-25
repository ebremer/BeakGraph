package com.ebremer.beakgraph.rdfarray;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import com.ebremer.falcon.hdf5.Dataset;
import com.ebremer.falcon.hdf5.Group;
import com.ebremer.falcon.hdf5.Hdf5File;
import com.ebremer.falcon.hdf5.Hdf5Object;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Step 9's gate (RDFARRY.md): whether, for array-heavy data, the dictionary rows of the leaves and the
 * canonical elements dominate the store's size or its build time. Over a SYNTHETIC store of embeddings
 * (2,000 x 768 doubles, a type and a label each), it builds the canonical v7 store, the same with
 * {@code -arrays}, and a control whose embeddings are one-element sequences, and prints each one's size,
 * build time and datasets by size. Opt-in: {@code -Dbeakgraph.bench=true}; the scale with
 * {@code -Dbeakgraph.bench.scale}. Never run it on real data.
 */
class StoreCompositionBenchmarkTest {

    @TempDir
    Path dir;

    /** Values with five decimals, which repeat (about 420,000 distinct in 1.5 million), or at full precision, which do not. */
    private static void data(Path file, int n, boolean embeddings, boolean fullPrecision) throws Exception {
        Random r = new Random(7);
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            w.write("PREFIX : <http://ex.org/>\n");
            for (int i = 0; i < n; i++) {
                w.write(":e" + i + " a :Embedding ; :label \"chunk " + i + "\" ; :emb ( ");
                if (embeddings) {
                    StringBuilder b = new StringBuilder(768 * 12);
                    for (int k = 0; k < 768; k++) {
                        b.append(fullPrecision ? String.format(Locale.ROOT, "%.16E ", r.nextGaussian())
                                : String.format(Locale.ROOT, "%.5fE0 ", r.nextGaussian()));
                    }
                    w.write(b.toString());
                } else {
                    w.write(i + " ");
                }
                w.write(") .\n");
            }
        }
    }

    @Test
    void whatAnArrayHeavyStoreHolds() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("beakgraph.bench"), "opt-in: -Dbeakgraph.bench=true");
        int n = 2000 * Integer.getInteger("beakgraph.bench.scale", 1);
        Path control = dir.resolve("control.ttl");
        data(control, n, false, false);
        build("control, ( i ) for each", control, null);
        for (boolean full : new boolean[] {false, true}) {
            Path emb = dir.resolve("emb-" + full + ".ttl");
            data(emb, n, true, full);
            String values = full ? "full precision" : "5 decimals";
            parse(values + ": parse only", emb);
            build(values + ": canonical v7", emb, null);
            build(values + ": v7 with -arrays", emb, new SequenceArrays.Options(16, 2, false));
            build(values + ": v8, -arrays-only", emb, new SequenceArrays.Options(16, 2, false, SequenceArrays.TimeUnit.US, true));
        }
    }

    /** The parse alone, to a counting sink: what any format pays. */
    private static void parse(String name, Path src) {
        for (int run = 0; run < 2; run++) {
            long t0 = System.nanoTime();
            long[] n = {0};
            org.apache.jena.riot.RDFParser.source(src).lang(org.apache.jena.riot.Lang.TURTLE)
                    .parse(new org.apache.jena.riot.system.StreamRDFBase() {
                        @Override public void triple(org.apache.jena.graph.Triple t) { n[0]++; }
                    });
            if (run == 1) {
                System.out.printf(Locale.ROOT, "GATE %-40s %8d triples %6.1f s%n", name, n[0], (System.nanoTime() - t0) / 1e9);
            }
        }
    }

    private void build(String name, Path src, SequenceArrays.Options arrays) throws Exception {
        File out = dir.resolve(name.replaceAll("[^a-z0-9]+", "_") + ".h5").toFile();
        // Twice: the first run warms the JVM, the second is timed.
        for (int run = 0; run < 2; run++) {
            Files.deleteIfExists(out.toPath());
            long t0 = System.nanoTime();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).setArrays(arrays).build().write();
            if (run == 1) {
                System.out.printf(Locale.ROOT, "GATE %-40s %8.1f MB %6.1f s%n", name, out.length() / 1e6,
                        (System.nanoTime() - t0) / 1e9);
            }
        }
        Map<String, Long> sizes = new TreeMap<>();
        try (Hdf5File f = Hdf5File.open(out.toPath())) {
            walk(f.root(), "", sizes);
        }
        long total = sizes.values().stream().mapToLong(Long::longValue).sum();
        Map<String, Long> byGroup = new TreeMap<>();
        sizes.forEach((path, size) -> {
            String key = path.startsWith("/.BG/arrays/") ? "/.BG/arrays"
                    : path.contains("/literals/") ? path.replace("_blocks", "")
                    : path.substring(0, Math.max(1, path.lastIndexOf('/')));
            byGroup.merge(key, size, Long::sum);
        });
        System.out.printf(Locale.ROOT, "GATE   datasets %.1f MB of %.1f MB%n", total / 1e6, out.length() / 1e6);
        byGroup.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(8)
                .forEach(e -> System.out.printf(Locale.ROOT, "GATE   %-48s %8.2f MB %5.1f%%%n", e.getKey(),
                        e.getValue() / 1e6, 100.0 * e.getValue() / out.length()));
    }

    private static void walk(Group g, String path, Map<String, Long> sizes) {
        for (Hdf5Object child : g.children()) {
            String p = path + "/" + child.name();
            if (child instanceof Group sub) {
                walk(sub, p, sizes);
            } else if (child instanceof Dataset d) {
                sizes.put(p, d.storageSize());
            }
        }
    }
}
