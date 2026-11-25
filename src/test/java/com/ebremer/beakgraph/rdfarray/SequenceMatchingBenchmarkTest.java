package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.PatternMatchBG;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Sequence patterns matched on ids against the same patterns handed to
 * jena-array's solver (RDFARRY.md step 7): wall time per query, in process,
 * over a SYNTHETIC store only - FHIR-style component tuples, rosters with
 * empty slots, 2x2 matrices, polygons as rows of points, and embeddings of
 * 768 doubles. Opt-in: {@code -Dbeakgraph.bench=true}; the scale with
 * {@code -Dbeakgraph.bench.scale} (1 by default). It also times Jena's
 * {@code SEQLEN}, {@code isSEQ} and {@code UNNEST} over the stored
 * sequences, one route, which is what said they need no id path of their
 * own (RDFARRY.md step 7). And it times containment patterns, whose element
 * the pattern does not place, by the scan and through the element index
 * ({@code -seqindex}, RDFARRY.md step 8), over a second store built with it.
 * Never run it on real data.
 */
class SequenceMatchingBenchmarkTest {

    @TempDir
    Path dir;

    private static final String[] CODES = {"8480-6", "8462-4", "8867-4", "9279-1", "2708-6", "8310-5", "29463-7",
        "8302-2", "39156-5", "59408-5", "8287-5", "3141-9", "2339-0", "2093-3", "2571-8", "2085-9", "13457-7",
        "4548-4", "33914-3", "2160-0"};

    private static void data(Path file, int scale) throws Exception {
        Random r = new Random(7);
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            w.write("PREFIX : <http://ex.org/>\n");
            for (int i = 0; i < 50_000 * scale; i++) {
                w.write(String.format(Locale.ROOT, ":pt%d :xy ( %.6fE0 %.6fE0 ) .%n", i, r.nextDouble(), r.nextDouble()));
            }
            for (int i = 0; i < 50_000 * scale; i++) {
                w.write(String.format(Locale.ROOT, ":obs%d :component ( \"%s\" %d \"mm[Hg]\" ) .%n",
                        i, CODES[r.nextInt(CODES.length)], 60 + r.nextInt(120)));
            }
            for (int i = 0; i < 20_000 * scale; i++) {
                StringBuilder b = new StringBuilder();
                for (int k = 0; k < 10; k++) {
                    b.append(r.nextInt(5) == 0 ? "undef " : ":player" + r.nextInt(5000) + " ");
                }
                w.write(":team" + i + " :roster ( " + b + ") .\n");
            }
            for (int i = 0; i < 20_000 * scale; i++) {
                w.write(String.format(Locale.ROOT, ":m%d :matrix ( ( %d %d ) ( %d %d ) ) .%n",
                        i, r.nextInt(100), r.nextInt(100), r.nextInt(100), r.nextInt(100)));
            }
            for (int i = 0; i < 10_000 * scale; i++) {
                StringBuilder b = new StringBuilder();
                for (int k = 0; k < 8; k++) {
                    b.append(String.format(Locale.ROOT, "( %.4fE0 %.4fE0 ) ", r.nextDouble(), r.nextDouble()));
                }
                w.write(":poly" + i + " :ring ( " + b + ") .\n");
            }
            // Squads: rosters without empty seats, so that a bound player names few of them, and the
            // clubs' captains to join through.
            for (int i = 0; i < 20_000 * scale; i++) {
                StringBuilder b = new StringBuilder();
                for (int k = 0; k < 10; k++) {
                    b.append(":player").append(r.nextInt(5000)).append(' ');
                }
                w.write(":squad" + i + " :members ( " + b + ") .\n");
            }
            for (int i = 0; i < 200; i++) {
                w.write(":club" + i + " :captain :player" + r.nextInt(5000) + " .\n");
            }
            for (int i = 0; i < 2_000 * scale; i++) {
                StringBuilder b = new StringBuilder(768 * 12);
                for (int k = 0; k < 768; k++) {
                    b.append(String.format(Locale.ROOT, "%.5fE0 ", r.nextGaussian()));
                }
                w.write(":e" + i + " :emb ( " + b + ") .\n");
            }
        }
    }

    private static final String[][] QUERIES = {
        {"points, both coordinates", "SELECT ?s ?x ?y { ?s :xy ( ?x ?y ) }"},
        {"FHIR, one code (prefix)", "SELECT ?s ?v ?u { ?s :component ( \"8480-6\" ?v ?u ) }"},
        {"FHIR, code and value", "SELECT ?s ?c ?v { ?s :component ( ?c ?v \"mm[Hg]\" ) }"},
        {"rosters, every member (gaps)", "SELECT ?t ?m { ?t :roster ( ... ?m ... ) }"},
        {"rosters, one player (gaps, constant)", "SELECT ?t { ?t :roster ( ... :player42 ... ) }"},
        {"matrices, nested", "SELECT ?s ?a ?d { ?s :matrix ( ( ?a ?b ) ( ?c ?d ) ) }"},
        {"polygons, first point", "SELECT ?p ?x ?y { ?p :ring ( ( ?x ?y ) ... ) }"},
        {"embeddings, first value", "SELECT ?e ?f { ?e :emb ( ?f ... ) }"},
        {"embeddings, last value", "SELECT ?e ?l { ?e :emb ( ... ?l ) }"},
        {"embeddings, every value", "SELECT (COUNT(*) AS ?n) { ?e :emb ( ... ?v ... ) }"},
    };

    /**
     * Containment: an element the pattern does not place, a constant or a variable bound by a join. Timed
     * on ids by the scan, and through the element index over the store built with it.
     */
    private static final String[][] CONTAINMENT = {
        {"rosters, one player", "SELECT ?t { ?t :roster ( ... :player42 ... ) }"},
        {"rosters, two players", "SELECT ?t { ?t :roster ( ... :player42 ... :player43 ... ) }"},
        {"rosters, a player and an empty seat", "SELECT ?t { ?t :roster ( ... :player42 ... undef ... ) }"},
        {"FHIR, one value after the code", "SELECT ?s ?c { ?s :component ( ?c 120 ?u ) }"},
        {"squads, a captain's (join)", "SELECT ?c ?t { ?c :captain ?m . ?t :members ( ... ?m ... ) }"},
        {"rosters, a captain's (join; slots pass)", "SELECT ?c ?t { ?c :captain ?m . ?t :roster ( ... ?m ... ) }"},
    };

    /** Not sequence patterns: Jena's functions and UNNEST over stored sequences, one route, for their cost. */
    private static final String[][] FUNCTIONS = {
        {"SEQLEN of every embedding", "SELECT (SUM(SEQLEN(?q)) AS ?n) { ?e :emb ?q }"},
        {"isSEQ of every embedding", "SELECT (COUNT(*) AS ?n) { ?e :emb ?q FILTER(isSEQ(?q)) }"},
        {"UNNEST every embedding", "SELECT (COUNT(*) AS ?n) { ?e :emb ?q UNNEST(?q AS ?v) }"},
        {"UNNEST every roster", "SELECT ?t ?m { ?t :roster ?q UNNEST(?q AS ?m) }"},
    };

    @Test
    @org.junit.jupiter.api.Timeout(value = 60, unit = java.util.concurrent.TimeUnit.MINUTES)  // jena-array's gaps are quadratic
    void matchingOnIdsAgainstJenaArray() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("beakgraph.bench"), "opt-in: -Dbeakgraph.bench=true");
        int scale = Integer.getInteger("beakgraph.bench.scale", 1);
        Path src = dir.resolve("bench.ttl");
        data(src, scale);
        File store = dir.resolve("bench.h5").toFile();
        long t0 = System.nanoTime();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(store).build().write();
        System.out.printf(Locale.ROOT, "BENCH build %.1f s, %.1f MB%n", (System.nanoTime() - t0) / 1e9, store.length() / 1e6);
        try (BeakGraph bg = BG.getBeakGraph(store)) {
            System.out.printf(Locale.ROOT, "BENCH %-40s %10s %12s %12s %8s%n", "query", "rows", "ids ms", "jena ms", "x");
            for (String[] q : QUERIES) {
                Query query = QueryFactory.create("PREFIX : <http://ex.org/>\n" + q[1], Syntax.syntaxARQ);
                long[] ids = time(bg, query, false);
                long[] jena = time(bg, query, true);
                assertEquals(jena[0], ids[0], q[0]);
                System.out.printf(Locale.ROOT, "BENCH %-40s %10d %12.1f %12.1f %8.1f%n", q[0], ids[0], ids[1] / 1e6,
                        jena[1] / 1e6, (double) jena[1] / ids[1]);
            }
            for (String[] q : FUNCTIONS) {
                Query query = QueryFactory.create("PREFIX : <http://ex.org/>\n" + q[1], Syntax.syntaxARQ);
                long[] t = time(bg, query, false);
                System.out.printf(Locale.ROOT, "BENCH %-40s %10d %12.1f%n", q[0], t[0], t[1] / 1e6);
            }
        }
        File indexed = dir.resolve("bench-seqindex.h5").toFile();
        t0 = System.nanoTime();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(indexed).setSequenceIndex(true).build().write();
        System.out.printf(Locale.ROOT, "BENCH build with -seqindex %.1f s, %.1f MB (the index: %.1f MB)%n",
                (System.nanoTime() - t0) / 1e9, indexed.length() / 1e6, (indexed.length() - store.length()) / 1e6);
        try (BeakGraph bg = BG.getBeakGraph(store); BeakGraph ix = BG.getBeakGraph(indexed)) {
            System.out.printf(Locale.ROOT, "BENCH %-40s %10s %12s %12s %8s%n", "containment", "rows", "scan ms", "index ms", "x");
            for (String[] q : CONTAINMENT) {
                Query query = QueryFactory.create("PREFIX : <http://ex.org/>\n" + q[1], Syntax.syntaxARQ);
                long[] scan = time(bg, query, false);
                long[] index = time(ix, query, false);
                assertEquals(scan[0], index[0], q[0]);
                System.out.printf(Locale.ROOT, "BENCH %-40s %10d %12.1f %12.1f %8.1f%n", q[0], scan[0], scan[1] / 1e6,
                        index[1] / 1e6, (double) scan[1] / index[1]);
            }
        }
    }

    /** {rows, the median of five timed runs in ns}, after one run to warm up. */
    private static long[] time(BeakGraph bg, Query query, boolean delegate) {
        String before = System.getProperty(PatternMatchBG.DELEGATE_PROPERTY);
        System.setProperty(PatternMatchBG.DELEGATE_PROPERTY, Boolean.toString(delegate));
        try {
            long rows = run(bg, query);
            List<Long> times = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                long t = System.nanoTime();
                long n = run(bg, query);
                times.add(System.nanoTime() - t);
                assertEquals(rows, n);
            }
            long[] sorted = times.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(sorted);
            return new long[]{rows, sorted[sorted.length / 2]};
        } finally {
            if (before == null) {
                System.clearProperty(PatternMatchBG.DELEGATE_PROPERTY);
            } else {
                System.setProperty(PatternMatchBG.DELEGATE_PROPERTY, before);
            }
        }
    }

    /** The rows, each read whole: every variable's term. */
    private static long run(BeakGraph bg, Query query) {
        long n = 0;
        try (QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(query).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = rs.getResultVars();
            while (rs.hasNext()) {
                var row = rs.next();
                for (String v : vars) {
                    var node = row.get(v);
                    if (node != null && node.isLiteral()) {
                        node.asLiteral().getLexicalForm();
                    }
                }
                n++;
                if (vars.equals(List.of("n"))) {
                    n += row.getLiteral("n").getLong();     // an aggregate: its value is the check
                }
            }
        }
        return n;
    }
}
