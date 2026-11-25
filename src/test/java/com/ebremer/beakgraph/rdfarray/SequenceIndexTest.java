package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.cmdline.VerifyCommand;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.BGIteratorPOS;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.query.Syntax;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The element index ({@code -seqindex}, RDFARRY.md step 8): which stored sequences hold each element.
 * Every engine writes the same bytes; the index is exactly the transpose of the sequences, slots
 * included; {@code -verify -deep} checks it; and a containment pattern reads only the sequences it names,
 * with the scan's answers.
 */
class SequenceIndexTest {

    @TempDir
    Path dir;

    /** Repeats, slots, nesting, triple terms as elements, empty sequences, and two graphs. */
    static final String DATA = """
        PREFIX : <http://ex.org/>
        :a :p ( 7 7 ) .
        :b :p ( 1 2 3 ) .
        :c :p ( 1 undef 3 ) .
        :d :p ( undef ) .
        :e :p ( ( 1 2 ) ( 3 4 ) ( 1 2 ) ) .
        :f :p () .
        :g :p ( :a :b :a ) .
        :h :p ( "x" "y"@en <<( :a :q 1 )>> ) .
        :i :r ( 1 ( 2 undef ) ) .
        GRAPH :g1 { :j :p ( 1 2 ) . :k :p ( undef 5 ) . }
        GRAPH :g2 { :j :p ( 1 2 ) . :l :p ( 9 9 9 ) . }
        """;

    private Path source(String name, String text) throws Exception {
        Path src = dir.resolve(name);
        Files.writeString(src, text);
        return src;
    }

    /** Builds {@code src} with every engine of {@link WriterEngines}, {@code -seqindex} on or off. */
    private File build(WriterEngines.Engine engine, Path src, String name, boolean seqindex) throws Exception {
        File out = dir.resolve(name).toFile();
        String before = System.getProperty(WriterEngines.SEQINDEX_PROPERTY);
        System.setProperty(WriterEngines.SEQINDEX_PROPERTY, Boolean.toString(seqindex));
        try {
            engine.buildStore(src.toFile(), out);
        } finally {
            if (before == null) {
                System.clearProperty(WriterEngines.SEQINDEX_PROPERTY);
            } else {
                System.setProperty(WriterEngines.SEQINDEX_PROPERTY, before);
            }
        }
        return out;
    }

    private static WriterEngines.Engine method0() {
        return WriterEngines.all().filter(e -> e.name().startsWith("method0")).findFirst().orElseThrow();
    }

    private static String verify(File store) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int exit = new VerifyCommand(store, true, new PrintStream(out, true)).run();
        return (exit == 0) ? null : out.toString();
    }

    @Test
    void everyEngineWritesTheSameIndex() throws Exception {
        Path src = source("data.trig", DATA);
        byte[] reference = Files.readAllBytes(build(method0(), src, "method0.h5", true).toPath());
        for (WriterEngines.Engine engine : WriterEngines.all().toList()) {
            File out = build(engine, src, engine.name() + ".h5", true);
            assertArrayEquals(reference, Files.readAllBytes(out.toPath()), engine.name());
            assertEquals(null, verify(out), engine.name());
        }
    }

    /**
     * Twenty thousand pairs: several blocks of each column, and on the disk engines a sort that spills
     * runs of a thousand and merges them.
     */
    @Test
    void theDiskEnginesWriteTheSameIndexFromManyRuns() throws Exception {
        Path src = source("rosters.ttl", rosters(2000));
        File m0 = dir.resolve("r0.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(m0).setSequenceIndex(true).build().write();
        byte[] reference = Files.readAllBytes(m0.toPath());
        File m1 = dir.resolve("r1.h5").toFile();
        HugeHDF5Writer.Builder b1 = HugeHDF5Writer.Builder().setDestination(m1).setSequenceIndex(true);
        b1.setSource(src.toFile());
        b1.setIdSpillBatch(1000);
        b1.build().write();
        assertArrayEquals(reference, Files.readAllBytes(m1.toPath()), "method 1");
        File m4 = dir.resolve("r4.h5").toFile();
        com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer.Builder b4 =
                com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer.Builder()
                        .setDestination(m4).setSequenceIndex(true).setCores(2);
        b4.setSource(src.toFile());
        b4.setIdSpillBatch(1000);
        b4.build().write();
        assertArrayEquals(reference, Files.readAllBytes(m4.toPath()), "method 4");
        assertEquals(null, verify(m4));
        try (BeakGraph g = BG.getBeakGraph(m0)) {
            assertTrue(dictionary(g).sequenceIndexSize()[1] > 15_000, "enough pairs for several blocks");
        }
    }

    @Test
    void theDiskEngineWritesTheSameIndexToZarr() throws Exception {
        Path src = source("data.trig", DATA);
        File m0 = dir.resolve("m0.zarr.zip").toFile();
        File m1 = dir.resolve("m1.zarr.zip").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(m0).setSequenceIndex(true).build().write();
        HugeHDF5Writer.Builder b = HugeHDF5Writer.Builder().setDestination(m1).setSequenceIndex(true);
        b.setSource(src.toFile());
        b.build().write();
        assertArrayEquals(Files.readAllBytes(m0.toPath()), Files.readAllBytes(m1.toPath()));
        assertEquals(null, verify(m1));
        try (BeakGraph g = BG.getBeakGraph(m1)) {
            assertTrue(dictionary(g).storesSequenceIndex());
        }
    }

    @Test
    void theIndexIsTheTransposeOfTheSequences() throws Exception {
        File out = build(method0(), source("data.trig", DATA), "t.h5", true);
        try (BeakGraph g = BG.getBeakGraph(out)) {
            PositionalDictionaryReader dict = dictionary(g);
            assertTrue(dict.hasSequenceIndex());
            // The transpose, worked out from the sequences.
            Map<Long, TreeSet<Long>> expected = new TreeMap<>();
            for (long id = dict.firstSequenceObjectId(); id <= dict.lastSequenceObjectId(); id++) {
                for (long e : dict.sequenceElementIds(id)) {
                    expected.computeIfAbsent(e, x -> new TreeSet<>()).add(id);
                }
            }
            assertTrue(expected.containsKey(0L), "the fixture has slots");
            // The index, read back.
            Map<Long, TreeSet<Long>> index = new TreeMap<>();
            long[] size = dict.sequenceIndexSize();
            List<Long> order = new ArrayList<>();
            for (long k = 0; k < size[0]; k++) {
                long e = dict.sequenceIndexElement(k);
                order.add(e);
                long[] range = dict.sequencesHolding(e);
                TreeSet<Long> seqs = new TreeSet<>();
                List<Long> listed = new ArrayList<>();
                for (long q = range[0]; q < range[1]; q++) {
                    listed.add(dict.sequenceIndexObjectId(q));
                    seqs.add(dict.sequenceIndexObjectId(q));
                }
                assertEquals(new ArrayList<>(seqs), listed, "element " + e + ": its sequences ascend, each once");
                index.put(e, seqs);
            }
            assertEquals(new ArrayList<>(expected.keySet()), order, "the elements ascend, 0 first");
            assertEquals(expected, index);
            long pairs = expected.values().stream().mapToLong(TreeSet::size).sum();
            assertEquals(pairs, size[1]);
        }
    }

    @Test
    void noIndexWithoutTheFlagOrWithoutElements() throws Exception {
        try (BeakGraph g = BG.getBeakGraph(build(method0(), source("data.trig", DATA), "plain.h5", false))) {
            assertFalse(dictionary(g).storesSequenceIndex());
        }
        // Only empty sequences: nothing to index, and no dataset.
        Path empty = source("empty.ttl", "PREFIX : <http://ex.org/>\n:a :p () .\n:b :q 1 .\n");
        try (BeakGraph g = BG.getBeakGraph(build(method0(), empty, "empty.h5", true))) {
            assertFalse(dictionary(g).storesSequenceIndex());
        }
        // No sequences at all: the flag changes nothing.
        Path none = source("none.ttl", "PREFIX : <http://ex.org/>\n:a :p 1 .\n:b :q :a .\n");
        assertArrayEquals(Files.readAllBytes(build(method0(), none, "none-off.h5", false).toPath()),
                Files.readAllBytes(build(method0(), none, "none-on.h5", true).toPath()));
    }

    /**
     * Rosters of ten from 500 players, a few seats empty, and a few captains to join through. Few: a variable
     * bound already passes a slot, so every roster with an empty seat is a candidate for every captain, and
     * with one seat in ten empty that is two rosters in three.
     */
    static String rosters(int teams) {
        StringBuilder sb = new StringBuilder("PREFIX : <http://ex.org/>\n");
        java.util.Random r = new java.util.Random(7);
        for (int t = 0; t < teams; t++) {
            sb.append(":team").append(t).append(" :roster (");
            for (int m = 0; m < 10; m++) {
                sb.append(r.nextInt(200) == 0 ? " undef" : " :player" + r.nextInt(500));
            }
            sb.append(" ) .\n");
            if (t % 100 == 0) {
                sb.append(":team").append(t).append(" :captain :player").append(t % 500).append(" .\n");
            }
        }
        return sb.toString();
    }

    @Test
    void containmentReadsOnlyTheCandidatesAndAnswersAsTheScan() throws Exception {
        File out = build(method0(), source("rosters.ttl", rosters(2000)), "rosters.h5", true);
        String[] queries = {
            "SELECT * { ?t :roster ( ... :player42 ... ) }",
            "SELECT * { ?t :roster ( ?first ... :player42 ... ) }",
            "SELECT * { ?t :roster ( ... undef ... ) }",
            "SELECT * { ?c :captain ?m . ?t :roster ( ... ?m ... ) }",
            "SELECT (COUNT(*) AS ?n) { ?t :roster ( ... :player42 ... :player43 ... ) }",
            // Jena joins a VALUES table, or UNNEST, with the pattern rather than feeding it the rows (its
            // join classifier takes a variable inside a sequence pattern for one a slot may leave unbound),
            // so the pattern runs with ?m unbound: a scan, with the same answers.
            "SELECT * { VALUES ?m { :player3 :nobody } ?t :roster ( ... ?m ... ) }",
            "SELECT * { :team0 :roster ?q UNNEST(?q AS ?m) ?t :roster ( ... ?m ... ) }",
        };
        try (BeakGraph g = BG.getBeakGraph(out)) {
            for (String q : queries) {
                long before = BGIteratorPOS.CANDIDATE_SCANS.sum();
                ResultSetRewindable indexed = select(g, q);
                assertTrue(q.contains("VALUES") || q.contains("UNNEST") || BGIteratorPOS.CANDIDATE_SCANS.sum() > before, "the index was not read: " + q);
                ResultSetRewindable scanned = withoutIndexReads(() -> select(g, q));
                assertTrue(indexed.size() > 0, "no rows: " + q);
                assertEquals(rows(scanned), rows(indexed), q);
            }
        }
    }

    /**
     * The join reorder costs a sequence pattern by its rows: those of its predicate whose object holds its
     * prefix, times the share of sequences its other constants name (RDFARRY.md step 8). A roster holding
     * one player of 500 goes before a check that keeps half the teams, with the index and without it; it
     * used to cost as every roster, and came after.
     */
    @Test
    void theReorderCostsSequencePatternsByTheirRows() throws Exception {
        StringBuilder data = new StringBuilder(rosters(2000));
        for (int t = 0; t < 2000; t++) {
            data.append(":team").append(t).append(" :league ").append((t % 2 == 0) ? ":east" : ":west").append(" .\n");
        }
        Path src = source("leagues.ttl", data.toString());
        for (boolean seqindex : new boolean[] {true, false}) {
            File out = dir.resolve("leagues-" + seqindex + ".h5").toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(out)
                    .setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setSequenceIndex(seqindex).build().write();
            try (BeakGraph g = BG.getBeakGraph(out)) {
                org.apache.jena.sparql.core.BasicPattern bgp = bgp(
                        "SELECT * { ?t :league :east . ?t :roster ( ... :player42 ... ) }");
                org.apache.jena.sparql.core.BasicPattern planned = g.getReorderTransform().reorder(bgp);
                assertEquals(bgp.get(1), planned.get(0), "with -seqindex " + seqindex + ": " + planned);
            }
        }
    }

    private static org.apache.jena.sparql.core.BasicPattern bgp(String body) {
        org.apache.jena.sparql.algebra.Op op = org.apache.jena.sparql.algebra.Algebra.compile(
                QueryFactory.create("PREFIX : <http://ex.org/>\n" + body, Syntax.syntaxARQ));
        while (!(op instanceof org.apache.jena.sparql.algebra.op.OpBGP)) {
            op = ((org.apache.jena.sparql.algebra.op.Op1) op).getSubOp();
        }
        return ((org.apache.jena.sparql.algebra.op.OpBGP) op).getPattern();
    }

    /**
     * A bound variable passes a slot, so the sequences with one are candidates for every value; but only
     * those among the scan's own objects. Squads without empty seats, beside rosters with many, are read
     * through the index for a captain, and the rosters by the scan.
     */
    @Test
    void aBoundVariableTakesTheSlotsOfItsOwnPredicateOnly() throws Exception {
        StringBuilder data = new StringBuilder("PREFIX : <http://ex.org/>\n");
        java.util.Random r = new java.util.Random(11);
        for (int t = 0; t < 2000; t++) {
            StringBuilder roster = new StringBuilder(), squad = new StringBuilder();
            for (int m = 0; m < 10; m++) {
                roster.append(r.nextInt(5) == 0 ? " undef" : " :player" + r.nextInt(500));
                squad.append(" :player").append(r.nextInt(500));
            }
            data.append(":team").append(t).append(" :roster (").append(roster).append(" ) .\n");
            data.append(":squad").append(t).append(" :members (").append(squad).append(" ) .\n");
        }
        for (int c = 0; c < 20; c++) {
            data.append(":club").append(c).append(" :captain :player").append(c * 7).append(" .\n");
        }
        File out = build(method0(), source("squads.ttl", data.toString()), "squads.h5", true);
        try (BeakGraph g = BG.getBeakGraph(out)) {
            for (String q : new String[] {
                    "SELECT * { ?c :captain ?m . ?t :members ( ... ?m ... ) }",
                    "SELECT * { ?c :captain ?m . ?t :roster ( ... ?m ... ) }"}) {
                long before = BGIteratorPOS.CANDIDATE_SCANS.sum();
                ResultSetRewindable indexed = select(g, q);
                long scans = BGIteratorPOS.CANDIDATE_SCANS.sum() - before;
                if (q.contains(":members")) {
                    assertEquals(20, scans, "a captain's squads, through the index: " + q);
                } else {
                    assertEquals(0, scans, "two rosters in three hold a slot, and each passes a captain: " + q);
                }
                ResultSetRewindable scanned = withoutIndexReads(() -> select(g, q));
                assertTrue(indexed.size() > 0, "no rows: " + q);
                assertEquals(rows(scanned), rows(indexed), q);
            }
        }
    }

    @Test
    void anOldReaderOrTheSwitchOffReadsNoIndex() throws Exception {
        File out = build(method0(), source("rosters.ttl", rosters(200)), "off.h5", true);
        String before = System.getProperty("beakgraph.seqindex.read");
        System.setProperty("beakgraph.seqindex.read", "false");
        try (BeakGraph g = BG.getBeakGraph(out)) {
            PositionalDictionaryReader dict = dictionary(g);
            assertFalse(dict.hasSequenceIndex());
            assertTrue(dict.storesSequenceIndex());
        } finally {
            restore("beakgraph.seqindex.read", before);
        }
    }

    /**
     * The rows as a sorted list of their bindings: the answers hold no blank nodes, so terms compare as
     * text. Jena's ResultsCompare recurses once a row, past the stack at tens of thousands.
     */
    private static List<String> rows(ResultSetRewindable rs) {
        rs.reset();
        List<String> out = new ArrayList<>();
        while (rs.hasNext()) {
            var b = rs.nextBinding();
            StringBuilder sb = new StringBuilder();
            new TreeSet<>(rs.getResultVars()).forEach(v -> sb.append(v).append('=')
                    .append(b.get(org.apache.jena.sparql.core.Var.alloc(v))).append(' '));
            out.add(sb.toString());
        }
        rs.reset();
        java.util.Collections.sort(out);
        return out;
    }

    private static PositionalDictionaryReader dictionary(BeakGraph g) {
        return (PositionalDictionaryReader) ((HDF5Reader) g.getReader()).getDictionary();
    }

    private static ResultSetRewindable select(BeakGraph g, String body) {
        try (QueryExecution qe = QueryExecution.dataset(g.getDataset())
                .query(QueryFactory.create("PREFIX : <http://ex.org/>\n" + body, Syntax.syntaxARQ)).build()) {
            return ResultSetFactory.makeRewindable(qe.execSelect());
        }
    }

    /** {@code body} on a reader opened afresh with the index switched off: the scan's answer. */
    private <T> T withoutIndexReads(java.util.concurrent.Callable<T> body) throws Exception {
        String before = System.getProperty("beakgraph.seqindex.ratio");
        // A ratio no candidate list can meet: every scan reads its whole run.
        System.setProperty("beakgraph.seqindex.ratio", Long.toString(Long.MAX_VALUE / (1L << 40)));
        try {
            long scans = BGIteratorPOS.CANDIDATE_SCANS.sum();
            T out = body.call();
            assertEquals(scans, BGIteratorPOS.CANDIDATE_SCANS.sum(), "the scan read the index");
            return out;
        } finally {
            restore("beakgraph.seqindex.ratio", before);
        }
    }

    private static void restore(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
