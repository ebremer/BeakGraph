package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.cmdline.VerifyCommand;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.util.IsoMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code -arrays-only} (format v8, RDFARRY.md step 9, part 1): the sequences a tensor or ragged class takes
 * are stored only in {@code /.BG/arrays}. The store holds what the v7 store holds, its leaves have
 * dictionary rows only when something else uses them, concrete sequences are found by term, and sequence
 * patterns answer as over the v7 store.
 */
class ArraysOnlyTest {

    @TempDir
    Path dir;

    /**
     * Tensors and ragged rows of every leaf type, a 2x3 nest, slots, a leaf shared with a plain triple, a
     * packed sequence nested in one that does not pack, an inner row that is also an object, record rows,
     * and sequences no class takes.
     */
    static final String DATA = """
        PREFIX : <http://ex.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        :v1 :emb ( 0.25E0 -1.5E0 3.75E0 ) .
        :v2 :emb ( 1.5E0 2.5E0 ) .
        :v3 :emb ( 9.0E0 8.0E0 7.0E0 6.0E0 5.0E0 ) .
        :i1 :ints ( 1 2 3 ) .
        :i2 :ints ( 10 undef 30 ) .
        :f1 :floats ( "1.25"^^xsd:float "2.5"^^xsd:float ) .
        :b1 :flags ( true false true ) .
        :m1 :matrix ( ( 1 2 3 ) ( 4 5 6 ) ) .
        :m2 :matrix ( ( 7 8 9 ) ( 10 11 12 ) ) .
        :x :val 1.5E0 .
        :w :mixed ( "label" ( 101 102 ) ) .
        :u :pair ( 101 102 ) .
        :r1 :reading ( "8480-6" 120 "mm[Hg]" ) .
        :r2 :reading ( "8462-4" 80 "mm[Hg]" ) .
        :n :iris ( :a :b ) .
        :e :empty () .
        :one :single ( 42 ) .
        """;

    /** Every class a tensor of its own (one row is enough), records too, two leaves at least. */
    static final SequenceArrays.Options ONLY = new SequenceArrays.Options(1, 2, false, SequenceArrays.TimeUnit.US, true);
    static final SequenceArrays.Options COPY = ONLY.withOnly(false);

    private File build(String name, String text, SequenceArrays.Options options) throws Exception {
        Path src = dir.resolve(name + ".ttl");
        Files.writeString(src, text);
        File out = dir.resolve(name).toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).setArrays(options).build().write();
        return out;
    }

    private static PositionalDictionaryReader dictionary(BeakGraph g) {
        return (PositionalDictionaryReader) ((HDF5Reader) g.getReader()).getDictionary();
    }

    private static DatasetGraph parsed(String text) {
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.fromString(text, Lang.TURTLE).parse(dsg);
        return dsg;
    }

    /** The store's default graph, every quad read back through the store's own find. */
    private static DatasetGraph stored(BeakGraph g) {
        DatasetGraph dsg = DatasetGraphFactory.create();
        g.find().forEachRemaining(t -> dsg.getDefaultGraph().add(t));
        return dsg;
    }

    private static String verify(File store) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int exit = new VerifyCommand(store, true, new PrintStream(out, true)).run();
        return (exit == 0) ? null : out.toString();
    }

    @Test
    void aV8StoreHoldsWhatTheSourceSays() throws Exception {
        for (String ext : new String[] {".h5", ".zarr.zip"}) {
            File v8 = build("only" + ext, DATA, ONLY);
            File v7 = build("copy" + ext, DATA, COPY);
            // The v7 store holds the source with its numbers canonical (SPECIFICATIONS.md §9.3): 0.25E0 is "0.25"^^xsd:double.
            try (BeakGraph g = BG.getBeakGraph(v8); BeakGraph c = BG.getBeakGraph(v7)) {
                assertEquals(Params.ARRAYS_ONLY_FORMAT_VERSION, ((HDF5Reader) g.getReader()).getFormatVersion(), ext);
                assertEquals(Params.FORMAT_VERSION, ((HDF5Reader) c.getReader()).getFormatVersion(), ext);
                assertTrue(dictionary(g).arraysOnly(), ext);
                assertEquals(parsed(DATA).getDefaultGraph().size(), stored(g).getDefaultGraph().size(), ext);
                assertTrue(IsoMatcher.isomorphic(stored(c), stored(g)), ext + ": the v8 store holds what the v7 one holds");
            }
            assertNull(verify(v8), ext);
        }
    }

    @Test
    void theLeavesOfPackedSequencesHaveNoRowsUnlessSomethingElseUsesThem() throws Exception {
        try (BeakGraph v8 = BG.getBeakGraph(build("only.h5", DATA, ONLY));
             BeakGraph v7 = BG.getBeakGraph(build("copy.h5", DATA, COPY))) {
            PositionalDictionaryReader d8 = dictionary(v8), d7 = dictionary(v7);
            assertFalse(d7.arraysOnly());
            // Only in a packed sequence: no row in v8, a row in v7.
            Node only = literal("3.75", "double");
            assertTrue(d8.getObjects().locate(only) < 1, "3.75 is a leaf of ( 0.25 -1.5 3.75 ) alone");
            assertTrue(d7.getObjects().locate(only) >= 1);
            // Also the object of :x :val: a row in both.
            assertTrue(d8.getObjects().locate(literal("1.5", "double")) >= 1, "1.5 is :x's value too");
            // ( 101 102 ) is :u's object and packs, but ( "label" ( 101 102 ) ), stored with its elements,
            // holds it: so it keeps its elements, and its leaves their rows, as in v7.
            Node pair = NodeFactory.createSeqNode(List.of(literal("101", "integer"), literal("102", "integer")));
            long pairId = d8.getObjects().locate(pair);
            assertTrue(pairId >= 1);
            assertFalse(d8.heldOnlyInArrays(pairId), "a sequence a canonical one holds keeps its elements");
            assertTrue(d8.getObjects().locate(literal("101", "integer")) >= 1, "and its leaves their rows");
            // ( 2.5 ... ) is a quad's object alone: held only in the arrays.
            Node emb = NodeFactory.createSeqNode(List.of(literal("1.5", "double"), literal("2.5", "double")));
            assertTrue(d8.heldOnlyInArrays(d8.getObjects().locate(emb)));
            assertTrue(d8.getObjects().locate(literal("2.5", "double")) < 1, "its leaves have no rows");
            // A record row keeps its canonical elements, and its leaves their rows.
            Node reading = NodeFactory.createSeqNode(List.of(NodeFactory.createLiteralString("8480-6"),
                    literal("120", "integer"), NodeFactory.createLiteralString("mm[Hg]")));
            long readingId = d8.getObjects().locate(reading);
            assertTrue(readingId >= 1 && !d8.heldOnlyInArrays(readingId), "a record row is canonical");
            assertTrue(d8.getObjects().locate(literal("120", "integer")) >= 1);
            // The inner rows of the 2x3 nests are rows only in v7.
            Node inner = NodeFactory.createSeqNode(List.of(literal("1", "integer"), literal("2", "integer"),
                    literal("3", "integer")));
            assertTrue(d8.getObjects().locate(inner) >= 1, "( 1 2 3 ) is :i1's object too");
            Node innerOnly = NodeFactory.createSeqNode(List.of(literal("4", "integer"), literal("5", "integer"),
                    literal("6", "integer")));
            assertTrue(d8.getObjects().locate(innerOnly) < 1, "( 4 5 6 ) is only a row of :m1's nest");
            assertTrue(d7.getObjects().locate(innerOnly) >= 1);
            // Fewer elements, fewer terms.
            assertTrue(d8.sequenceElementCount() < d7.sequenceElementCount());
            assertTrue(d8.getObjects().getNumberOfNodes() < d7.getObjects().getNumberOfNodes());
        }
    }

    @Test
    void everySequenceIsFoundByItsTerm() throws Exception {
        try (BeakGraph g = BG.getBeakGraph(build("only.h5", DATA, ONLY))) {
            PositionalDictionaryReader d = dictionary(g);
            int held = 0;
            for (long id = d.firstSequenceObjectId(); id <= d.lastSequenceObjectId(); id++) {
                Node seq = d.getObjects().extract(id);
                assertEquals(id, d.getObjects().locate(seq), "sequence " + id + " " + seq);
                if (d.heldOnlyInArrays(id)) {
                    held++;
                }
            }
            assertTrue(held >= 9, held + " sequences held only in the arrays");
            assertEquals(List.of("v2"), subjects(g, "SELECT ?s { ?s :emb ( \"1.5\"^^xsd:double \"2.5\"^^xsd:double ) }"));
            assertEquals(List.of("i2"), subjects(g, "SELECT ?s { ?s :ints ( 10 undef 30 ) }"));
            assertEquals(List.of("u"), subjects(g, "SELECT ?s { ?s :pair ( 101 102 ) }"));
            assertEquals(List.of(), subjects(g, "SELECT ?s { ?s :emb ( \"1.5\"^^xsd:double \"2.0\"^^xsd:double ) }"));
        }
    }

    /** The same patterns, over the v7 store on ids and over the v8 store by term, neither through jena-array's solver. */
    @Test
    void patternsAnswerAsOverTheV7Store() throws Exception {
        String[] queries = {
            "SELECT ?s ?x { ?s :emb ( ?x ... ) }",
            "SELECT ?s ?x { ?s :emb ( ... ?x ... ) }",
            "SELECT ?s { ?s :emb ( ... \"2.5\"^^xsd:double ... ) }",
            "SELECT ?s ?a ?b { ?s :matrix ( ( ?a ... ) ( ... ?b ) ) }",
            "SELECT ?s ?x { ?s :ints ( ?x undef ?y ) }",
            "SELECT ?s ?m { ?s :mixed ( ?l ( ?m ... ) ) }",
            "SELECT ?s ?v { ?s :reading ( \"8480-6\" ?v ?u ) }",
            "SELECT ?s ?t { :x :val ?v . ?s :emb ( ... ?v ... ) . ?s ?p ?t }",
            "SELECT ?s ?r { ?s :emb ( \"1.5\"^^xsd:double ...?r ) }",
            // ( 101 102 ) packs but ( "label" ( 101 102 ) ) holds it: a join through its leaf, by id.
            "SELECT ?s ?t ?a { ?s :pair ( ?a ?b ) . ?t :mixed ( ?l ( ?a ... ) ) }",
            "SELECT ?s ?v { ?s :pair ( ?v ... ) . :w :mixed ( ?l ( ... ?v ... ) ) }",
            "SELECT ?q (SEQLEN(?q) AS ?n) { ?s :emb ?q }",
            "SELECT ?s ?v { ?s :emb ?q UNNEST(?q AS ?v) }",
        };
        try (BeakGraph v8 = BG.getBeakGraph(build("only.h5", DATA, ONLY));
             BeakGraph v7 = BG.getBeakGraph(build("copy.h5", DATA, COPY))) {
            for (String q : queries) {
                long delegated = com.ebremer.beakgraph.hdf5.jena.PatternMatchBG.DELEGATED.get();
                assertEquals(rows(v7, q), rows(v8, q), q);
                assertEquals(delegated, com.ebremer.beakgraph.hdf5.jena.PatternMatchBG.DELEGATED.get(), "delegated: " + q);
                assertFalse(rows(v7, q).isEmpty(), "no rows: " + q);
            }
        }
    }

    /**
     * The reorder's figures (RDFARRY.md step 8) over a v8 store: a constant only held rows hold may match, at
     * the guess's share of the held rows' share, and lengths come from the classes.
     */
    @Test
    void theReorderCostsSequencePatternsOverAV8Store() throws Exception {
        try (BeakGraph v8 = BG.getBeakGraph(build("cost.h5", DATA, ONLY))) {
            PositionalDictionaryReader d = dictionary(v8);
            assertTrue(d.heldSequenceCount() >= 9, d.heldSequenceCount() + " held");
            Node gap = org.apache.jena.sparql.core.Node_SeqGap.create();
            Node heldOnly = NodeFactory.createSeqNode(List.of(gap, literal("2.5", "double"), gap));
            var e = com.ebremer.beakgraph.hdf5.jena.SequenceMatcher.estimate(heldOnly, d, v -> false);
            assertTrue(e != null && e.share() > 0, "a leaf with no row may still be held: " + e);
            double held = (double) d.heldSequenceCount() / d.sequenceCount();
            assertEquals(held * com.ebremer.beakgraph.hdf5.jena.SequenceMatcher.GUESS, e.share(), 1e-9);
            Node free = NodeFactory.createSeqNode(List.of(gap, NodeFactory.createVariable("v"), gap));
            var f = com.ebremer.beakgraph.hdf5.jena.SequenceMatcher.estimate(free, d, v -> false);
            assertTrue(f != null && f.placements() >= 1, "placements from the classes' lengths: " + f);
        }
    }

    /**
     * With {@code -void} the join reorder costs a sequence pattern over a v8 store too: a value one held row
     * holds goes before a check that keeps half the teams. It used to have no figure there, and came after.
     */
    @Test
    void theReorderPutsARareHeldValueFirst() throws Exception {
        StringBuilder data = new StringBuilder("PREFIX : <http://ex.org/>\n");
        for (int t = 0; t < 2000; t++) {
            data.append(":team").append(t).append(" :league ").append((t % 2 == 0) ? ":east" : ":west")
                .append(" ; :emb ( ").append(t).append(".25E0 ").append(t % 7).append(".5E0 ) .\n");
        }
        Path src = dir.resolve("leagues.ttl");
        Files.writeString(src, data.toString());
        File out = dir.resolve("leagues.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).setArrays(ONLY)
                .setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).build().write();
        try (BeakGraph g = BG.getBeakGraph(out)) {
            assertTrue(dictionary(g).arraysOnly());
            String q = "SELECT * { ?t :league :east . ?t :emb ( ... \"42.25\"^^xsd:double ... ) }";
            org.apache.jena.sparql.algebra.Op op = org.apache.jena.sparql.algebra.Algebra.compile(
                    QueryFactory.create(PREFIXES + q, Syntax.syntaxARQ));
            while (!(op instanceof org.apache.jena.sparql.algebra.op.OpBGP)) {
                op = ((org.apache.jena.sparql.algebra.op.Op1) op).getSubOp();
            }
            org.apache.jena.sparql.core.BasicPattern bgp = ((org.apache.jena.sparql.algebra.op.OpBGP) op).getPattern();
            org.apache.jena.sparql.core.BasicPattern planned = g.getReorderTransform().reorder(bgp);
            assertEquals(bgp.get(1), planned.get(0), "planned: " + planned);
            assertEquals(List.of("team42"), subjects(g, "SELECT ?s { ?s :league :east . ?s :emb ( ... \"42.25\"^^xsd:double ... ) }"));
        }
    }

    @Test
    void withoutAPackedSequenceTheStoreIsV7AndUnchanged() throws Exception {
        String text = "PREFIX : <http://ex.org/>\n:a :p ( \"x\" 1 ) .\n:b :q 2 .\n";
        File only = build("none-only.h5", text, ONLY);
        File copy = build("none-copy.h5", text, COPY);
        assertArrayEquals(Files.readAllBytes(copy.toPath()), Files.readAllBytes(only.toPath()));
        try (BeakGraph g = BG.getBeakGraph(only)) {
            assertEquals(Params.FORMAT_VERSION, ((HDF5Reader) g.getReader()).getFormatVersion());
        }
    }

    /**
     * Sequences held only in the arrays and nothing else: no element at all, every number left out of the
     * dictionary but the one a plain triple also has.
     */
    static final String ONLY_PACKED = """
        PREFIX : <http://ex.org/>
        :a :emb ( 0.5E0 1.5E0 2.5E0 ) .
        :b :emb ( 3.5E0 4.5E0 5.5E0 ) .
        :c :emb ( 6.5E0 undef 8.5E0 ) .
        :d :val 4.5E0 .
        """;

    /**
     * Every engine leaves out what method 0 leaves out, the disk engines by the same rule in their merge: the
     * same bytes, the element index too, in both containers.
     */
    @Test
    void everyEngineWritesTheSameV8Store() throws Exception {
        String[] engines = {"method1", "method2", "method3", "method4", "method5"};
        int fixture = 0;
        for (String text : new String[] {DATA, ONLY_PACKED}) {
            Path src = dir.resolve("same" + fixture + ".ttl");
            Files.writeString(src, text);
            for (boolean seqindex : new boolean[] {false, true}) {
                for (String ext : new String[] {".h5", ".zarr.zip"}) {
                    String base = "same" + fixture + "-" + seqindex;
                    File reference = dir.resolve(base + "-method0" + ext).toFile();
                    HDF5Writer.Builder().setSource(src.toFile()).setDestination(reference).setArrays(ONLY)
                            .setSequenceIndex(seqindex).build().write();
                    for (String engine : engines) {
                        File out = dir.resolve(base + "-" + engine + ext).toFile();
                        engineWith(engine, src.toFile(), out, seqindex).write();
                        String what = "fixture " + fixture + ", " + engine + ext + (seqindex ? " -seqindex" : "");
                        assertArrayEquals(Files.readAllBytes(reference.toPath()), Files.readAllBytes(out.toPath()), what);
                    }
                    try (BeakGraph g = BG.getBeakGraph(reference)) {
                        assertEquals(Params.ARRAYS_ONLY_FORMAT_VERSION, ((HDF5Reader) g.getReader()).getFormatVersion());
                        assertEquals(parsed(text).getDefaultGraph().size(), stored(g).getDefaultGraph().size());
                    }
                    assertNull(verify(reference), base + ext);
                }
            }
            fixture++;
        }
    }

    /** A store of packed sequences alone: their leaves have no rows, but for the one a triple also has. */
    @Test
    void aStoreOfPackedSequencesAloneHasNoElements() throws Exception {
        try (BeakGraph g = BG.getBeakGraph(build("alone.h5", ONLY_PACKED, ONLY));
             BeakGraph c = BG.getBeakGraph(build("alone-copy.h5", ONLY_PACKED, COPY))) {
            PositionalDictionaryReader d = dictionary(g);
            assertEquals(0, d.sequenceElementCount());
            assertTrue(d.getObjects().locate(literal("2.5", "double")) < 1);
            assertTrue(d.getObjects().locate(literal("4.5", "double")) >= 1, ":d's value");
            // :b holds :d's value; :c's empty slot takes any value, so it matches too.
            String join = "SELECT ?s { :d :val ?v . ?s :emb ( ... ?v ... ) }";
            assertEquals(List.of("b", "c"), subjects(g, join));
            assertEquals(subjects(c, join), subjects(g, join));
            assertEquals(List.of("c"), subjects(g, "SELECT ?s { ?s :emb ( ?a undef ?b ) }"));
        }
    }

    private static com.ebremer.beakgraph.core.BeakGraphWriter engineWith(String name, File src, File dest, boolean seqindex) {
        return switch (name.substring(0, 7)) {
            case "method1" -> { var b = com.ebremer.beakgraph.huge.HugeHDF5Writer.Builder().setDestination(dest).setArrays(ONLY).setSequenceIndex(seqindex); b.setSource(src); yield b.build(); }
            case "method2" -> { var b = com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY).setSequenceIndex(seqindex); b.setSource(src); yield b.build(); }
            case "method3" -> { var b = com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY).setSequenceIndex(seqindex); b.setSource(src); yield b.build(); }
            case "method4" -> { var b = com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY).setSequenceIndex(seqindex); b.setSource(src); yield b.build(); }
            case "method5" -> { var b = com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY).setSequenceIndex(seqindex); b.setSource(src); yield b.build(); }
            default -> throw new IllegalArgumentException(name);
        };
    }

    private static final String PREFIXES = "PREFIX : <http://ex.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

    private static Node literal(String lex, String xsd) {
        return NodeFactory.createLiteralDT(lex, org.apache.jena.datatypes.TypeMapper.getInstance()
                .getSafeTypeByName("http://www.w3.org/2001/XMLSchema#" + xsd));
    }

    private static List<String> subjects(BeakGraph g, String body) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(g.getDataset())
                .query(QueryFactory.create(PREFIXES + body, Syntax.syntaxARQ)).build()) {
            qe.execSelect().forEachRemaining(r -> out.add(r.getResource("s").getLocalName()));
        }
        return out;
    }

    /** The rows as sorted text: no blank nodes in these answers. */
    private static List<String> rows(BeakGraph g, String body) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(g.getDataset())
                .query(QueryFactory.create(PREFIXES + body, Syntax.syntaxARQ)).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = rs.getResultVars();
            rs.forEachRemaining(r -> {
                StringBuilder sb = new StringBuilder();
                for (String v : vars) {
                    sb.append(v).append('=').append(r.get(v)).append(' ');
                }
                out.add(sb.toString());
            });
        }
        Collections.sort(out);
        return out;
    }
}
