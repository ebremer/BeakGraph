package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.WriterEngines;
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
            // A leaf of ( 101 102 ), which ( "label" ( 101 102 ) ) holds, does not pack, and refers to by id.
            Node pair = NodeFactory.createSeqNode(List.of(literal("101", "integer"), literal("102", "integer")));
            long pairId = d8.getObjects().locate(pair);
            assertTrue(pairId >= 1, "the packed ( 101 102 ) keeps its row");
            assertTrue(d8.heldOnlyInArrays(pairId));
            assertTrue(d8.getObjects().locate(literal("101", "integer")) < 1, "its leaves do not");
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

    @Test
    void theOtherEnginesRefuseItForNow() throws Exception {
        Path src = dir.resolve("refuse.ttl");
        Files.writeString(src, DATA);
        for (WriterEngines.Engine e : WriterEngines.all().filter(e -> !e.name().startsWith("method0")).toList()) {
            File out = dir.resolve("refuse-" + e.name() + ".h5").toFile();
            Exception ex = assertThrows(Exception.class, () -> engineWith(e.name(), src.toFile(), out).write(), e.name());
            boolean named = false;
            for (Throwable c = ex; c != null; c = c.getCause()) {
                named |= String.valueOf(c.getMessage()).contains("-arrays-only");
            }
            assertTrue(named, e.name() + ": " + ex);
            assertFalse(out.exists(), e.name() + " left a store");
        }
    }

    private static com.ebremer.beakgraph.core.BeakGraphWriter engineWith(String name, File src, File dest) {
        return switch (name.substring(0, 7)) {
            case "method1" -> { var b = com.ebremer.beakgraph.huge.HugeHDF5Writer.Builder().setDestination(dest).setArrays(ONLY); b.setSource(src); yield b.build(); }
            case "method2" -> { var b = com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY); b.setSource(src); yield b.build(); }
            case "method3" -> { var b = com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY); b.setSource(src); yield b.build(); }
            case "method4" -> { var b = com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY); b.setSource(src); yield b.build(); }
            case "method5" -> { var b = com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer.Builder().setDestination(dest).setCores(2).setArrays(ONLY); b.setSource(src); yield b.build(); }
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
