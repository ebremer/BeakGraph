package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.SeqTerms;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.IsoMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * RDF-array sequences in a method-0 store (format v7, SPECIFICATIONS.md
 * §7.6b; RDFARRY.md step 3): the round trip (slots, empty and nested
 * sequences, blank nodes inside, triple terms inside), the stores the writer
 * lays down, the sequence suffix and the triple-term band, locate on ids,
 * numeric canonicalization inside, and the §9.5 guards.
 */
public class RdfArraySequenceStorageTest {

    private static final String P = "http://ex.org/";

    static final String TRIG = """
        @prefix : <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

        :a :list ( 1 2 3 ) .
        :a :mixed ( "x" :iri 2.5 "2026-11-24"^^xsd:date ) .
        :a :empty () .
        :a :slots ( 1 undef 3 ) .
        :a :allSlots ( undef undef ) .
        :a :nested ( ( 1 2 ) ( 1 2 ) ( ) ( undef ) ) .
        :a :prefix ( 1 2 ) .
        _:b :label "outside" .
        :a :bnode ( _:b _:c _:b ) .
        :a :tt ( <<( :s :p "o" )>> 7 ) .
        :a :tt2 <<( :s :p ( ) )>> .
        :a :plain "just a literal" .
        :g { :a :list ( 1 2 3 ) . :a :inGraph ( :iri ) . }
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static File dest;

    @BeforeAll
    static void build() throws Exception {
        // :tt2 holds a sequence inside a triple term, which is refused; the
        // fixture without it is the one stored.
        Path src = dir.resolve("seq.trig");
        Files.writeString(src, fixture());
        dest = dir.resolve("seq.h5").toFile();
        WriterEngines.all().filter(e -> e.name().startsWith("method0")).findFirst().orElseThrow()
                .buildStore(src.toFile(), dest);
        bg = BG.getBeakGraph(dest);
    }

    public static String fixture() {
        return TRIG.replace(":a :tt2 <<( :s :p ( ) )>> .\n", "");
    }

    @AfterAll
    static void close() throws Exception {
        if (bg != null) {
            bg.close();
        }
    }

    private static DatasetGraph parsed(String trig) {
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().fromString(trig).lang(Lang.TRIG).parse(dsg);
        return dsg;
    }

    private static DatasetGraph stored(BeakGraph b) {
        DatasetGraph got = DatasetGraphFactory.create();
        b.getDataset().asDatasetGraph().find().forEachRemaining(got::add);
        return got;
    }

    @Test
    void roundTripIsIsomorphic() {
        assertTrue(IsoMatcher.isomorphic(parsed(fixture()), stored(bg)),
                "store must round-trip sequences (slots, empty, nested, blank nodes, triple terms inside)");
    }

    @Test
    void storeStampsFormatSeven() {
        try (HdfFile hdf = new HdfFile(dest.toPath())) {
            Group bgGroup = (Group) hdf.getChild(".BG");
            assertEquals(7, ((Number) bgGroup.getAttribute("formatVersion").getData()).intValue());
        }
    }

    @Test
    void sequenceStoresAndSlotCount() {
        try (HdfFile hdf = new HdfFile(dest.toPath())) {
            Group literals = (Group) hdf.getByPath(".BG/dictionary/literals");
            Dataset starts = (Dataset) literals.getChild("sequenceStarts");
            Dataset elements = (Dataset) literals.getChild("sequenceElements");
            assertTrue(starts != null && elements != null, "both sequence stores are written");
            assertTrue(literals.getChild("sequenceStarts_blocks") != null, "block-structured (§5.1a)");
            // ( 1 undef 3 ), ( undef undef ), ( undef ): four slots.
            assertEquals(4L, ((Number) elements.getAttribute("numSlots").getData()).longValue());
        }
        PositionalDictionaryReader dict = dictionary(bg);
        assertEquals(4L, dict.numSlots());
    }

    @Test
    void sequencesAreTheSuffixAndTripleTermsTheBandBeforeIt() {
        PositionalDictionaryReader dict = dictionary(bg);
        long first = dict.firstSequenceObjectId();
        long last = dict.lastSequenceObjectId();
        assertEquals(dict.getObjects().getNumberOfNodes(), last, "sequences end the object space");
        for (long id = first; id <= last; id++) {
            assertTrue(dict.getObjects().extract(id).isNodeSeq(), "row " + id + " of the suffix is a sequence");
        }
        assertFalse(dict.getObjects().extract(first - 1).isNodeSeq());
        assertEquals(first - 1, dict.lastTripleTermObjectId(), "the triple-term band ends where the sequences begin");
        assertTrue(dict.getObjects().extract(dict.lastTripleTermObjectId()).isTripleTerm());
        // The suffix ascends on element-id arrays, a prefix first (§7.6b).
        for (long id = first; id < last; id++) {
            assertTrue(compareIds(dict.sequenceElementIds(id), dict.sequenceElementIds(id + 1)) < 0,
                    "suffix ascends on ids at " + id);
        }
    }

    @Test
    void locateOnIdsFindsEveryStoredSequence() {
        PositionalDictionaryReader dict = dictionary(bg);
        for (long id = dict.firstSequenceObjectId(); id <= dict.lastSequenceObjectId(); id++) {
            Node lazy = dict.getObjects().extract(id);
            Node whole = SeqTerms.create(new ArrayList<>(lazy.getSeq()));
            assertEquals(id, dict.getObjects().locate(whole), "a materialized copy of " + lazy + " locates");
            assertEquals(lazy, whole, "a lazy row equals its copy");
            assertEquals(lazy.hashCode(), whole.hashCode());
        }
    }

    /** Turtle's {@code 3}: an {@code xsd:integer}, not an {@code xsd:int}. */
    static Node integer(long n) {
        return NodeFactory.createLiteralDT(Long.toString(n), org.apache.jena.datatypes.xsd.XSDDatatype.XSDinteger);
    }

    @Test
    void locateMissesSequencesNotStored() {
        PositionalDictionaryReader dict = dictionary(bg);
        Node one = integer(1);
        Node two = integer(2);
        Node absent = NodeFactory.createLiteralString("not in the store");
        assertEquals(-1, dict.getObjects().locate(SeqTerms.create(List.of(two, one))), "stored elements, not stored order");
        assertEquals(-1, dict.getObjects().locate(SeqTerms.create(List.of(one, absent))), "an element not stored");
        assertEquals(-1, dict.getObjects().locate(SeqTerms.create(List.of(one, SeqTerms.UNDEF))), "a slot not stored there");
        // An insertion point inside the suffix for a sequence between stored ones.
        long search = dict.getObjects().search(SeqTerms.create(List.of(one, two, integer(4))));
        assertTrue(search < 0);
        long insertion = -search - 1;
        assertTrue(insertion >= dict.firstSequenceObjectId() && insertion <= dict.lastSequenceObjectId() + 1);
    }

    @Test
    void sparqlFindsAConcreteSequence() {
        Node a = NodeFactory.createURI(P + "a");
        Node list = NodeFactory.createURI(P + "list");
        Node seq = SeqTerms.create(List.of(integer(1), integer(2), integer(3)));
        DatasetGraph dsg = bg.getDataset().asDatasetGraph();
        List<Quad> found = new ArrayList<>();
        dsg.find(Node.ANY, a, list, seq).forEachRemaining(found::add);
        assertEquals(2, found.size(), "( 1 2 3 ) in the default graph and in :g");
    }

    @Test
    void sparqlMatchesAConcreteSequenceOnIds() {
        String q = "SELECT ?s WHERE { ?s <http://ex.org/list> ( 1 2 3 ) }";
        try (org.apache.jena.query.QueryExecution qe =
                     org.apache.jena.query.QueryExecutionFactory.create(q, bg.getDataset())) {
            assertEquals(1, org.apache.jena.query.ResultSetFormatter.consume(qe.execSelect()), "the default graph's");
        }
        String g = "SELECT ?g WHERE { GRAPH ?g { ?s <http://ex.org/list> ( 1 2 3 ) } }";
        try (org.apache.jena.query.QueryExecution qe =
                     org.apache.jena.query.QueryExecutionFactory.create(g, bg.getDataset())) {
            assertEquals(1, org.apache.jena.query.ResultSetFormatter.consume(qe.execSelect()), ":g's");
        }
    }

    @Test
    void aSequencePatternMatchesOnIdsAndGoesToJenaArraysSolverOnRequest() {
        // RDFARRY.md §3.7: a BGP holding a sequence pattern matches on ids
        // (layer B); with -Dbeakgraph.sequences.delegate=true it is handed
        // whole to jena-array's solver (layer A). A concrete sequence is
        // located on ids either way.
        java.util.concurrent.atomic.AtomicLong delegatedBgps = com.ebremer.beakgraph.hdf5.jena.PatternMatchBG.DELEGATED;
        String q = "SELECT ?x WHERE { ?s <http://ex.org/list> ( ?x 2 3 ) }";
        for (boolean delegate : new boolean[]{false, true}) {
            long delegated = delegatedBgps.get();
            String before = System.getProperty(com.ebremer.beakgraph.hdf5.jena.PatternMatchBG.DELEGATE_PROPERTY);
            System.setProperty(com.ebremer.beakgraph.hdf5.jena.PatternMatchBG.DELEGATE_PROPERTY, Boolean.toString(delegate));
            try (org.apache.jena.query.QueryExecution qe =
                         org.apache.jena.query.QueryExecutionFactory.create(q, bg.getDataset())) {
                org.apache.jena.query.ResultSet rs = qe.execSelect();
                assertEquals(integer(1), rs.next().get("x").asNode(), "the default graph's ( 1 2 3 )");
                assertFalse(rs.hasNext());
            } finally {
                if (before == null) {
                    System.clearProperty(com.ebremer.beakgraph.hdf5.jena.PatternMatchBG.DELEGATE_PROPERTY);
                } else {
                    System.setProperty(com.ebremer.beakgraph.hdf5.jena.PatternMatchBG.DELEGATE_PROPERTY, before);
                }
            }
            assertEquals(delegate, delegatedBgps.get() > delegated, delegate ? "delegated" : "matched on ids");
        }
        long before = delegatedBgps.get();
        String c = "SELECT ?s WHERE { ?s <http://ex.org/list> ( 1 2 3 ) }";
        try (org.apache.jena.query.QueryExecution qe =
                     org.apache.jena.query.QueryExecutionFactory.create(c, bg.getDataset())) {
            assertEquals(1, org.apache.jena.query.ResultSetFormatter.consume(qe.execSelect()));
        }
        assertEquals(before, delegatedBgps.get(), "a constant is not delegated");
        // A pattern is no term: the dictionary answers it with a miss, never by ranking a variable.
        assertEquals(-1, dictionary(bg).getObjects().locate(SeqTerms.create(List.of(
                org.apache.jena.sparql.core.Var.alloc("x"), integer(2)))));
    }

    @Test
    void aSequenceCachedByJenaOutlivesItsStore() throws Exception {
        // Jena's JVM-wide NodeValue cache keeps the lazy sequences a query
        // compares; a later, equal sequence from another store meets the kept
        // one there after its store is closed (RDFARRY.md §6).
        Path src = dir.resolve("outlive.ttl");
        Files.writeString(src, "<http://ex.org/a> <http://ex.org/p> ( 1 2 ) , ( 3 4 ) .\n");
        String q = "SELECT ?o WHERE { ?s ?p ?o } ORDER BY ?o";
        for (int round = 0; round < 2; round++) {
            File out = dir.resolve("outlive" + round + ".h5").toFile();
            method0().buildStore(src.toFile(), out);
            try (BeakGraph b = BG.getBeakGraph(out);
                 org.apache.jena.query.QueryExecution qe =
                         org.apache.jena.query.QueryExecutionFactory.create(q, b.getDataset())) {
                assertEquals(2, org.apache.jena.query.ResultSetFormatter.consume(qe.execSelect()), "round " + round);
            }
        }
    }

    @Test
    void blankNodeCoReferenceInsideSequenceSurvives() {
        DatasetGraph got = stored(bg);
        Node bnodeSeq = got.find(Node.ANY, Node.ANY, NodeFactory.createURI(P + "bnode"), Node.ANY).next().getObject();
        List<Node> el = bnodeSeq.getSeq();
        assertEquals(el.get(0), el.get(2), "_:b twice is one node");
        assertFalse(el.get(0).equals(el.get(1)));
        Node labelled = got.find(Node.ANY, Node.ANY, NodeFactory.createURI(P + "label"), Node.ANY).next().getSubject();
        assertEquals(labelled, el.get(0), "_:b inside the sequence is _:b outside");
    }

    @Test
    void nestedEqualSequencesAreOneTerm() {
        PositionalDictionaryReader dict = dictionary(bg);
        Node nested = stored(bg).find(Node.ANY, Node.ANY, NodeFactory.createURI(P + "nested"), Node.ANY).next().getObject();
        long id = dict.getObjects().locate(nested);
        long[] ids = dict.sequenceElementIds(id);
        assertEquals(4, ids.length);
        assertEquals(ids[0], ids[1], "( 1 2 ) twice is one id");
        assertTrue(dict.isSequenceObjectId(ids[0]) && dict.isSequenceObjectId(ids[2]) && dict.isSequenceObjectId(ids[3]));
    }

    @Test
    void numericCanonicalizationRecursesIntoSequences() throws Exception {
        String two = """
            @prefix : <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :r :num ( "042"^^xsd:int ( "7"^^xsd:int "007"^^xsd:int ) ) .
            :r :num ( "42"^^xsd:int ( "7"^^xsd:int "7"^^xsd:int ) ) .
            """;
        Path src = dir.resolve("canon.ttl");
        Files.writeString(src, two);
        File out = dir.resolve("canon.h5").toFile();
        method0().buildStore(src.toFile(), out);
        try (BeakGraph b2 = BG.getBeakGraph(out)) {
            DatasetGraph got = stored(b2);
            assertEquals(1, got.stream().count(), "lexical variants of one value inside sequences collapse to one term");
            Node o = got.stream().findFirst().orElseThrow().getObject();
            assertEquals("42", o.getSeq().get(0).getLiteralLexicalForm());
        }
    }

    @Test
    void relativeIrisInsideSequencesAreStoredRelative() throws Exception {
        String rel = """
            <s> <p> ( <a.png> ( <../b.png> ) ) .
            """;
        Path src = dir.resolve("rel.ttl");
        Files.writeString(src, rel);
        File out = dir.resolve("rel.h5").toFile();
        method0().buildStore(src.toFile(), out);
        try (BeakGraph b2 = BG.getBeakGraph(out)) {
            PositionalDictionaryReader dict = dictionary(b2);
            Node seq = dict.getObjects().extract(dict.firstSequenceObjectId() + 1);
            assertEquals("a.png", seq.getSeq().get(0).getURI(), "no sentinel inside the sequence: " + seq);
            assertEquals("../b.png", seq.getSeq().get(1).getSeq().get(0).getURI());
        }
    }

    @Test
    void shuffledInputBuildsTheSameBytes() throws Exception {
        List<String> lines = new ArrayList<>(List.of(
            "<http://ex.org/a> <http://ex.org/p> ( 1 2 3 ) .",
            "<http://ex.org/a> <http://ex.org/p> ( 1 undef ( 2 ) ) .",
            "<http://ex.org/a> <http://ex.org/q> ( ) .",
            "<http://ex.org/b> <http://ex.org/p> ( \"x\" <http://ex.org/i> ) .",
            "<http://ex.org/b> <http://ex.org/q> ( ( 1 2 ) ( 1 ) ) .",
            "<http://ex.org/c> <http://ex.org/p> ( 1 2 ) ."));
        byte[] first = null;
        for (long seed = 0; seed < 3; seed++) {
            Collections.shuffle(lines, new java.util.Random(seed));
            Path src = dir.resolve("shuffle" + seed + ".ttl");
            Files.writeString(src, String.join("\n", lines) + "\n");
            File out = dir.resolve("shuffle" + seed + ".h5").toFile();
            method0().buildStore(src.toFile(), out);
            byte[] bytes = Files.readAllBytes(out.toPath());
            if (first == null) {
                first = bytes;
            } else {
                assertTrue(java.util.Arrays.equals(first, bytes), "shuffle " + seed + " built different bytes");
            }
        }
    }

    // ------------------------------------------------------------- the guards

    @Test
    void sequenceSubjectIsRefused() {
        assertRefused("( 1 2 ) <http://ex.org/p> <http://ex.org/o> .", "subject");
    }

    @Test
    void sequenceInsideTripleTermIsRefused() {
        // The fork's parsers refuse one first ("Bad object in triple term");
        // the build fails either way.
        assertThrows(Exception.class, () -> {
            Path src = Files.createTempFile(dir, "guard", ".ttl");
            Files.writeString(src, "<http://ex.org/a> <http://ex.org/p> <<( <http://ex.org/s> <http://ex.org/p> ( 1 ) )>> .\n");
            method0().buildStore(src.toFile(), dir.resolve(src.getFileName() + ".h5").toFile());
        });
    }

    @Test
    void nestingPastTheParsersBoundIsRefused() throws Exception {
        // The fork's parsers stop at 500 levels, below SeqTerms.MAX_DEPTH; a
        // deeper term reaches a build only programmatically (SeqTermsGuardTest).
        int depth = SeqTerms.MAX_DEPTH + 1;
        StringBuilder sb = new StringBuilder("<http://ex.org/a> <http://ex.org/p> ");
        sb.append("( ".repeat(depth)).append(") ".repeat(depth)).append(".\n");
        Path src = dir.resolve("deep.ttl");
        Files.writeString(src, sb.toString());
        File out = dir.resolve("deep.h5").toFile();
        assertThrows(Exception.class, () -> method0().buildStore(src.toFile(), out));
    }

    @Test
    void theParallelEngineBuildsTheSameBytes() throws Exception {
        // Method 2 shares method 0's parse, and writes sequences as it does.
        Path src = dir.resolve("parallel.trig");
        Files.writeString(src, fixture());
        File m0 = dir.resolve("parallel-m0.h5").toFile();
        File m2 = dir.resolve("parallel-m2.h5").toFile();
        method0().buildStore(src.toFile(), m0);
        WriterEngines.all().filter(e -> e.name().startsWith("method2")).findFirst().orElseThrow()
                .buildStore(src.toFile(), m2);
        assertTrue(java.util.Arrays.equals(Files.readAllBytes(m0.toPath()), Files.readAllBytes(m2.toPath())),
                "method 0 and method 2 must build identical stores");
    }

    @Test
    void everyEngineStoresSequences() throws Exception {
        // RDFARRY.md step 5: all six engines, the disk ones through HugeSequences.
        Path src = dir.resolve("engines.trig");
        Files.writeString(src, fixture());
        DatasetGraph expected = parsed(fixture());
        for (WriterEngines.Engine e : WriterEngines.all().toList()) {
            File out = dir.resolve("engines-" + e.name() + ".h5").toFile();
            e.buildStore(src.toFile(), out);
            try (BeakGraph b = BG.getBeakGraph(out)) {
                assertTrue(IsoMatcher.isomorphic(expected, stored(b)), e.name() + " round trip");
                assertEquals(4L, dictionary(b).numSlots(), e.name() + " numSlots");
            }
        }
    }

    @Test
    void theDiskEnginesRefuseWhatMethodZeroRefuses() throws Exception {
        // The guards are one walk (SeqTerms.walk) for every engine.
        for (WriterEngines.Engine e : WriterEngines.all().filter(WriterEngines.Engine::diskBased).toList()) {
            Path src = dir.resolve("subject-" + e.name() + ".ttl");
            Files.writeString(src, "( 1 ) <http://ex.org/p> <http://ex.org/o> .\n");
            File out = dir.resolve("subject-" + e.name() + ".h5").toFile();
            Exception ex = assertThrows(Exception.class, () -> e.buildStore(src.toFile(), out), e.name());
            assertTrue(chain(ex).contains("Unexpected") && chain(ex).contains("only as an object"), e.name() + ": " + chain(ex));
        }
    }

    private static void assertRefused(String turtle, String expect) {
        Exception ex = assertThrows(Exception.class, () -> {
            Path src = Files.createTempFile(dir, "guard", ".ttl");
            Files.writeString(src, turtle + "\n");
            method0().buildStore(src.toFile(), dir.resolve(src.getFileName() + ".h5").toFile());
        });
        String all = chain(ex);
        assertTrue(all.contains("Unexpected") && all.contains(expect), all);
    }

    // ---------------------------------------------------------------- helpers

    static WriterEngines.Engine method0() {
        return WriterEngines.all().filter(e -> e.name().startsWith("method0")).findFirst().orElseThrow();
    }

    static PositionalDictionaryReader dictionary(BeakGraph b) {
        return (PositionalDictionaryReader) b.getReader().getDictionary();
    }

    private static int compareIds(long[] a, long[] b) {
        for (int i = 0, n = Math.min(a.length, b.length); i < n; i++) {
            if (a[i] != b[i]) {
                return Long.compare(a[i], b[i]);
            }
        }
        return Integer.compare(a.length, b.length);
    }

    private static String chain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage()).append(" | ");
        }
        return sb.toString();
    }
}
