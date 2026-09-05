package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The dictionary contracts the review found bent: the insertion point an
 * absent section answers (BG-312, BG-351), graph membership over the stored
 * list (BG-252), and the literal search with memoised tier values and packed
 * numeric probes (BG-244) - every stored term found, every miss at the same
 * insertion point a plain NodeComparator scan gives, under concurrency.
 */
class DictionaryContractTest {

    @TempDir
    static Path dir;
    static File iriOnly;
    static File graphs;
    static File manyLiterals;

    @BeforeAll
    static void build() throws Exception {
        iriOnly = build("iri.ttl", """
            @prefix ex: <http://ex.org/> .
            ex:s1 ex:p ex:o1 , ex:o2 . ex:s2 ex:p ex:o3 . ex:s3 ex:q ex:s1 .
            """);
        graphs = build("graphs.trig", """
            @prefix ex: <http://ex.org/> .
            ex:s ex:p ex:o .
            ex:g1 { ex:s ex:p ex:o1 . }
            ex:g2 { ex:s ex:p ex:g1 . }
            ex:g3 { ex:g2 ex:p ex:o3 . }
            """);
        StringBuilder sb = new StringBuilder("@prefix ex: <http://ex.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
        for (int i = 0; i < 4000; i++) sb.append("ex:s").append(i).append(" ex:int ").append(i * 3).append(" .\n");
        for (int i = 0; i < 1500; i++) sb.append("ex:s").append(i).append(" ex:dec ").append(i).append(".5 .\n");
        for (int i = 0; i < 1000; i++) sb.append("ex:s").append(i).append(" ex:dbl ").append(i).append(".25e0 .\n");
        for (int i = 0; i < 500; i++) sb.append("ex:s").append(i).append(" ex:lng \"").append(1L << 40 + (i % 20)).append("\"^^xsd:long .\n");
        for (int i = 0; i < 500; i++) sb.append("ex:s").append(i).append(" ex:when \"2020-01-01T00:00:").append(String.format("%02d", i % 60)).append("Z\"^^xsd:dateTime .\n");
        for (int i = 0; i < 500; i++) sb.append("ex:s").append(i).append(" ex:str \"str").append(String.format("%04d", i)).append("\" .\n");
        for (int i = 0; i < 300; i++) sb.append("ex:s").append(i).append(" ex:lang \"lang").append(i).append("\"@en .\n");
        manyLiterals = build("many.ttl", sb.toString());
    }

    private static File build(String name, String text) throws Exception {
        Path src = dir.resolve(name);
        Files.writeString(src, text);
        File h5 = dir.resolve(name + ".h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        return h5;
    }

    // --- BG-312 / BG-351 ---------------------------------------------------

    @Test
    void anAbsentLiteralsSectionAnswersTheInsertionPointAfterTheEntities() throws Exception {
        try (HDF5Reader reader = new HDF5Reader(iriOnly)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            long entities = dict.getSubjects().getNumberOfNodes();
            assertTrue(entities >= 6);
            Node five = NodeFactory.createLiteralByValue(5);
            assertEquals(-(entities + 1) - 1, dict.getObjects().search(five),
                    "a literal ranks after every entity: insertion point maxEntityId + 1");
            assertEquals(-(entities + 1) - 1, dict.getObjects().search(NodeFactory.createLiteralDT("x", XSDDatatype.XSDstring)));
            assertEquals(-1, dict.getObjects().locate(five));
            assertEquals(entities, dict.getObjects().getNumberOfNodes());
        }
        assertEquals(-2, Dictionary.EMPTY.search(NodeFactory.createURI("http://ex.org/x")), "the empty dictionary's only insertion point is 1");
        assertEquals(-1, Dictionary.EMPTY.locate(NodeFactory.createURI("http://ex.org/x")));
        assertFalse(Dictionary.EMPTY.hasFloatLiterals());
    }

    // --- BG-252 ------------------------------------------------------------

    @Test
    void graphMembershipIsAnsweredExactlyFromTheStoredList() throws Exception {
        try (HDF5Reader reader = new HDF5Reader(graphs)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            Set<Long> ids = new TreeSet<>();
            dict.streamGraphIds().forEach(ids::add);
            assertEquals(4, ids.size(), "default graph plus three named graphs");
            long n = dict.getSubjects().getNumberOfNodes();
            for (long id = 1; id <= n; id++) {
                assertEquals(ids.contains(id), dict.isGraph(id), "id " + id);
            }
            assertFalse(dict.isGraph(0));
            assertFalse(dict.isGraph(n + 1));
            assertFalse(dict.isGraph(-1));
            assertTrue(reader.containsGraph(NodeFactory.createURI("http://ex.org/g2")));
            assertFalse(reader.containsGraph(NodeFactory.createURI("http://ex.org/o1")), "an entity that is not a graph");
        }
    }

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void everyEngineStoresTheGraphListInAscendingIdOrder(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path src = dir.resolve(engine.name() + "-graphs.trig");
        Files.copy(dir.resolve("graphs.trig"), src);
        File h5 = dir.resolve(engine.name() + "-graphs.h5").toFile();
        engine.buildStore(src.toFile(), h5);
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            long[] ids = ((PositionalDictionaryReader) reader.getDictionary()).streamGraphIds().toArray();
            assertTrue(ids.length >= 4);
            for (int i = 1; i < ids.length; i++) {
                assertTrue(ids[i - 1] < ids[i], "ascending, strictly: " + ids[i - 1] + " before " + ids[i]);
            }
        }
    }

    // --- BG-244 / BG-215 ---------------------------------------------------

    @Test
    void literalSearchFindsEveryStoredTermAndAgreesWithAPlainScanOnMisses() throws Exception {
        try (HDF5Reader reader = new HDF5Reader(manyLiterals)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            Dictionary objects = dict.getObjects();
            long n = objects.getNumberOfNodes();
            long entities = dict.getSubjects().getNumberOfNodes();
            assertTrue(n - entities > 1024 * 3, "the literals section is large enough for the sampled tier: " + (n - entities));
            Node[] all = objects.streamNodes().toArray(Node[]::new);
            assertEquals(n, all.length);
            for (long id = 1; id <= n; id++) {
                assertEquals(id, objects.locate(all[(int) id - 1]), "id " + id + " " + all[(int) id - 1]);
            }
            List<Node> probes = List.of(
                    NodeFactory.createLiteralDT("2.75", XSDDatatype.XSDdecimal),
                    NodeFactory.createLiteralByValue(-1),
                    NodeFactory.createLiteralByValue(999_999),
                    NodeFactory.createLiteralByValue(4.125),
                    NodeFactory.createLiteralByValue(1L << 40),
                    NodeFactory.createLiteralDT("2020-01-01T00:00:30.5Z", XSDDatatype.XSDdateTime),
                    NodeFactory.createLiteralDT("2019-12-31T23:59:59Z", XSDDatatype.XSDdateTime),
                    NodeFactory.createLiteralString("str0100a"),
                    NodeFactory.createLiteralString("zzz"),
                    NodeFactory.createLiteralLang("lang5", "de"),
                    NodeFactory.createURI("http://ex.org/nowhere"));
            for (Node probe : probes) {
                long expected = -1;
                for (int i = 0; i < all.length; i++) {
                    int c = NodeComparator.INSTANCE.compare(all[i], probe);
                    if (c == 0) { expected = i + 1; break; }
                    if (c > 0) { expected = -(i + 1) - 1; break; }
                }
                if (expected == -1) expected = -(n + 1) - 1;
                assertEquals(expected, objects.search(probe), "probe " + probe);
            }
            ExecutorService pool = Executors.newFixedThreadPool(8);
            try {
                List<Future<Integer>> tasks = new ArrayList<>();
                for (int t = 0; t < 8; t++) {
                    int seed = t;
                    tasks.add(pool.submit(() -> {
                        int ok = 0;
                        for (int k = 0; k < 400; k++) {
                            long id = 1 + (seed * 997L + k * 7919L) % n;
                            if (objects.locate(all[(int) id - 1]) == id) ok++;
                        }
                        return ok;
                    }));
                }
                for (Future<Integer> f : tasks) assertEquals(400, f.get());
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
