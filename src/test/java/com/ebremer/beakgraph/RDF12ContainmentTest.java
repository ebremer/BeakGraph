package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.MultiTypeDictionaryWriter;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import com.ebremer.beakgraph.huge.NativeHdf5File;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * RDF 1.2 containment: Jena 6.x parses base-direction literals ("x"@en--ltr,
 * rdf:dirLangString) by default, but the BeakGraph format has nowhere to store
 * the direction - before the ingest guards, such a term was silently rewritten
 * to "x"@en (a DIFFERENT RDF term; three distinct source terms collapsed onto
 * one). These tests pin the containment stance: a source containing a
 * base-direction literal must fail the build loudly, in every writer engine,
 * and must not publish a store.
 *
 * <p>When real rdf:dirLangString storage lands (a langDirs column beside
 * langs/langTags), the rejection guards are removed and these tests are
 * REPLACED by term-exact round-trip tests - do not delete them without that
 * replacement.
 */
class RDF12ContainmentTest {

    private static final String BAD_TTL = """
        @prefix : <http://ex.org/> .
        :s :p "x"@en--ltr .
        :s :q "plain"@en .
        """;

    private static final String GOOD_TTL = """
        @prefix : <http://ex.org/> .
        :s :q "plain"@en .
        :s :r "untagged" .
        """;

    private static final String GUARD_MESSAGE = "base direction cannot be stored";

    @TempDir
    static Path dir;

    @FunctionalInterface
    interface Engine {
        BeakGraphWriter create(File src, File dest) throws Exception;
    }

    static Stream<Arguments> engines() {
        return Stream.of(
            Arguments.of("method0-HDF5Writer", false, (Engine) (src, dest) ->
                HDF5Writer.Builder().setSource(src).setDestination(dest).build()),
            Arguments.of("method1-Huge", true, (Engine) (src, dest) -> {
                HugeHDF5Writer.Builder b = HugeHDF5Writer.Builder().setDestination(dest);
                b.setSource(src);
                return b.build();
            }),
            Arguments.of("method2-Parallel", false, (Engine) (src, dest) -> {
                ParallelHDF5Writer.Builder b = ParallelHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            }),
            Arguments.of("method3-Ultra", false, (Engine) (src, dest) -> {
                UltraHDF5Writer.Builder b = UltraHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            }),
            Arguments.of("method4-HugeUltra", true, (Engine) (src, dest) -> {
                HugeUltraHDF5Writer.Builder b = HugeUltraHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            }),
            Arguments.of("method5-Plaid", true, (Engine) (src, dest) -> {
                PlaidHDF5Writer.Builder b = PlaidHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            })
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void baseDirectionLiteralAbortsTheBuild(String name, boolean needsNative, Engine engine) throws Exception {
        Assumptions.assumeTrue(!needsNative || NativeHdf5File.isAvailable(),
                "native HDF5 library unavailable");
        Path src = dir.resolve(name + "-bad.ttl");
        Files.writeString(src, BAD_TTL);
        File dest = dir.resolve(name + "-bad.h5").toFile();

        Exception ex = assertThrows(Exception.class,
                () -> engine.create(src.toFile(), dest).write());
        assertTrue(chainMentionsGuard(ex),
                "build failed, but not on the base-direction guard: " + ex);
        assertNoUsableStore(dest);
    }

    @Test
    void plainLanguageTagsStillBuildAndRoundTrip() throws Exception {
        Path src = dir.resolve("good.ttl");
        Files.writeString(src, GOOD_TTL);
        File dest = dir.resolve("good.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).build().write();
        assertControlRoundTrips(dest);
    }

    @Test
    void plainLanguageTagsStillBuildAndRoundTripUltra() throws Exception {
        Path src = dir.resolve("good-ultra.ttl");
        Files.writeString(src, GOOD_TTL);
        File dest = dir.resolve("good-ultra.h5").toFile();
        UltraHDF5Writer.Builder b = UltraHDF5Writer.Builder().setDestination(dest).setCores(2);
        b.setSource(src.toFile());
        b.build().write();
        assertControlRoundTrips(dest);
    }

    /** The dictionary encoder's own guard, driven directly (bypasses the ingest guards). */
    @Test
    void multiTypeDictionaryWriterRejectsBaseDirectionLiteral() {
        Stats stats = new Stats();
        stats.numStrings = 1;
        stats.longestStringLength = 5;
        stats.shortestStringLength = 5;
        Node bad = NodeFactory.createLiteralDirLang("hello", "en", "ltr");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            new MultiTypeDictionaryWriter.Builder()
                .enable(Types.STRING, Types.INTEGER, Types.LONG, Types.FLOAT, Types.DOUBLE)
                .setStats(stats)
                .setNodes(new HashSet<>(Set.of(bad)))
                .setDataTypes(Set.of(RDF.dirLangString.getURI()))
                .setName("literals")
                .build());
        assertTrue(ex.getMessage().contains(GUARD_MESSAGE), ex.getMessage());
    }

    // ---- helpers ----

    private static boolean chainMentionsGuard(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(GUARD_MESSAGE)) {
                return true;
            }
        }
        return false;
    }

    /**
     * "Not silently store": the aborted build must not publish a readable store.
     * Writers build into a sibling *.tmp and publish by atomic rename, so the
     * destination normally does not exist; if some engine left a file behind,
     * it must at least not open as a BeakGraph.
     */
    private static void assertNoUsableStore(File dest) {
        if (!dest.exists()) {
            return;
        }
        assertThrows(Exception.class, () -> {
            try (BeakGraph bg = BG.getBeakGraph(dest)) {
                bg.getDataset();
            }
        }, "a store was published from a build that should have aborted");
    }

    private static void assertControlRoundTrips(File dest) throws Exception {
        Node expected = NodeFactory.createLiteralLang("plain", "en");
        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            Dataset ds = bg.getDataset();
            List<Node> objects = ds.getDefaultModel()
                    .listObjectsOfProperty(ds.getDefaultModel()
                            .createProperty("http://ex.org/q"))
                    .mapWith(o -> o.asNode()).toList();
            assertEquals(1, objects.size());
            assertEquals(expected, objects.get(0));
            assertNull(objects.get(0).getLiteralBaseDirection(),
                    "a plain lang tag must carry no base direction");
        }
    }
}
