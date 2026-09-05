package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.graph.Node;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;

/**
 * Blank nodes inside composite (cdt:) literals are rejected at ingest: the
 * SPARQL-CDT spec (section 5.2) requires a blank node label inside a composite
 * literal to co-refer with the same label outside it, but BeakGraph
 * regenerates blank-node labels from dictionary rank, so the label in the
 * literal's text would silently stop naming anything in the graph (verified
 * pre-guard: the outside bnode read back as _:b00000000000000000002 while the
 * literal still said _:b1).
 *
 * <p>One rejection test per writer engine (methods 0-5: ProcessQuad,
 * HugeBuildPipeline.collectLiteralStats, the parallel/ultra/hugeUltra/plaid
 * ingests that inherit or sink into them). Controls pin the boundaries: composite literals without blank
 * nodes build; "_:x" inside a quoted string element is a string, not a blank
 * node (the detector parses, it does not substring-match); ill-formed
 * composite literals stay allowed and round-trip as opaque terms.
 */
class CdtBlankNodeGuardTest {

    private static final String GUARD_MESSAGE = "blank node inside cdt: composite literal";

    private static final String BAD_TTL = """
        @prefix : <http://ex.org/> .
        @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
        :s :list "[_:b1]"^^cdt:List .
        _:b1 :label "co-referenced outside" .
        """;

    @TempDir
    static Path dir;

    private static Path ttl(String name, String content) throws Exception {
        Path p = dir.resolve(name);
        Files.writeString(p, content);
        return p;
    }

    private static void assertGuardFires(Executable build) {
        Exception ex = assertThrows(Exception.class, build::run);
        boolean found = false;
        for (Throwable c = ex; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(GUARD_MESSAGE)) {
                found = true;
                break;
            }
        }
        assertTrue(found, "build failed, but not on the blank-node-in-composite guard: " + ex);
    }

    @FunctionalInterface
    interface Executable {
        void run() throws Exception;
    }

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    /** BG-182: one rejection test per ENGINE, not per ingest hierarchy - inheritance is not coverage. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void everyEngineRejectsBlankNodeInsideList(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path src = ttl(engine.name() + "-bad.ttl", BAD_TTL);
        File dest = dir.resolve(engine.name() + "-bad.h5").toFile();
        assertGuardFires(() -> engine.buildStore(src.toFile(), dest));
        assertFalse(dest.exists(), "a rejected build must leave no store behind");
    }

    @Test
    void nestedAndMapBlankNodesAreAlsoRejected() throws Exception {
        Path nested = ttl("nested-bad.ttl", """
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            :s :list "[[_:b1]]"^^cdt:List .
            """);
        assertGuardFires(() -> HDF5Writer.Builder()
            .setSource(nested.toFile())
            .setDestination(dir.resolve("nested-bad.h5").toFile()).build().write());

        Path mapValue = ttl("map-bad.ttl", """
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            :s :map "{\\"k\\": _:b1}"^^cdt:Map .
            """);
        assertGuardFires(() -> HDF5Writer.Builder()
            .setSource(mapValue.toFile())
            .setDestination(dir.resolve("map-bad.h5").toFile()).build().write());

        Path deep = ttl("deep-bad.ttl", """
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            :s :list "[{\\"k\\": [_:deep]}]"^^cdt:List .
            """);
        assertGuardFires(() -> HDF5Writer.Builder()
            .setSource(deep.toFile())
            .setDestination(dir.resolve("deep-bad.h5").toFile()).build().write());
    }

    @Test
    void boundariesStayOpen() throws Exception {
        // No blank node; and "_:x" inside a QUOTED STRING element (a string, not
        // a bnode - rejecting it would mean the detector substring-matches
        // instead of parsing). Both must build and round-trip.
        Path src = ttl("good.ttl", """
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            :s :plain  "[1, 2]"^^cdt:List .
            :s :quoted "[\\"_:x\\"]"^^cdt:List .
            """);
        File dest = dir.resolve("good.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).build().write();

        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            java.util.Set<String> stored = new java.util.TreeSet<>();
            bg.getDataset().getDefaultModel().listStatements().forEachRemaining(st -> {
                Node n = st.getObject().asNode();
                if (n.isLiteral()) {
                    stored.add(n.getLiteralLexicalForm());
                }
            });
            assertEquals(new java.util.TreeSet<>(java.util.Set.of(
                    "[1, 2]", "[\"_:x\"]")), stored);
        }
    }

    /**
     * Not a BeakGraph guard, but a boundary this suite depends on: RIOT's
     * CDT-aware parser profile validates composite lexical forms, so an
     * ILL-FORMED composite literal never reaches BeakGraph's ingest from a
     * parsed source - the parse itself fails. (Programmatically-built
     * ill-formed nodes are still possible; NodeComparatorCdtTest covers their
     * ordering.) If a Jena upgrade relaxes this, the blank-node detector's
     * ill-formed branch becomes reachable from documents and this suite should
     * grow a corresponding ingest test.
     */
    @Test
    void illFormedCompositeIsRejectedByTheParser() throws Exception {
        Path src = ttl("illformed.ttl", """
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            :s :illformed "[_:b1"^^cdt:List .
            """);
        File dest = dir.resolve("illformed.h5").toFile();
        Exception ex = assertThrows(Exception.class, () ->
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).build().write());
        boolean parserRejected = false;
        for (Throwable c = ex; c != null; c = c.getCause()) {
            if (c instanceof org.apache.jena.datatypes.DatatypeFormatException) {
                parserRejected = true;
                break;
            }
        }
        assertTrue(parserRejected, "expected RIOT's CDT validation to reject the parse: " + ex);
    }
}
