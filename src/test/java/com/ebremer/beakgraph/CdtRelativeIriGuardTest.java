package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-395: a document-relative IRI inside a composite (cdt:) literal's
 * lexical form is stored verbatim - neither relativized at ingest nor
 * resolved when served - so {@code cdt:get} re-parses it against the
 * server's working directory while the same reference outside the literal
 * is served under the store's URL: one resource, two IRIs, and a
 * {@code file:} path leaked to clients. Such literals are rejected at ingest
 * by every engine, like blank nodes inside composites.
 */
class CdtRelativeIriGuardTest {

    private static final String GUARD_MESSAGE = "relative IRI inside cdt: composite literal";
    private static final String BAD_TTL = """
        @prefix : <http://ex.org/> .
        @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
        <> :gallery "[<a.png>, <b.png>]"^^cdt:List .
        <a.png> a :ImageObject .
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
        assertTrue(found, "build failed, but not on the relative-IRI-in-composite guard: " + ex);
    }

    @FunctionalInterface
    interface Executable {
        void run() throws Exception;
    }

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void everyEngineRejectsARelativeIriInsideAList(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path src = ttl(engine.name() + "-bad.ttl", BAD_TTL);
        File dest = dir.resolve(engine.name() + "-bad.h5").toFile();
        assertGuardFires(() -> engine.buildStore(src.toFile(), dest));
        assertFalse(dest.exists(), "a rejected build must leave no store behind");
    }

    @Test
    void nestedListsMapValuesAndMapKeysAreAlsoRejected() throws Exception {
        String[][] cases = {
            {"nested", ":s :list \"[[<a.png>]]\"^^cdt:List ."},
            {"map-value", ":s :map \"{\\\"k\\\": <a.png>}\"^^cdt:Map ."},
            {"map-key", ":s :map \"{<a.png>: 1}\"^^cdt:Map ."},
            {"empty-ref", ":s :list \"[<>]\"^^cdt:List ."},
            {"parent", ":s :list \"[<../up.png>]\"^^cdt:List ."},
        };
        for (String[] c : cases) {
            Path src = ttl(c[0] + "-bad.ttl", """
                @prefix : <http://ex.org/> .
                @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
                """ + c[1] + "\n");
            assertGuardFires(() -> HDF5Writer.Builder().setSource(src.toFile())
                    .setDestination(dir.resolve(c[0] + "-bad.h5").toFile()).build().write());
        }
    }

    @Test
    void absoluteIrisInsideCompositesStillBuild() throws Exception {
        Path src = ttl("good.ttl", """
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            <> :gallery "[<http://ex.org/a.png>, 1, \\"<b.png>\\"]"^^cdt:List .
            :s :map "{\\"k\\": <urn:x:1>}"^^cdt:Map .
            """);
        File dest = dir.resolve("good.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).build().write();
        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            assertTrue(bg.find().toList().size() == 2);
        }
    }
}
