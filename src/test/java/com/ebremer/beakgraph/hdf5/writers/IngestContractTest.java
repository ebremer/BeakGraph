package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Two ingest contracts of the in-memory writers.
 * <ul>
 *   <li>BG-295: the predicate position is guarded at ingest, with the same
 *       message shape as the graph/subject/object guards. Every accepted
 *       syntax's parser already refuses a non-IRI predicate (probed: Turtle,
 *       N-Triples, N-Quads and TriG error out, JSON-LD drops the property),
 *       so the guard is exercised directly.</li>
 *   <li>BG-98: a parse failure or a guard keeps its own message at the top
 *       of the cause chain; only a source that cannot be OPENED is reported
 *       as an I/O error.</li>
 * </ul>
 */
class IngestContractTest {

    @TempDir
    Path dir;

    @Test
    void thePredicateGuardRejectsEveryNonIriKind() {
        PositionalDictionaryWriterBuilder.requirePredicate(NodeFactory.createURI("http://ex.org/p"));
        for (var bad : List.of(NodeFactory.createBlankNode("p"), NodeFactory.createLiteralString("p"),
                NodeFactory.createTripleTerm(org.apache.jena.graph.Triple.create(
                        NodeFactory.createURI("http://a"), NodeFactory.createURI("http://b"), NodeFactory.createURI("http://c"))))) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> PositionalDictionaryWriterBuilder.requirePredicate(bad), bad.toString());
            assertTrue(ex.getMessage().startsWith("Unexpected predicate node type (not URI)"), ex.getMessage());
        }
    }

    private static List<String> messages(Throwable t) {
        List<String> out = new ArrayList<>();
        for (Throwable c = t; c != null; c = c.getCause()) {
            out.add(String.valueOf(c.getMessage()));
        }
        return out;
    }

    private void assertParseFailureKeepsItsMessage(Throwable thrown) {
        List<String> chain = messages(thrown);
        String first = chain.stream().filter(m -> m.startsWith("Failed while parsing/processing RDF source")).findFirst().orElse(null);
        assertNotNull(first, "the parse failure names itself: " + chain);
        assertFalse(chain.stream().anyMatch(m -> m.contains("I/O error while reading RDF source")),
                "a parse failure is not re-wrapped as an I/O error: " + chain);
    }

    @Test
    void aParseFailureIsNotReportedAsAnIoError() throws Exception {
        File bad = dir.resolve("bad.ttl").toFile();
        Files.writeString(bad.toPath(), "this is not turtle @@@ }}}", StandardCharsets.UTF_8);
        assertParseFailureKeepsItsMessage(assertThrows(Exception.class, () ->
                HDF5Writer.Builder().setSource(bad).setDestination(dir.resolve("m0.h5").toFile()).build().write()));
        assertParseFailureKeepsItsMessage(assertThrows(Exception.class, () ->
                UltraHDF5Writer.Builder().setSource(bad).setDestination(dir.resolve("m3.h5").toFile()).setCores(1).build().write()));
    }

    @Test
    void aSourceThatCannotBeOpenedIsReportedAsSuch() {
        File missing = dir.resolve("missing.ttl").toFile();
        Exception ex = assertThrows(Exception.class, () ->
                HDF5Writer.Builder().setSource(missing).setDestination(dir.resolve("m0.h5").toFile()).build().write());
        assertTrue(messages(ex).stream().anyMatch(m -> m.startsWith("Source file not found")), messages(ex).toString());
    }
}
