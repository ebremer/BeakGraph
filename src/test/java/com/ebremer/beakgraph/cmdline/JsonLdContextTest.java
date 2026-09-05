package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.utils.JsonLdContexts;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-426 / BG-424: a JSON-LD document's {@code @context} references. A
 * relative reference ({@code "context.jsonld"} next to the document) used to
 * resolve against the sentinel base and be fetched from the reserved
 * {@code .invalid} host - it could never load, on any engine; an absolute
 * http(s) reference was fetched with no timeout and no opt-out, and a
 * {@code file:} reference read any local file. Contexts now load from the
 * source tree; remote ones only with {@code -jsonLdRemote}.
 */
@Timeout(120)
class JsonLdContextTest {

    private static final String CONTEXT = """
        {"@context": {"name": "http://ex.org/name", "ex": "http://ex.org/"}}
        """;

    @TempDir
    static Path dir;

    private static Path write(Path p, String content) throws Exception {
        Files.createDirectories(p.getParent());
        Files.writeString(p, content);
        return p;
    }

    private static boolean ask(File h5, String query) throws Exception {
        try (BeakGraph bg = BG.getBeakGraph(h5);
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(QueryFactory.create(query)).build()) {
            return qe.execAsk();
        }
    }

    private static String chainMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getMessage()).append(" | ");
        }
        return sb.toString();
    }

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void aSiblingContextLoadsFromTheSourceTree(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path src = dir.resolve("sib-" + engine.name());
        write(src.resolve("context.jsonld"), CONTEXT);
        Path data = write(src.resolve("data.jsonld"), """
            {"@context": "context.jsonld", "@id": "http://ex.org/item1", "name": "x"}
            """);
        File h5 = dir.resolve("sib-" + engine.name() + ".h5").toFile();
        engine.buildStore(data.toFile(), h5);
        assertTrue(ask(h5, "ASK { <http://ex.org/item1> <http://ex.org/name> \"x\" }"));
    }

    @Test
    void mergedDocumentsFindTheirContextsBelowTheMergeRoot() throws Exception {
        Path src = dir.resolve("merge");
        write(src.resolve("a/context.jsonld"), CONTEXT);
        Path a = write(src.resolve("a/data.jsonld"), """
            {"@context": "context.jsonld", "@id": "http://ex.org/a1", "name": "from a"}
            """);
        write(src.resolve("shared/ctx.jsonld"), CONTEXT);
        Path b = write(src.resolve("b/data.jsonld"), """
            {"@context": "../shared/ctx.jsonld", "@id": "http://ex.org/b1", "name": "from b"}
            """);
        Path c = write(src.resolve("c.ttl"), "<http://ex.org/c1> <http://ex.org/name> \"from c\" .\n");
        File h5 = dir.resolve("merge.h5").toFile();
        HDF5Writer.Builder().setSources(List.of(a.toFile(), b.toFile(), c.toFile())).setSourceRoot(src.toFile())
                .setDestination(h5).build().write();
        assertTrue(ask(h5, "ASK { <http://ex.org/a1> <http://ex.org/name> \"from a\" }"));
        assertTrue(ask(h5, "ASK { <http://ex.org/b1> <http://ex.org/name> \"from b\" }"), "../shared/ctx.jsonld stays inside the merge root");
        assertTrue(ask(h5, "ASK { <http://ex.org/c1> <http://ex.org/name> \"from c\" }"));
    }

    @Test
    void aContextAboveTheSourceTreeIsRefused() throws Exception {
        write(dir.resolve("outside.jsonld"), CONTEXT);
        Path src = dir.resolve("above");
        Path data = write(src.resolve("data.jsonld"), """
            {"@context": "../outside.jsonld", "@id": "http://ex.org/item1", "name": "x"}
            """);
        Exception ex = assertThrows(Exception.class, () -> HDF5Writer.Builder().setSource(data.toFile())
                .setDestination(dir.resolve("above.h5").toFile()).build().write());
        String chain = chainMessages(ex);
        assertTrue(chain.contains("outside the source tree"), chain);
        assertFalse(dir.resolve("above.h5").toFile().exists());
    }

    @Test
    void aFileContextOutsideTheSourceTreeIsRefused() throws Exception {
        Path outside = write(dir.resolve("elsewhere/secret.json"), CONTEXT);
        Path src = dir.resolve("filectx");
        Path data = write(src.resolve("data.jsonld"), """
            {"@context": "%s", "@id": "http://ex.org/item1", "name": "x"}
            """.formatted(outside.toUri().toString()));
        Exception ex = assertThrows(Exception.class, () -> HDF5Writer.Builder().setSource(data.toFile())
                .setDestination(dir.resolve("filectx.h5").toFile()).build().write());
        String chain = chainMessages(ex);
        assertTrue(chain.contains("outside the source tree"), chain);
    }

    @Test
    void remoteContextsAreOffByDefaultAndOnWithTheProperty() throws Exception {
        String iri = "http://127.0.0.1:1/ctx.jsonld"; // port 1: refused at once, nothing listens
        Path src = dir.resolve("remote");
        Path data = write(src.resolve("data.jsonld"), """
            {"@context": "%s", "@id": "http://ex.org/item1", "name": "x"}
            """.formatted(iri));
        String previous = System.getProperty(JsonLdContexts.REMOTE_PROPERTY);
        try {
            System.clearProperty(JsonLdContexts.REMOTE_PROPERTY);
            Exception off = assertThrows(Exception.class, () -> HDF5Writer.Builder().setSource(data.toFile())
                    .setDestination(dir.resolve("remote-off.h5").toFile()).build().write());
            String chain = chainMessages(off);
            assertTrue(chain.contains("remote contexts are disabled") && chain.contains(iri) && chain.contains("-jsonLdRemote"), chain);

            System.setProperty(JsonLdContexts.REMOTE_PROPERTY, "true");
            Exception on = assertThrows(Exception.class, () -> HDF5Writer.Builder().setSource(data.toFile())
                    .setDestination(dir.resolve("remote-on.h5").toFile()).build().write());
            chain = chainMessages(on);
            assertFalse(chain.contains("remote contexts are disabled"), "with the property the fetch is attempted: " + chain);
            assertTrue(chain.contains("127.0.0.1") || chain.contains("ctx.jsonld"), "the failure names the context: " + chain);
        } finally {
            if (previous == null) System.clearProperty(JsonLdContexts.REMOTE_PROPERTY);
            else System.setProperty(JsonLdContexts.REMOTE_PROPERTY, previous);
        }
    }
}
