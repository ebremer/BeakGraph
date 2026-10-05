package com.ebremer.beakgraph.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;
import org.apache.jena.riot.Lang;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;

/**
 * BG-425: JSON-LD is the one accepted syntax Jena cannot stream (Titanium
 * loads and expands the whole document), so the disk-based engines' bounded
 * RAM promise does not cover it; RdfSources knows which syntaxes stream, maps
 * a file name to its syntax the way {@link RdfSources#open} will, and warns
 * loudly for a materialized source.
 * <p>
 * Also the {@link RdfSources#open} contract: a rejected {@code .gz} releases
 * its file handle (BG-24), and a zip's document is chosen by name over the
 * whole archive rather than taken first-come (BG-25).
 */
class RdfSourcesStreamingTest {

    @TempDir
    Path dir;

    private static String readAll(RdfSources.OpenedSource opened) throws IOException {
        try (opened) {
            return new String(opened.stream().readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void zip(Path p, String... nameThenContent) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(p))) {
            for (int i = 0; i < nameThenContent.length; i += 2) {
                zos.putNextEntry(new ZipEntry(nameThenContent[i]));
                if (!nameThenContent[i].endsWith("/")) {
                    zos.write(nameThenContent[i + 1].getBytes(StandardCharsets.UTF_8));
                }
                zos.closeEntry();
            }
        }
    }

    @Test
    void aRejectedGzipReleasesItsFileHandle() throws IOException {
        Path bad = dir.resolve("bad.nt.gz");
        Files.writeString(bad, "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o> .\n");
        assertThrows(ZipException.class, () -> RdfSources.open(bad.toFile()), "not a gzip stream");
        // On Windows an open handle makes this delete fail - the leaked FileInputStream.
        Files.delete(bad);
        assertFalse(Files.exists(bad));
    }

    @Test
    void aGoodGzipIsStreamedAndClosed() throws IOException {
        Path gz = dir.resolve("good.nq.gz");
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(gz))) {
            out.write("<http://ex.org/s> <http://ex.org/p> <http://ex.org/o> <http://ex.org/g> .\n".getBytes(StandardCharsets.UTF_8));
        }
        RdfSources.OpenedSource opened = RdfSources.open(gz.toFile());
        assertEquals(Lang.NQUADS, opened.lang());
        assertTrue(readAll(opened).contains("<http://ex.org/g>"));
        Files.delete(gz);
    }

    @Test
    void zipMetadataEntriesAreSkippedAndTheRdfNamedEntryWins() throws IOException {
        Path z = dir.resolve("doc.ttl.zip");
        zip(z,
                "__MACOSX/", "",
                "__MACOSX/._x.ttl", "resource fork garbage",
                ".DS_Store", "finder garbage",
                "README.txt", "not the document",
                "x.ttl", "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o-x> .\n");
        RdfSources.OpenedSource opened = RdfSources.open(z.toFile());
        assertEquals(Lang.TURTLE, opened.lang());
        assertEquals("<http://ex.org/s> <http://ex.org/p> <http://ex.org/o-x> .\n", readAll(opened));
        Files.delete(z);   // closing the entry stream closed the archive
    }

    @Test
    void aZipsSingleUnnamedEntryTakesTheArchivesSyntax() throws IOException {
        Path z = dir.resolve("data.nq.zip");
        zip(z, "payload", "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o> <http://ex.org/g> .\n");
        RdfSources.OpenedSource opened = RdfSources.open(z.toFile());
        assertEquals(Lang.NQUADS, opened.lang(), "no RDF-named entry: the archive name decides");
        assertTrue(readAll(opened).contains("<http://ex.org/g>"));
    }

    @Test
    void aZipWithSeveralOrNoCandidatesIsRejected() throws IOException {
        Path two = dir.resolve("two.ttl.zip");
        zip(two, "a.ttl", "<http://ex.org/s> <http://ex.org/p> <http://ex.org/a> .\n",
                "b.ttl", "<http://ex.org/s> <http://ex.org/p> <http://ex.org/b> .\n");
        IOException ex = assertThrows(IOException.class, () -> RdfSources.open(two.toFile()));
        assertTrue(ex.getMessage().contains("exactly one RDF document"), ex.getMessage());
        assertTrue(ex.getMessage().contains("a.ttl") && ex.getMessage().contains("b.ttl"), ex.getMessage());
        Files.delete(two);

        Path none = dir.resolve("none.ttl.zip");
        zip(none, "__MACOSX/._x.ttl", "fork", "sub/", "");
        ex = assertThrows(IOException.class, () -> RdfSources.open(none.toFile()));
        assertTrue(ex.getMessage().contains("found none"), ex.getMessage());
        Files.delete(none);
    }

    @Test
    void onlyJsonLdIsMaterialized() {
        for (Lang streaming : List.of(Lang.TURTLE, Lang.NTRIPLES, Lang.NQUADS, Lang.TRIG, Lang.RDFXML)) {
            assertTrue(RdfSources.isStreaming(streaming), streaming.getName());
        }
        assertFalse(RdfSources.isStreaming(Lang.JSONLD));
        assertTrue(RdfSources.isStreaming(null));
    }

    @Test
    void langOfFollowsTheOpenRules() {
        assertEquals(Lang.JSONLD, RdfSources.langOf(new File("data.jsonld")));
        assertEquals(Lang.JSONLD, RdfSources.langOf(new File("data.JSONLD.GZ")));
        assertEquals(Lang.JSONLD, RdfSources.langOf(new File("data.jsonld.zip")));
        assertEquals(Lang.NQUADS, RdfSources.langOf(new File("data.nq.gz")));
        assertEquals(Lang.TURTLE, RdfSources.langOf(new File("data.ttl")));
        assertEquals(Lang.TURTLE, RdfSources.langOf(new File("data.unknown")), "the quad store's fallback is Turtle");
    }

    private static Logger recording(List<String> sink) {
        return (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class<?>[]{Logger.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("warn") && args != null && args.length > 0) {
                        StringBuilder sb = new StringBuilder(String.valueOf(args[0]));
                        for (int i = 1; i < args.length; i++) {
                            if (args[i] instanceof Object[] arr) {
                                for (Object o : arr) sb.append(' ').append(o);
                            } else {
                                sb.append(' ').append(args[i]);
                            }
                        }
                        sink.add(sb.toString());
                        return null;
                    }
                    if (method.getReturnType() == boolean.class) return false;
                    if (method.getReturnType() == String.class) return "recording";
                    return null;
                });
    }

    @Test
    void materializedSourcesAreWarnedAbout() {
        List<String> warnings = new ArrayList<>();
        Logger log = recording(warnings);
        RdfSources.warnIfMaterialized(new File("huge.nq.gz"), Lang.NQUADS, log);
        assertTrue(warnings.isEmpty(), "streaming syntaxes are silent");
        RdfSources.warnIfMaterialized(new File("huge.jsonld.gz"), Lang.JSONLD, log);
        assertEquals(1, warnings.size());
        String w = warnings.get(0);
        assertTrue(w.contains("no streaming parser"), w);
        assertTrue(w.contains("huge.jsonld.gz"), w);
        assertTrue(w.contains("NOT bounded"), w);
        assertTrue(w.contains("riot --output=nq"), w);
        assertTrue(w.contains("compressed"), w);
    }
}
