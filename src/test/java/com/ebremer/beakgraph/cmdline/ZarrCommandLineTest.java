package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.BeakGraphFiles;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Zarr container from the command line: {@code -zarr} and a
 * {@code -dest} named {@code *.zarr} write it, and every command that takes
 * a store takes a Zarr store's directory as one - without walking into it.
 */
class ZarrCommandLineTest {

    @TempDir
    Path dir;

    private static final String A = "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n";
    private static final String B = "<http://ex.org/c> <http://ex.org/p> \"text\" .\n<http://ex.org/c> <http://ex.org/q> <http://ex.org/d> .\n";

    private static boolean isZarrStore(Path p) {
        return Files.isDirectory(p) && Files.isRegularFile(p.resolve("zarr.json")) && Files.isDirectory(p.resolve(".BG"));
    }

    private static long quads(Path store) throws Exception {
        try (BeakGraph bg = BG.getBeakGraph(store)) {
            return StoreParity.count(bg.getDataset(), "SELECT (COUNT(*) AS ?n) WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }");
        }
    }

    private Path sources() throws Exception {
        Path src = Files.createDirectories(dir.resolve("src/sub"));
        Files.writeString(src.getParent().resolve("a.ttl"), A, StandardCharsets.UTF_8);
        Files.writeString(src.resolve("b.nt"), B, StandardCharsets.UTF_8);
        return src.getParent();
    }

    @Test
    void perFileModeWritesAZarrStorePerSource() throws Exception {
        Path src = sources();
        Path out = dir.resolve("out");
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", out.toString(), "-zarr"}));
        assertTrue(isZarrStore(out.resolve("a.zarr")));
        assertTrue(isZarrStore(out.resolve("sub/b.zarr")));
        assertFalse(Files.exists(out.resolve("a.h5")));
        assertEquals(1, quads(out.resolve("a.zarr")));
        assertEquals(2, quads(out.resolve("sub/b.zarr")));

        // A second run leaves the stores that are there alone; -force rebuilds them.
        Path marker = out.resolve("a.zarr/zarr.json");
        byte[] before = Files.readAllBytes(marker);
        Files.writeString(src.resolve("a.ttl"), A + B, StandardCharsets.UTF_8);
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", out.toString(), "-zarr"}));
        assertEquals(1, quads(out.resolve("a.zarr")), "skipped: the store was there");
        assertEquals(new String(before, StandardCharsets.UTF_8), Files.readString(marker));
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", out.toString(), "-zarr", "-force"}));
        assertEquals(3, quads(out.resolve("a.zarr")), "rebuilt in place");

        // The same tree without -zarr: HDF5 files beside the Zarr stores, and a source scan that does not walk into a store.
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", out.toString()}));
        assertTrue(Files.isRegularFile(out.resolve("a.h5")));
        Parameters p = new Parameters();
        p.src = out.toFile();
        FileCounter fc = new FileCounter();
        assertEquals(List.of(), BeakGraphCLI.scanSources(out, fc), "a store's own files are not sources");
        assertEquals(2, fc.getDirectoryCount(), "out and out/sub: the two stores' directories are not walked");
    }

    @Test
    void aDestinationNamedZarrIsAZarrStore() throws Exception {
        Path src = sources();
        // One source, one named output: the name says the container, for every engine.
        for (int method = 0; method <= 5; method++) {
            Path one = dir.resolve("one-" + method + ".zarr");
            assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.resolve("a.ttl").toString(), "-dest", one.toString(),
                "-method", Integer.toString(method)}));
            assertTrue(isZarrStore(one), "method " + method);
            assertEquals(1, quads(one));
        }
        // -merge: the one store, by name; rebuilt where it exists.
        Path merged = dir.resolve("all.zarr");
        for (int round = 0; round < 2; round++) {
            assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", merged.toString(), "-merge"}));
            assertTrue(isZarrStore(merged));
            assertFalse(Files.exists(merged.resolve("merged.h5")), "an existing *.zarr is the store, not a directory to fill");
            assertEquals(3, quads(merged));
        }
        // -merge into a directory: merged.zarr with -zarr, merged.h5 without.
        Path into = dir.resolve("into");
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", into.toString(), "-merge", "-zarr"}));
        assertTrue(isZarrStore(into.resolve("merged.zarr")));
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", into.toString(), "-merge"}));
        assertTrue(Files.isRegularFile(into.resolve("merged.h5")));
        assertEquals(3, quads(into.resolve("merged.h5")));
        // One source into an existing directory: <name>.zarr with -zarr.
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.resolve("a.ttl").toString(), "-dest", into.toString(), "-zarr"}));
        assertTrue(isZarrStore(into.resolve("a.zarr")));
    }

    @Test
    void verifyAndExportTakeAZarrStoreAsOneStore() throws Exception {
        Path src = sources();
        Path out = dir.resolve("stores");
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.toString(), "-dest", out.toString(), "-zarr"}));
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.resolve("a.ttl").toString(), "-dest", out.resolve("c.h5").toString()}));

        assertEquals(List.of(out.resolve("a.zarr"), out.resolve("c.h5"), out.resolve("sub/b.zarr")), BeakGraphFiles.findStores(out));
        assertTrue(BeakGraphFiles.isZarrStoreDirectory(out.resolve("a.zarr")));
        assertFalse(BeakGraphFiles.isZarrStoreDirectory(out.resolve("sub")));
        assertTrue(BeakGraphFiles.storeExists(out.resolve("a.zarr")) && BeakGraphFiles.storeExists(out.resolve("c.h5")));
        assertFalse(BeakGraphFiles.storeExists(out.resolve("sub")) || BeakGraphFiles.storeExists(out.resolve("missing.zarr")));
        assertEquals("a", BeakGraphFiles.stripExtension("a.zarr"));
        assertTrue(BeakGraphFiles.isStoreName("a.ZARR") && BeakGraphFiles.isStoreName("a.h5") && !BeakGraphFiles.isStoreName("a.ttl"));

        // -verify of the tree: three stores, each one line; of one Zarr store: that store.
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
            assertEquals(0, new VerifyCommand(out.toFile(), true, ps).run());
            assertEquals(0, new VerifyCommand(out.resolve("sub/b.zarr").toFile(), true, ps).run());
        }
        String report = captured.toString(StandardCharsets.UTF_8);
        assertTrue(report.contains("OK    " + out.resolve("a.zarr")) && report.contains("OK    " + out.resolve("c.h5"))
                && report.contains("OK    " + out.resolve("sub/b.zarr")), report);
        assertTrue(report.contains("Verified 3 file(s) (deep): 3 OK, 0 FAILED") && report.contains("Verified 1 file(s) (deep): 1 OK"), report);
        assertEquals(0, BeakGraphCLI.run(new String[] {"-verify", out.resolve("a.zarr").toString(), "-deep"}));

        // -export of one Zarr store, and of the tree: the export lands beside each store.
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", out.resolve("sub/b.zarr").toString(), "-export", "NT"}));
        List<String> lines = Files.readAllLines(out.resolve("sub/b.nt"));
        assertEquals(2, lines.size(), lines.toString());
        Files.delete(out.resolve("sub/b.nt"));
        Files.delete(out.resolve("c.h5"));
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", out.toString(), "-export", "NT"}));
        assertEquals(1, Files.readAllLines(out.resolve("a.nt")).size());
        assertEquals(2, Files.readAllLines(out.resolve("sub/b.nt")).size());
    }

    @Test
    void thePoolOpensAZarrStoreAndSeesItReplaced() throws Exception {
        Path src = sources();
        Path store = dir.resolve("pooled.zarr");
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.resolve("a.ttl").toString(), "-dest", store.toString()}));
        URI uri = store.toUri();
        com.ebremer.beakgraph.pool.BeakGraphKeyedPool pool = BeakGraphPool.getPool();
        String count = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }";
        BeakGraph first = pool.borrowObject(uri);
        try {
            assertEquals(1, StoreParity.count(first.getDataset(), count));
        } finally {
            pool.returnObject(uri, first);
        }
        // Rebuilt from another source: a pooled reader of the old store must not answer for the new one.
        pool.clear(uri); // a mapped store cannot be replaced on Windows; the pool lets go of it first
        assertEquals(0, BeakGraphCLI.run(new String[] {"-src", src.resolve("sub/b.nt").toString(), "-dest", store.toString(), "-force"}));
        BeakGraph second = pool.borrowObject(uri);
        try {
            assertEquals(2, StoreParity.count(second.getDataset(), count));
        } finally {
            pool.returnObject(uri, second);
            pool.clear(uri);
        }
    }
}
