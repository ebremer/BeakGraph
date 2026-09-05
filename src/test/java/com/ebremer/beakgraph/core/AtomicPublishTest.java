package com.ebremer.beakgraph.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-415: every writer published its finished build
 * with a rename inside the same catch that deletes a FAILED build's temp
 * file, so a destination that could not be replaced - on Windows one that a
 * reader has memory-mapped refuses the move - destroyed a completed store.
 * Publishing now lives outside the build's cleanup, retries briefly, and on
 * failure keeps the build as {@code <dest>.new} and says so.
 */
@Timeout(120)
class AtomicPublishTest {

    @TempDir
    Path dir;

    private static boolean windows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    private File build(String name, String ttl, File dest) throws Exception {
        File src = dir.resolve(name + ".ttl").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(src).setDestination(dest).setSpatial(false).setFeatures(false).build().write();
        return dest;
    }

    private static int count(File h5) {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5));
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(QueryFactory.create("SELECT * WHERE { ?s ?p ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void publishReplacesAFreeDestination() throws Exception {
        Path dest = dir.resolve("free.h5");
        Files.writeString(dest, "old", StandardCharsets.UTF_8);
        Path tmp = dir.resolve("free.h5.tmp");
        Files.writeString(tmp, "new", StandardCharsets.UTF_8);
        AtomicPublish.publish(tmp, dest);
        assertEquals("new", Files.readString(dest));
        assertFalse(Files.exists(tmp));
    }

    @Test
    void rebuildOverAMappedStoreKeepsTheFinishedBuild() throws Exception {
        File dest = dir.resolve("store.h5").toFile();
        build("one", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n", dest);
        HDF5Reader mapped = new HDF5Reader(dest);
        mapped.getDictionary().getPredicates().extract(1);   // force the mapping
        try {
            String two = "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n<http://ex.org/c> <http://ex.org/p> <http://ex.org/d> .\n";
            File retained = dir.resolve("store.h5.new").toFile();
            if (windows()) {
                // The mapped destination refuses the move: the build must survive.
                IOException ex = assertThrows(IOException.class, () -> build("two", two, dest));
                assertTrue(ex.getMessage().contains(retained.getName()), ex.getMessage());
                assertTrue(ex.getMessage().contains("KEPT"), ex.getMessage());
                assertTrue(retained.isFile(), "the finished build must be kept as <dest>.new");
                try (var files = Files.list(dir)) { assertTrue(files.noneMatch(f -> f.getFileName().toString().endsWith(".tmp")), "no stray temp file"); }
                assertEquals(2, count(retained), "the retained file is the complete new store");
                assertEquals(1, count(dest), "the old store is untouched");
            } else {
                // Rename-over works while mapped: the new store is published and
                // the mapped reader keeps its old inode.
                build("two", two, dest);
                assertEquals(2, count(dest));
                assertFalse(retained.exists());
            }
        } finally {
            mapped.close();
        }
    }

    @Test
    void aFailedBuildStillCleansItsTempFile() throws Exception {
        File dest = dir.resolve("bad.h5").toFile();
        File src = dir.resolve("bad.ttl").toFile();
        Files.writeString(src.toPath(), "this is not turtle <<<\n", StandardCharsets.UTF_8);
        assertThrows(Exception.class, () ->
                HDF5Writer.Builder().setSource(src).setDestination(dest).setSpatial(false).setFeatures(false).build().write());
        try (var files = Files.list(dir)) { assertTrue(files.noneMatch(f -> f.getFileName().toString().endsWith(".tmp")), "a failed BUILD still removes its temp file"); }
        assertFalse(dir.resolve("bad.h5.new").toFile().exists());
        assertFalse(dest.exists());
    }
}
