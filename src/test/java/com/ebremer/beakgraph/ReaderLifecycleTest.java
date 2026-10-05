package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for the reader lifecycle fixes:
 * <ul>
 *   <li>H5 - opening a structurally valid HDF5 file that is not a BeakGraph
 *       file must fail with a clear exception AND release the underlying file
 *       mapping (a leaked HdfFile pins the file lock on Windows).</li>
 *   <li>H6 - closing a named-graph wrapper obtained from the dataset must not
 *       close the shared reader under the rest of the dataset, and the keyed
 *       pool must not re-issue an instance whose reader has been closed.</li>
 * </ul>
 */
class ReaderLifecycleTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:s ex:p ex:o .
        ex:s ex:p "v" .
        """;
    // (the rebuild test appends a triple to a copy of this source)

    @TempDir
    static Path dir;
    static File h5;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("life.ttl").toFile();
        h5 = dir.resolve("life.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
    }

    @AfterAll
    static void drainPool() {
        BeakGraphPool.getPool().clear(h5.toURI());
    }

    private static int count(Dataset ds) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("SELECT * WHERE { ?s ?p ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    // --- H5 ---------------------------------------------------------------

    @Test
    void malformedFileIsRejectedAndReleased() throws Exception {
        Path notBG = dir.resolve("notbeakgraph.h5");
        try (WritableHdfFile out = HdfFile.write(notBG)) {
            out.putGroup("SomethingElse").putAttribute("hello", 1);
        }
        assertThrows(IllegalStateException.class, () -> new HDF5Reader(notBG.toFile()),
            "a non-BeakGraph HDF5 file must be rejected explicitly");
        // Rejection happens at the .BG group lookup, before any dataset is
        // mapped, so nothing here observes the mapping-release contract; the
        // channel overload below does (BG-449), portably, and the lifetime test
        // covers the real open-query-close-delete sequence.
        Files.delete(notBG);
    }

    /** A channel that records whether the reader closed it. */
    private static final class ObservedChannel implements SeekableByteChannel {
        private final FileChannel delegate;
        volatile boolean closed;

        ObservedChannel(FileChannel delegate) { this.delegate = delegate; }
        @Override public int read(ByteBuffer dst) throws IOException { return delegate.read(dst); }
        @Override public int write(ByteBuffer src) throws IOException { return delegate.write(src); }
        @Override public long position() throws IOException { return delegate.position(); }
        @Override public SeekableByteChannel position(long p) throws IOException { delegate.position(p); return this; }
        @Override public long size() throws IOException { return delegate.size(); }
        @Override public SeekableByteChannel truncate(long s) throws IOException { delegate.truncate(s); return this; }
        @Override public boolean isOpen() { return delegate.isOpen(); }
        @Override public void close() throws IOException { closed = true; delegate.close(); }
    }

    @Test
    void failedConstructionReleasesTheCallersChannelOnEveryOs() throws Exception {
        Path notBG = dir.resolve("notbeakgraph-channel.h5");
        try (WritableHdfFile out = HdfFile.write(notBG)) {
            out.putGroup("SomethingElse").putAttribute("hello", 1);
        }
        ObservedChannel channel = new ObservedChannel(FileChannel.open(notBG, StandardOpenOption.READ));
        assertThrows(IllegalStateException.class, () -> new HDF5Reader(channel, notBG.toUri()));
        assertTrue(channel.closed, "the reader owns the channel it was given and must close it on failure");
        assertFalse(channel.isOpen());

        // Same contract for a healthy open followed by close().
        ObservedChannel good = new ObservedChannel(FileChannel.open(h5.toPath(), StandardOpenOption.READ));
        HDF5Reader reader = new HDF5Reader(good, h5.toURI());
        assertTrue(reader.isOpen());
        reader.close();
        assertTrue(good.closed, "close() must release the channel");

        if (System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win")) {
            // Windows-only observable: a leaked read handle makes an exclusive
            // open a sharing violation. NIO's own opens use FILE_SHARE_DELETE, so
            // Files.delete would NOT have noticed a leak.
            try (FileChannel exclusive = FileChannel.open(notBG, StandardOpenOption.READ,
                    com.sun.nio.file.ExtendedOpenOption.NOSHARE_READ)) {
                assertTrue(exclusive.isOpen());
            }
        }
        Files.delete(notBG);
    }

    /**
     * The scenario that bites the pool and the W3C suites: a store that was
     * opened, queried through BOTH indexes and closed is deleted and rebuilt
     * in-process. With jHDF 0.13 the reader's close() releases the file, so
     * the delete succeeds on Windows too and the case is enforced everywhere.
     * Should a future jHDF keep a mapping alive until GC (Windows refuses to
     * delete a mapped file), the case aborts with that reason on Windows
     * rather than failing, matching the documented expectation there
     * (INSTRUCTIONS.md, "Replacing a served store").
     */
    @Test
    void deleteAndRebuildAfterASuccessfulOpenQueryClose() throws Exception {
        Path src = dir.resolve("rebuild.ttl");
        Files.writeString(src, TTL);
        File dest = dir.resolve("rebuild.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest)
                .setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(dest))) {
            Dataset ds = bg.getDataset();
            assertEquals(2, count(ds));                                       // GSPO
            assertEquals(1, countRows(ds, "SELECT * WHERE { ?s ?p <http://ex.org/o> }"));   // GPOS
        }
        try {
            Files.delete(dest.toPath());
        } catch (IOException stillMapped) {
            boolean windows = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win");
            org.junit.jupiter.api.Assumptions.assumeTrue(!windows,
                    "documented Windows expectation: jHDF keeps the closed store's mapping until GC (" + stillMapped + ")");
            throw stillMapped;
        }
        assertFalse(dest.exists());
        Files.writeString(src, TTL + "ex:s ex:p ex:o2 .\n");
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest)
                .setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(dest))) {
            assertEquals(3, count(bg.getDataset()), "the rebuilt store must be the new one");
        }
    }

    private static int countRows(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    // --- H6: named-graph wrapper close ------------------------------------

    @Test
    void closingNamedGraphWrapperDoesNotKillDataset() throws Exception {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            // Routine Jena usage: obtain a named model, read it, close it - BEFORE
            // anything else has touched the file, so a close that wrongly cascades
            // to the shared reader cannot hide behind already-cached index buffers.
            ds.getNamedModel(Params.VOIDSTRING).close();
            assertTrue(count(ds) > 0,
                "closing a named-graph view must not close the shared reader");
            assertTrue(bg.getReader().isOpen(), "shared reader must remain open");
        }
    }

    // --- H6: pool must not re-issue a closed instance -----------------------

    @Test
    void poolDoesNotReissueClosedInstance() throws Exception {
        URI key = h5.toURI();
        BeakGraph first = BeakGraphPool.getPool().borrowObject(key);
        assertNotNull(first);
        // Poison the instance the way a buggy/abusive caller would, then return it.
        first.close();
        BeakGraphPool.getPool().returnObject(key, first);

        BeakGraph second = BeakGraphPool.getPool().borrowObject(key);
        try {
            // With testOnBorrow active and a working validateObject, the poisoned
            // instance is destroyed and a fresh one is created - so this works.
            assertTrue(count(second.getDataset()) > 0,
                "borrowed instance must be usable after a poisoned return");
        } finally {
            BeakGraphPool.getPool().returnObject(key, second);
        }
    }
}
