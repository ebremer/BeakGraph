package com.ebremer.beakgraph.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.StandardCopyOption;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.pool2.PooledObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pool validation must actually probe the reader (testOnBorrow is on): an
 * isOpen-only check re-issued a poisoned instance to every borrower forever,
 * since a reader whose backing data went bad still reports "open".
 */
class BeakGraphPoolValidationTest {

    @TempDir
    Path dir;

    @Test
    void validateObjectProbesRealDataAndRejectsClosedReaders() throws Exception {
        File ttl = dir.resolve("pool.ttl").toFile();
        File h5 = dir.resolve("pool.ttl.h5").toFile();
        Files.write(ttl.toPath(),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        BeakGraphPoolFactory factory = new BeakGraphPoolFactory();
        BeakGraph bg = factory.create(h5.toURI());
        PooledObject<BeakGraph> pooled = factory.wrap(bg);
        try {
            assertTrue(factory.validateObject(h5.toURI(), pooled),
                    "a healthy instance must validate (probe included)");
        } finally {
            bg.close();
        }
        assertFalse(factory.validateObject(h5.toURI(), pooled),
                "a closed instance must fail validation and be discarded");
    }

    private File build(String name, String ttl) throws Exception {
        File src = dir.resolve(name + ".ttl").toFile();
        File h5 = dir.resolve(name + ".h5").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        return h5;
    }

    private static int count(BeakGraph bg) {
        try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                .query(QueryFactory.create("SELECT * WHERE { ?s ?p ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    // --- BG-416: NIO-form keys, as the servlet mints them ---------------------

    @Test
    void nioFileUriKeysCreateReaders() throws Exception {
        // LWSStorageServlet keys the pool with Path.toUri(), not File.toURI().
        File h5 = build("nio", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        BeakGraphPoolFactory factory = new BeakGraphPoolFactory();
        BeakGraph bg = factory.create(h5.toPath().toUri());
        try {
            assertEquals(1, count(bg));
        } finally {
            bg.close();
        }
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void uncAuthorityFormUriMapsToTheUncPath() {
        // Path.toUri() of \\server\share\x.h5 is file://server/share/x.h5; java.io.File
        // rejects any URI with an authority, so every query on a share 500ed.
        assertEquals(Path.of("\\\\server\\share\\x.h5"),
                BeakGraphPoolFactory.toPath(URI.create("file://server/share/x.h5")));
    }

    // --- BG-404: a replaced file is detected on the next borrow -------------

    /** Puts {@code replacement} at {@code target}; on Windows the mapped target must be renamed away first. */
    private static void replace(Path target, Path replacement, Path parked) throws IOException {
        try {
            Files.move(replacement, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException mappedTargetBlocksReplace) {
            Files.move(target, parked, StandardCopyOption.ATOMIC_MOVE);
            Files.move(replacement, target, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    @Test
    void replacedStoreFailsValidationSoTheNextBorrowReopens() throws Exception {
        File a = build("store", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        File b = build("store-next", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n<http://ex.org/c> <http://ex.org/p> <http://ex.org/d> .\n");
        Path parked = dir.resolve("store-old.h5");
        BeakGraphPoolFactory factory = new BeakGraphPoolFactory();
        URI key = a.toPath().toUri();
        BeakGraph opened = factory.create(key);
        PooledObject<BeakGraph> pooled = factory.wrap(opened);
        try {
            assertTrue(factory.validateObject(key, pooled), "fresh instance validates");
            assertEquals(1, count(opened));

            replace(a.toPath(), b.toPath(), parked);

            // The old mapping still reads the old data (the probe alone passed),
            // but the file behind the key is a different one now.
            assertEquals(1, count(opened), "the stale mapping still serves the old data");
            assertFalse(factory.validateObject(key, pooled),
                    "a replaced file must fail validation so the instance is discarded");
        } finally {
            opened.close();
        }
        BeakGraph reopened = factory.create(key);
        try {
            assertEquals(2, count(reopened), "the next reader opens the replacement");
            assertTrue(factory.validateObject(key, factory.wrap(reopened)));
        } finally {
            reopened.close();
        }
    }

    @Test
    void deletedStoreFailsValidation() throws Exception {
        File a = build("gone", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        BeakGraphPoolFactory factory = new BeakGraphPoolFactory();
        URI key = a.toPath().toUri();
        BeakGraph opened = factory.create(key);
        PooledObject<BeakGraph> pooled = factory.wrap(opened);
        try {
            Path parked = dir.resolve("gone-parked.h5");
            Files.move(a.toPath(), parked, StandardCopyOption.ATOMIC_MOVE);   // rename works even while mapped
            assertFalse(factory.validateObject(key, pooled), "a missing file must fail validation");
        } finally {
            opened.close();
        }
    }

    @Test
    void poolReopensAReplacedStore() throws Exception {
        File a = build("pooled", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        File b = build("pooled-next", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n<http://ex.org/c> <http://ex.org/p> <http://ex.org/d> .\n");
        URI key = a.toPath().toUri();
        BeakGraphKeyedPool pool = BeakGraphPool.getPool();
        try {
            BeakGraph first = pool.borrowObject(key);
            assertEquals(1, count(first));
            pool.returnObject(key, first);
            long created = pool.getCreatedCount();

            replace(a.toPath(), b.toPath(), dir.resolve("pooled-old.h5"));

            BeakGraph second = pool.borrowObject(key);
            try {
                assertEquals(2, count(second), "borrow after replacement must see the new store");
                assertNotEquals(created, pool.getCreatedCount(), "the stale instance was discarded and a new one created");
            } finally {
                pool.returnObject(key, second);
            }
        } finally {
            pool.clear(key);
        }
    }
}
