package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * isInTransaction() must report REAL per-thread state. The old implementation
 * unconditionally answered true - a contract lie that masked mispaired
 * begin/end in callers (a skipped begin, an end without begin, and a nested
 * begin all passed silently).
 */
class TransactionStateTest {

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static DatasetGraph dsg;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("txn.ttl").toFile();
        File h5 = dir.resolve("txn.ttl.h5").toFile();
        Files.write(ttl.toPath(),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        dsg = bg.getDataset().asDatasetGraph();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    @Test
    void transactionStateFollowsTheLifecycle() {
        assertFalse(dsg.isInTransaction(), "no transaction has begun");
        dsg.begin(ReadWrite.READ);
        assertTrue(dsg.isInTransaction());
        assertEquals(ReadWrite.READ, dsg.transactionMode());
        dsg.end();
        assertFalse(dsg.isInTransaction(), "end() must leave the transaction");

        dsg.begin(ReadWrite.READ);
        dsg.commit();
        dsg.end();
        assertFalse(dsg.isInTransaction());
    }

    @Test
    void txnExecuteReadWorks() {
        long[] count = {0};
        Txn.executeRead(dsg, () ->
                dsg.getDefaultGraph().find().forEachRemaining(t -> count[0]++));
        assertTrue(count[0] >= 1);
        assertFalse(dsg.isInTransaction());
    }

    @Test
    void writeTransactionsAreRejected() {
        assertThrows(UnsupportedOperationException.class, () -> dsg.begin(ReadWrite.WRITE));
        assertFalse(dsg.isInTransaction(), "a rejected begin must not leave txn state behind");
    }

    @Test
    void promotableReadsAreReads() {
        // BG-3: the standard promotable-read entry points used to throw from
        // TransactionalLock before any read ran; promote() already said false.
        for (org.apache.jena.query.TxnType t : new org.apache.jena.query.TxnType[]{
                org.apache.jena.query.TxnType.READ_PROMOTE, org.apache.jena.query.TxnType.READ_COMMITTED_PROMOTE,
                org.apache.jena.query.TxnType.READ}) {
            dsg.begin(t);
            assertTrue(dsg.isInTransaction(), t.toString());
            assertFalse(dsg.promote(), "an immutable store never promotes");
            assertEquals(org.apache.jena.query.TxnType.READ, dsg.transactionType(), "a promotable read runs as a read");
            assertEquals(ReadWrite.READ, dsg.transactionMode());
            dsg.end();
            assertFalse(dsg.isInTransaction());
        }
        dsg.begin();   // the no-arg default is READ_PROMOTE
        assertTrue(dsg.isInTransaction());
        dsg.end();
        long n = Txn.calculate(dsg, () -> dsg.stream().count());   // Txn defaults to READ_PROMOTE
        assertTrue(n > 0);
        Txn.execute(dsg, () -> assertTrue(dsg.isInTransaction()));
        assertFalse(dsg.isInTransaction());
        assertThrows(UnsupportedOperationException.class, () -> dsg.begin(org.apache.jena.query.TxnType.WRITE));
    }
}
