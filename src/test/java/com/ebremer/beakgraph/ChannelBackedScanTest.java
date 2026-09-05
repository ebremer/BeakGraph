package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.hdf5.jena.ParallelScan;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-247: a store read through an HTTP range channel serializes every read
 * behind the channel's lock and its block cache, so a chunked parallel scan
 * gains nothing there and only contends. The same scan that parallelizes on
 * the local file must stay sequential over the channel - with identical rows.
 */
class ChannelBackedScanTest {

    private static final String NS = "http://ex.org/";
    private static final int SUBJECTS = 400;

    @TempDir
    static Path dir;
    static File h5;
    static byte[] h5Bytes;
    static String oldThreshold;
    static String oldMinChunk;

    @BeforeAll
    static void build() throws Exception {
        oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
        StringBuilder ttl = new StringBuilder("@prefix ex: <" + NS + "> .\n");
        for (int i = 0; i < SUBJECTS; i++) {
            ttl.append("ex:s").append(i).append(" ex:value ").append(i).append(" .\n");
        }
        File src = dir.resolve("chan.ttl").toFile();
        h5 = dir.resolve("chan.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        h5Bytes = Files.readAllBytes(h5.toPath());
    }

    @AfterAll
    static void restore() {
        if (oldThreshold == null) System.clearProperty("beakgraph.scan.parallel.threshold");
        else System.setProperty("beakgraph.scan.parallel.threshold", oldThreshold);
        if (oldMinChunk == null) System.clearProperty("beakgraph.scan.parallel.minchunk");
        else System.setProperty("beakgraph.scan.parallel.minchunk", oldMinChunk);
    }

    private static Set<String> rows(BeakGraph bg) {
        Set<String> out = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                .query(QueryFactory.create("SELECT ?s ?v WHERE { ?s <" + NS + "value> ?v }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                var row = rs.next();
                out.add(row.get("s") + "=" + row.get("v"));
            }
        }
        return out;
    }

    @Test
    void channelBackedStoresScanSequentiallyWithTheSameRows() throws Exception {
        Set<String> local;
        try (BeakGraph bg = BG.getBeakGraph(h5)) {
            assertFalse(((HDF5Reader) bg.getReader()).isChannelBacked());
            long before = ParallelScan.HITS.get();
            local = rows(bg);
            assertEquals(SUBJECTS, local.size());
            assertTrue(ParallelScan.HITS.get() - before >= 1, "the local file parallelizes at this threshold");
        }
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(h5Bytes)) {
            HTTPSeekableByteChannel channel = new HTTPSeekableByteChannel(server.uri("/chan.ttl.h5"));
            try (BeakGraph remote = BG.getBeakGraph(channel)) {
                assertTrue(((HDF5Reader) remote.getReader()).isChannelBacked());
                long before = ParallelScan.HITS.get();
                assertEquals(local, rows(remote));
                assertEquals(0, ParallelScan.HITS.get() - before,
                        "a channel-backed store must not be chunked: every read is serialized behind one lock");
            }
        }
    }
}
