package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * BG-246 / BG-380 / BG-225: the HTTP range channel reads ahead on sequential
 * scans, pins the server's validator so an in-place replacement fails loudly
 * instead of mixing two versions, and never lets a URL's query string
 * (a presigned URL's signature) into messages, logs or the graph's identity.
 */
class HttpChannelValidatorAndReadAheadTest {

    private static final int BLOCK = 128 * 1024;

    private static byte[] pattern(int length, int seed) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i * 31 + seed);
        }
        return data;
    }

    private static byte[] readAll(HTTPSeekableByteChannel ch, int chunk) throws IOException {
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        ByteBuffer bb = ByteBuffer.allocate(chunk);
        int n;
        while ((n = ch.read(bb)) != -1) {
            all.write(bb.array(), 0, n);
            bb.clear();
        }
        return all.toByteArray();
    }

    private static byte[] readAt(HTTPSeekableByteChannel ch, long pos, int len) throws IOException {
        ch.position(pos);
        ByteBuffer bb = ByteBuffer.allocate(len);
        while (bb.hasRemaining()) {
            if (ch.read(bb) < 0) break;
        }
        return Arrays.copyOf(bb.array(), bb.position());
    }

    // ---- BG-246 -------------------------------------------------------------

    @Test
    void sequentialScanFetchesBlocksInWindows() throws Exception {
        byte[] data = pattern(24 * BLOCK + 1234, 7);   // 25 blocks
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(data);
             HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/scan.bin"))) {
            assertArrayEquals(data, readAll(ch, 70_000));
            long requests = ch.getRangeRequestCount();
            int readAhead = HTTPSeekableByteChannel.DEFAULT_READ_AHEAD_BLOCKS;
            long worst = 1 + (24 + readAhead - 1) / readAhead + 1;
            assertTrue(requests <= worst, "25 blocks with read-ahead " + readAhead + " took " + requests + " requests");
            assertTrue(requests < 25, "far fewer requests than blocks: " + requests);
            assertEquals(data.length, ch.getBytesFetched(), "every byte fetched exactly once");
            // A multi-block request really was issued.
            assertTrue(server.requests.stream().anyMatch(r -> {
                int dash = r.lastIndexOf('-');
                int eq = r.lastIndexOf('=');
                if (dash < 0 || eq < 0 || eq > dash) return false;
                long from = Long.parseLong(r.substring(eq + 1, dash));
                long to = Long.parseLong(r.substring(dash + 1));
                return to - from + 1 > BLOCK;
            }), "at least one range request spans several blocks: " + server.requests);
        }
    }

    @Test
    void scatteredReadsFetchSingleBlocks() throws Exception {
        byte[] data = pattern(32 * BLOCK, 3);
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(data);
             HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/probe.bin"))) {
            // A binary-search-like pattern: no two consecutive blocks in a row.
            int[] blocks = {16, 8, 24, 4, 20, 12, 28, 2};
            for (int b : blocks) {
                byte[] got = readAt(ch, (long) b * BLOCK + 100, 8);
                assertArrayEquals(Arrays.copyOfRange(data, b * BLOCK + 100, b * BLOCK + 108), got);
            }
            assertEquals(blocks.length, ch.getRangeRequestCount(), "one single-block request per probe");
            assertEquals((long) blocks.length * BLOCK, ch.getBytesFetched());
        }
    }

    // ---- BG-380 -------------------------------------------------------------

    @Test
    void replacementUnderAnOpenChannelIsDetectedByTheValidator() throws Exception {
        byte[] v1 = pattern(6 * BLOCK, 1);
        byte[] v2 = pattern(6 * BLOCK, 2);   // same size: only a validator can tell
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(v1);
             HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/v.bin"))) {
            assertArrayEquals(Arrays.copyOfRange(v1, 0, 16), readAt(ch, 0, 16));
            assertTrue(server.requests.stream().anyMatch(r -> r.startsWith("GET")), server.requests.toString());
            server.replace(v2);
            IOException ex = assertThrows(IOException.class, () -> readAt(ch, 5L * BLOCK, 16), "a block beyond the cache must not be served from v2");
            assertTrue(ex.getMessage().contains("changed on the server"), ex.getMessage());
            // Cached v1 blocks are not served next to v2 either: the channel refuses everything now.
            IOException again = assertThrows(IOException.class, () -> readAt(ch, 0, 16));
            assertTrue(again.getMessage().contains("changed on the server"), again.getMessage());
            assertTrue(ch.isOpen(), "the channel stays open (closing is the owner's job) but refuses reads");
        }
    }

    @Test
    void withoutAValidatorOnlyASizeChangeIsDetected() throws Exception {
        byte[] v1 = pattern(4 * BLOCK, 1);
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(v1)) {
            server.emitEtag = false;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/nv.bin"))) {
                assertArrayEquals(Arrays.copyOfRange(v1, 0, 16), readAt(ch, 0, 16));
                assertFalse(server.requests.stream().anyMatch(r -> r.contains("If-Range")), "no validator, no If-Range");
                server.replace(pattern(4 * BLOCK + 5000, 2));   // grew: the Content-Range total gives it away
                IOException ex = assertThrows(IOException.class, () -> readAt(ch, 3L * BLOCK, 16));
                assertTrue(ex.getMessage().contains("changed on the server"), ex.getMessage());
                assertTrue(ex.getMessage().contains("size"), ex.getMessage());
            }
        }
    }

    // ---- BG-225 -------------------------------------------------------------

    @Test
    void redactionKeepsTheQueryStringOut() {
        URI signed = URI.create("https://bucket.s3.example.com/data/x.h5?X-Amz-Credential=AKIA&X-Amz-Signature=SECRET");
        assertEquals("https://bucket.s3.example.com/data/x.h5?query-redacted", HTTPSeekableByteChannel.redact(signed));
        assertEquals("http://host:8080/a%20b.h5", HTTPSeekableByteChannel.redact(URI.create("http://user:pw@host:8080/a%20b.h5#frag")));
        assertEquals("https://h/", HTTPSeekableByteChannel.redact(URI.create("https://h")));
    }

    @Test
    void failuresAndIdentityNeverCarryTheSignature() throws Exception {
        byte[] data = pattern(3 * BLOCK, 9);
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(data)) {
            URI signed = server.uri("/missing.bin?X-Amz-Signature=SECRET&X-Amz-Security-Token=TOKEN");
            server.notFound = true;
            HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(signed);   // HEAD still answers
            assertEquals(signed, ch.getURI());
            assertFalse(ch.getDisplayURI().toString().contains("SECRET"));
            Throwable failure = assertThrows(Throwable.class, () -> BG.getBeakGraph(ch));
            for (Throwable t = failure; t != null; t = t.getCause()) {
                String m = String.valueOf(t.getMessage());
                assertFalse(m.contains("SECRET") || m.contains("TOKEN"), "leaked: " + m);
            }
            boolean namesThePath = false;
            for (Throwable t = failure; t != null; t = t.getCause()) {
                if (String.valueOf(t.getMessage()).contains("/missing.bin")) namesThePath = true;
            }
            assertTrue(namesThePath, "the path is still named: " + failure);
            assertFalse(ch.isOpen(), "a failed open releases the channel");
        }
    }

    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path dir;

    @Test
    void graphIdentityIsTheRedactedUrl() throws Exception {
        java.io.File ttl = dir.resolve("id.ttl").toFile();
        java.nio.file.Files.writeString(ttl.toPath(), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        java.io.File h5File = dir.resolve("id.h5").toFile();
        com.ebremer.beakgraph.hdf5.writers.HDF5Writer.Builder().setSource(ttl).setDestination(h5File)
                .setSpatial(false).setFeatures(false).build().write();
        byte[] h5 = java.nio.file.Files.readAllBytes(h5File.toPath());
        assertNotNull(h5);
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(h5)) {
            URI signed = server.uri("/remote.ttl.h5?X-Amz-Signature=SECRET");
            HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(signed);
            try (BeakGraph remote = BG.getBeakGraph(ch)) {
                assertEquals(server.uri("/remote.ttl.h5"), remote.getURI(), "identity is the URL without its query");
                assertFalse(remote.getURI().toString().contains("SECRET"));
                List<org.apache.jena.graph.Triple> triples = remote.find().toList();
                assertFalse(triples.isEmpty(), "and the graph reads through the signed URL");
            }
        }
    }
}
