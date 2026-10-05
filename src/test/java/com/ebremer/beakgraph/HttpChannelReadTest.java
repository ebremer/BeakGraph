package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import io.jhdf.exceptions.HdfException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage for reading a BeakGraph over HTTP range requests:
 * {@link HTTPSeekableByteChannel} semantics (positioning, EOF, block
 * stitching, caching, read-only and closed behavior, size-probe fallback)
 * and the {@code BG.getBeakGraph(SeekableByteChannel)} facade, including
 * channel ownership on success and failure. The server is a minimal
 * in-test loopback HTTP server that records every request, so the tests
 * can assert the whole-file download never happens.
 */
class HttpChannelReadTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:s1 ex:p ex:o1 .
        ex:s1 ex:p "plain" .
        ex:s2 ex:p "hello"@en .
        ex:s2 ex:q "42"^^xsd:integer .
        ex:s3 ex:p ex:o2 .
        ex:s3 ex:long "%s" .
        """.formatted("z".repeat(2048));   // past the 64-byte FCD zstd threshold: the fragment path runs over HTTP

    @TempDir
    static Path dir;
    static File h5;
    static byte[] h5Bytes;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("remote.ttl").toFile();
        h5 = dir.resolve("remote.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        h5Bytes = Files.readAllBytes(h5.toPath());
    }

    private static int count(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    /** Deterministic filler that never repeats with period 256 (catches offset slips). */
    private static byte[] pattern(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i * 31 + 7);
        }
        return data;
    }

    // --- the whole point: query a remote BeakGraph in place -----------------

    @Test
    void remoteGraphAnswersIdenticallyToLocal() throws Exception {
        Set<Triple> expected;
        try (BeakGraph local = BG.getBeakGraph(h5)) {
            expected = new HashSet<>(local.find().toList());
        }
        try (RangeServer server = new RangeServer(h5Bytes)) {
            URI uri = server.uri("/remote.ttl.h5");
            HTTPSeekableByteChannel channel = new HTTPSeekableByteChannel(uri);
            try (BeakGraph remote = BG.getBeakGraph(channel)) {
                assertEquals(uri, remote.getURI(), "graph identity must be the remote URI");
                assertEquals(expected, new HashSet<>(remote.find().toList()));
                assertEquals(expected.size(),
                        count(remote.getDataset(), "SELECT * WHERE { ?s ?p ?o }"));
                assertEquals(2, count(remote.getDataset(),
                        "SELECT * WHERE { <http://ex.org/s1> <http://ex.org/p> ?o }"));
                assertEquals(1, count(remote.getDataset(),
                        "SELECT * WHERE { <http://ex.org/s3> <http://ex.org/long> \"" + "z".repeat(2048) + "\" }"),
                        "a zstd-compressed literal must decode through the channel");
                List<String> gets = server.requests.stream()
                        .filter(r -> r.startsWith("GET ")).toList();
                assertFalse(gets.isEmpty());
                assertTrue(gets.stream().allMatch(r -> r.contains("bytes=")),
                        "every GET must be a range request, got: " + gets);
            }
            assertFalse(channel.isOpen(), "closing the graph must close the channel it owns");
        }
    }

    @Test
    void nonBeakGraphContentFailsAndReleasesChannel() throws Exception {
        try (RangeServer server = new RangeServer(new byte[100_000])) { // zeros: no HDF5 signature
            HTTPSeekableByteChannel channel = new HTTPSeekableByteChannel(server.uri("/junk.bin"));
            assertThrows(HdfException.class, () -> BG.getBeakGraph(channel));
            assertFalse(channel.isOpen(), "failed open must release the channel");
        }
    }

    // --- channel semantics ---------------------------------------------------

    @Test
    void readsPositionAndEofBehaveLikeAChannel() throws Exception {
        byte[] data = pattern(300_000); // spans three 128 KiB blocks
        try (RangeServer server = new RangeServer(data);
             HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/pattern.bin"))) {
            assertEquals(data.length, ch.size());
            assertEquals(0, ch.position());

            // a read spanning the 128 KiB block boundary must stitch correctly
            ByteBuffer bb = ByteBuffer.allocate(64);
            ch.position(131_040);
            assertEquals(64, ch.read(bb));
            assertArrayEquals(Arrays.copyOfRange(data, 131_040, 131_104), bb.array());
            assertEquals(131_104, ch.position());

            // sequential scan reconstructs the resource exactly
            ch.position(0);
            ByteArrayOutputStream all = new ByteArrayOutputStream();
            ByteBuffer chunk = ByteBuffer.allocate(50_000);
            int n;
            while ((n = ch.read(chunk)) != -1) {
                all.write(chunk.array(), 0, n);
                chunk.clear();
            }
            assertArrayEquals(data, all.toByteArray());
            long afterScan = ch.getRangeRequestCount();
            assertTrue(afterScan <= 2, "block 0 alone, then blocks 1-2 in one read-ahead request: " + afterScan);
            assertEquals(data.length, ch.getBytesFetched(), "every byte fetched exactly once");

            // cache: re-reading issues no further requests
            ch.position(0);
            chunk.clear();
            assertEquals(50_000, ch.read(chunk));
            assertEquals(afterScan, ch.getRangeRequestCount());

            // EOF semantics, including the legal beyond-size position
            ch.position(data.length);
            assertEquals(-1, ch.read(ByteBuffer.allocate(8)));
            ch.position(data.length + 1_000);
            assertEquals(-1, ch.read(ByteBuffer.allocate(8)));

            // reader-only aspects
            assertThrows(NonWritableChannelException.class, () -> ch.write(ByteBuffer.allocate(1)));
            assertThrows(NonWritableChannelException.class, () -> ch.truncate(10));
            assertThrows(IllegalArgumentException.class, () -> ch.position(-1));
        }
    }

    @Test
    void closedChannelThrowsClosedChannelException() throws Exception {
        try (RangeServer server = new RangeServer(pattern(1_000))) {
            HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/small.bin"));
            assertTrue(ch.isOpen());
            ch.close();
            assertFalse(ch.isOpen());
            assertThrows(ClosedChannelException.class, () -> ch.read(ByteBuffer.allocate(1)));
            assertThrows(ClosedChannelException.class, ch::size);
            assertThrows(ClosedChannelException.class, ch::position);
            assertDoesNotThrow(ch::close, "close must be idempotent");
        }
    }

    @Test
    void headRejectingServerFallsBackToRangeProbe() throws Exception {
        byte[] data = pattern(10_000);
        try (RangeServer server = new RangeServer(data)) {
            server.rejectHead = true; // e.g. a method-scoped presigned URL
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/x.bin"))) {
                assertEquals(data.length, ch.size());
            }
            assertTrue(server.requests.stream().anyMatch(r -> r.endsWith("bytes=0-0")),
                    "size must have come from the range probe: " + server.requests);
        }
    }

    @Test
    void serverWithoutRangeSupportIsRejectedNotDownloaded() throws Exception {
        byte[] data = pattern(300_000);
        try (RangeServer server = new RangeServer(data)) {
            server.ignoreRanges = true;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/x.bin"))) {
                IOException e = assertThrows(IOException.class,
                        () -> ch.read(ByteBuffer.allocate(16)));
                assertTrue(e.getMessage().contains("range"),
                        "failure must name the missing range support: " + e.getMessage());
            }
        }
    }

    // --- BG-180: retry, validation, truncation and eviction branches ------

    private static List<String> rangedGets(RangeServer server) {
        return server.requests.stream().filter(r -> r.startsWith("GET ") && r.contains("bytes=")).toList();
    }

    // --- BG-387 / BG-10 / BG-9 / BG-11 / BG-277 ------------------------------

    @Test
    void redirectsAreFollowedOnceAtOpenNotPerBlock() throws Exception {
        byte[] data = pattern(400_000);
        try (RangeServer server = new RangeServer(data)) {
            server.redirectFrom = "/redir.bin";
            server.redirectTo = server.uri("/target.bin").toString();
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/redir.bin"))) {
                assertEquals(data.length, ch.size());
                ch.position(300_000);
                ByteBuffer bb = ByteBuffer.allocate(1000);
                assertEquals(1000, ch.read(bb));
                assertArrayEquals(Arrays.copyOfRange(data, 300_000, 301_000), bb.array());
                ch.position(10);
                assertEquals(1000, ch.read(ByteBuffer.allocate(1000)));
            }
            List<String> viaRedirect = server.requests.stream().filter(r -> r.contains(" /redir.bin")).toList();
            assertEquals(1, viaRedirect.size(), "only the probe goes through the redirector: " + server.requests);
            assertTrue(viaRedirect.get(0).startsWith("HEAD "), viaRedirect.toString());
            assertTrue(rangedGets(server).stream().allMatch(r -> r.contains(" /target.bin ")),
                    "every block request goes to the resolved target: " + rangedGets(server));
        }
    }

    @Test
    void a206WithoutOrWithAnOverlongBodyIsRejected() throws Exception {
        byte[] data = pattern(300_000);
        try (RangeServer server = new RangeServer(data)) {
            server.omitContentRange = true;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/nocr.bin"))) {
                ch.position(131_072);
                IOException e = assertThrows(IOException.class, () -> ch.read(ByteBuffer.allocate(16)));
                assertTrue(e.getMessage().contains("no Content-Range"), e.getMessage());
            }
            assertEquals(1, rangedGets(server).size(), "a missing Content-Range is permanent: no retry");
        }
        try (RangeServer server = new RangeServer(data)) {
            server.fullBodyOn206 = true;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/full.bin"))) {
                ch.position(131_072);
                IOException e = assertThrows(IOException.class, () -> ch.read(ByteBuffer.allocate(16)));
                assertTrue(e.getMessage().contains("Content-Length"), e.getMessage());
            }
        }
    }

    @Test
    void aHeadWithZeroContentLengthFallsBackToTheRangeProbe() throws Exception {
        byte[] data = pattern(20_000);
        try (RangeServer server = new RangeServer(data)) {
            server.headContentLengthZero = true;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/zero-head.bin"))) {
                assertEquals(data.length, ch.size(), "the range probe's Content-Range total is the size");
                ByteBuffer bb = ByteBuffer.allocate(100);
                assertEquals(100, ch.read(bb));
                assertArrayEquals(Arrays.copyOf(data, 100), bb.array());
            }
            assertTrue(server.requests.stream().anyMatch(r -> r.contains("bytes=0-0")), server.requests.toString());
        }
    }

    @Test
    void retryAfterIsHonouredAndEveryAttemptCounts() throws Exception {
        byte[] data = pattern(10_000);
        try (RangeServer server = new RangeServer(data)) {
            server.failFirstN = 2;
            server.failStatus = "503 Service Unavailable";
            server.retryAfterSeconds = 1;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/throttled.bin"))) {
                long t0 = System.nanoTime();
                ByteBuffer bb = ByteBuffer.allocate(100);
                assertEquals(100, ch.read(bb));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                assertArrayEquals(Arrays.copyOf(data, 100), bb.array());
                assertTrue(elapsedMs >= 1_900, "two Retry-After: 1 waits were honoured, took " + elapsedMs + " ms");
                assertEquals(rangedGets(server).size(), ch.getRangeRequestCount(),
                        "the metric agrees with what the server saw: " + rangedGets(server));
                assertEquals(3, ch.getRangeRequestCount());
            }
        }
    }

    @Test
    void thePoolOpensHttpKeysAndNamesTheSchemesItAccepts() throws Exception {
        try (RangeServer server = new RangeServer(h5Bytes)) {
            URI key = server.uri("/pooled.ttl.h5");
            BeakGraph pooled = com.ebremer.beakgraph.pool.BeakGraphPool.getPool().borrowObject(key);
            try {
                assertEquals(key, pooled.getURI());
                assertEquals(key, pooled.getBase(), "a remote store resolves against its own URL by default");
                assertTrue(count(pooled.getDataset(), "SELECT * WHERE { ?s ?p ?o }") > 0);
            } finally {
                com.ebremer.beakgraph.pool.BeakGraphPool.getPool().returnObject(key, pooled);
            }
            com.ebremer.beakgraph.pool.BeakGraphPool.getPool().clear(key);
        }
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new com.ebremer.beakgraph.pool.BeakGraphPoolFactory().create(URI.create("ftp://host/x.h5")));
        assertTrue(e.getMessage().contains("file: and http(s):"), e.getMessage());
    }

    @Test
    void transientServerErrorsAreRetriedThenGivenUp() throws Exception {
        byte[] data = pattern(10_000);
        try (RangeServer server = new RangeServer(data)) {
            server.failFirstN = 2;   // two 503s, then the real answer
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/flaky.bin"))) {
                ByteBuffer bb = ByteBuffer.allocate(100);
                assertEquals(100, ch.read(bb));
                assertArrayEquals(Arrays.copyOf(data, 100), bb.array());
                assertEquals(3, ch.getRangeRequestCount(), "every attempt that got an answer counts (BG-11)");
            }
            List<String> gets = rangedGets(server);
            assertEquals(3, gets.size(), "two failures plus the success: " + gets);
            assertEquals(1, new HashSet<>(gets).size(), "every attempt asks for the same range: " + gets);
        }
        try (RangeServer server = new RangeServer(data)) {
            server.failFirstN = 5;
            server.failStatus = "429 Too Many Requests";
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/dead.bin"))) {
                IOException e = assertThrows(IOException.class, () -> ch.read(ByteBuffer.allocate(16)));
                assertTrue(e.getMessage().contains("after 5 attempts"), e.getMessage());
                assertTrue(e.getCause() != null && e.getCause().getMessage().contains("HTTP 429"),
                        "the last failure rides along as the cause: " + e.getCause());
            }
            assertEquals(5, rangedGets(server).size(), "MAX_ATTEMPTS bounds the retries");
        }
    }

    @Test
    void mismatchedContentRangeIsRejectedWithoutRetry() throws Exception {
        try (RangeServer server = new RangeServer(pattern(10_000))) {
            server.contentRangeOffsetSkew = 1;   // body is right, header claims the wrong offset
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/skew.bin"))) {
                IOException e = assertThrows(IOException.class, () -> ch.read(ByteBuffer.allocate(16)));
                assertTrue(e.getMessage().contains("mismatched Content-Range"), e.getMessage());
            }
            assertEquals(1, rangedGets(server).size(), "a wrong offset is permanent: no retry");
        }
    }

    @Test
    void truncatedBodyIsRetried() throws Exception {
        byte[] data = pattern(10_000);
        try (RangeServer server = new RangeServer(data)) {
            server.truncateFirstN = 1;   // first answer stops one byte short, then closes
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/short.bin"))) {
                ch.position(4_000);
                ByteBuffer bb = ByteBuffer.allocate(500);
                assertEquals(500, ch.read(bb));
                assertArrayEquals(Arrays.copyOfRange(data, 4_000, 4_500), bb.array());
                assertEquals(2, ch.getRangeRequestCount(), "the truncated attempt counts too");
            }
            assertEquals(2, rangedGets(server).size(), "the truncated answer is retried once");
        }
        try (RangeServer server = new RangeServer(data)) {
            server.truncateFirstN = Integer.MAX_VALUE;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/always-short.bin"))) {
                IOException e = assertThrows(IOException.class, () -> ch.read(ByteBuffer.allocate(16)));
                assertTrue(e.getMessage().contains("after 5 attempts"), e.getMessage());
            }
        }
    }

    @Test
    void notFoundFailsImmediately() throws Exception {
        try (RangeServer server = new RangeServer(pattern(1_000))) {
            server.notFound = true;
            try (HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/gone.bin"))) {
                IOException e = assertThrows(IOException.class, () -> ch.read(ByteBuffer.allocate(16)));
                assertTrue(e.getMessage().contains("HTTP 404"), e.getMessage());
            }
            assertEquals(1, rangedGets(server).size(), "4xx is permanent: no retry");
        }
    }

    @Test
    void cacheEvictsBeyondItsCapacityAndStillReadsCorrectly() throws Exception {
        int block = 4096;
        byte[] data = pattern(64 * block);
        try (RangeServer server = new RangeServer(data);
             HTTPSeekableByteChannel ch = new HTTPSeekableByteChannel(server.uri("/big.bin"), block, 4)) {
            ByteArrayOutputStream all = new ByteArrayOutputStream();
            ByteBuffer chunk = ByteBuffer.allocate(7_000);
            int n;
            while ((n = ch.read(chunk)) != -1) {
                all.write(chunk.array(), 0, n);
                chunk.clear();
            }
            assertArrayEquals(data, all.toByteArray());
            long sequential = ch.getRangeRequestCount();
            // Read-ahead is capped at half the cache (2 blocks here): about one
            // request per two blocks, never more than one per block.
            assertTrue(sequential >= 32 && sequential <= 64, "sequential pass: " + sequential + " requests");
            assertEquals(data.length, ch.getBytesFetched(), "every byte fetched once on the sequential pass");
            java.util.Random rnd = new java.util.Random(5);
            for (int i = 0; i < 200; i++) {
                int at = rnd.nextInt(data.length - 64);
                int len = 1 + rnd.nextInt(64);
                ch.position(at);
                ByteBuffer bb = ByteBuffer.allocate(len);
                assertEquals(len, ch.read(bb));
                assertArrayEquals(Arrays.copyOfRange(data, at, at + len), bb.array(), "random read at " + at);
            }
            assertTrue(ch.getRangeRequestCount() > sequential,
                    "with a 4-block cache the random reads must miss (evicted blocks are re-fetched)");
            assertTrue(ch.getRangeRequestCount() < 64 + 200, "but hot blocks must still hit");
        }
    }

    @Test
    void nonHttpUriIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new HTTPSeekableByteChannel(URI.create("ftp://example.org/x.h5")));
    }

    // --- minimal loopback HTTP server with real HEAD/Range semantics --------

    /**
     * One-request-per-connection HTTP server over a raw socket: full control
     * over HEAD and Range behavior (the JDK's HttpServer manages those
     * headers itself), plus a request log for assertions. Toggles:
     * {@link #rejectHead} answers HEAD with 405, {@link #ignoreRanges}
     * answers ranged GETs with the full resource (a server without range
     * support), {@link #failFirstN} answers the first N ranged GETs with
     * {@link #failStatus}, {@link #truncateFirstN} cuts the body of the first
     * N ranged GETs one byte short, {@link #contentRangeOffsetSkew} shifts
     * the offset claimed in Content-Range while sending the right bytes, and
     * {@link #notFound} answers every GET with 404.
     */
    static final class RangeServer implements AutoCloseable {

        private static final Pattern RANGE = Pattern.compile("bytes=(\\d+)-(\\d+)");

        private final ServerSocket server;
        private volatile byte[] content;
        final List<String> requests = Collections.synchronizedList(new ArrayList<>());
        /** Emit a strong ETag derived from the content and honour If-Range (BG-380). */
        volatile boolean emitEtag = true;
        volatile boolean rejectHead;
        volatile boolean ignoreRanges;
        volatile int failFirstN;
        volatile String failStatus = "503 Service Unavailable";
        volatile int truncateFirstN;
        volatile long contentRangeOffsetSkew;
        volatile boolean notFound;
        /** Path answered with a 302 to {@code redirectTo} (an absolute URL). */
        volatile String redirectFrom;
        volatile String redirectTo;
        /** 206 answers without the Content-Range header (BG-10). */
        volatile boolean omitContentRange;
        /** 206 answers carrying the whole resource as the body (BG-10). */
        volatile boolean fullBodyOn206;
        /** HEAD answers 200 with Content-Length: 0 (BG-9). */
        volatile boolean headContentLengthZero;
        /** Retry-After (seconds) sent with the failing answers (BG-11). */
        volatile int retryAfterSeconds;
        private final java.util.concurrent.atomic.AtomicInteger rangedGets = new java.util.concurrent.atomic.AtomicInteger();

        RangeServer(byte[] content) throws IOException {
            this.content = content;
            this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread.ofVirtual().start(this::acceptLoop);
        }

        URI uri(String path) {
            return URI.create("http://127.0.0.1:" + server.getLocalPort() + path);
        }

        /** Overwrites the served resource in place, as an S3 PUT or a rebuild into a served directory would. */
        void replace(byte[] newContent) {
            this.content = newContent;
        }

        private String etag() {
            return "\"" + Integer.toHexString(Arrays.hashCode(content)) + "-" + content.length + "\"";
        }

        @Override
        public void close() throws IOException {
            server.close();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Thread.ofVirtual().start(() -> handle(socket));
                } catch (IOException closed) {
                    return;
                }
            }
        }

        private void handle(Socket socket) {
            try (socket) {
                List<String> head = readHead(socket.getInputStream());
                if (head == null) {
                    return;
                }
                String[] requestLine = head.get(0).split(" ");
                String method = requestLine[0];
                String path = requestLine[1];
                String range = null;
                String ifRange = null;
                for (int i = 1; i < head.size(); i++) {
                    int colon = head.get(i).indexOf(':');
                    if (colon > 0 && head.get(i).substring(0, colon).trim().equalsIgnoreCase("Range")) {
                        range = head.get(i).substring(colon + 1).trim();
                    }
                    if (colon > 0 && head.get(i).substring(0, colon).trim().equalsIgnoreCase("If-Range")) {
                        ifRange = head.get(i).substring(colon + 1).trim();
                    }
                }
                byte[] content = this.content;
                if (range != null && ifRange != null && emitEtag && !ifRange.equals(etag())) {
                    // RFC 9110 13.1.5: a stale validator turns the range request into a full 200.
                    range = null;
                }
                requests.add(method + " " + path + (range == null ? "" : " " + range));
                OutputStream out = socket.getOutputStream();
                if (redirectFrom != null && path.equals(redirectFrom)) {
                    out.write(("HTTP/1.1 302 Found\r\nLocation: " + redirectTo + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                            .getBytes(StandardCharsets.ISO_8859_1));
                    out.flush();
                    return;
                }
                if (method.equals("HEAD")) {
                    if (rejectHead) {
                        writeHead(out, "405 Method Not Allowed", 0, null);
                    } else {
                        writeHead(out, "200 OK", headContentLengthZero ? 0 : content.length, null);
                    }
                    return;
                }
                if (notFound) {
                    writeHead(out, "404 Not Found", 0, null);
                    return;
                }
                if (range != null && !ignoreRanges) {
                    int nth = rangedGets.incrementAndGet();
                    if (nth <= failFirstN) {
                        writeHead(out, failStatus, 0, null, retryAfterSeconds > 0 ? "Retry-After: " + retryAfterSeconds : null);
                        return;
                    }
                    Matcher m = RANGE.matcher(range);
                    if (m.matches()) {
                        long from = Long.parseLong(m.group(1));
                        long to = Math.min(Long.parseLong(m.group(2)), content.length - 1L);
                        if (from <= to && from < content.length) {
                            int len = (int) (to - from + 1);
                            String contentRange = omitContentRange ? null
                                    : "bytes " + (from + contentRangeOffsetSkew) + "-" + to + "/" + content.length;
                            if (fullBodyOn206) {
                                writeHead(out, "206 Partial Content", content.length, contentRange);
                                out.write(content);
                                out.flush();
                                return;
                            }
                            writeHead(out, "206 Partial Content", len, contentRange);
                            int send = (nth <= truncateFirstN) ? len - 1 : len;
                            out.write(content, (int) from, send);
                            out.flush();
                            return;   // try-with-resources closes the socket: a short body, then EOF
                        }
                    }
                    writeHead(out, "416 Range Not Satisfiable", 0, "bytes */" + content.length);
                    return;
                }
                writeHead(out, "200 OK", content.length, null);
                out.write(content);
                out.flush();
            } catch (IOException clientWentAway) {
                // normal when the client aborts a body it does not want
            }
        }

        /** Reads request line + headers (through the blank line); null on EOF. */
        private static List<String> readHead(InputStream in) throws IOException {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            int a = 0, b = 0, c = 0, d;
            while ((d = in.read()) != -1) {
                buf.write(d);
                if (a == '\r' && b == '\n' && c == '\r' && d == '\n') {
                    String head = buf.toString(StandardCharsets.ISO_8859_1);
                    return Arrays.stream(head.split("\r\n")).filter(s -> !s.isEmpty()).toList();
                }
                a = b;
                b = c;
                c = d;
            }
            return null;
        }

        private void writeHead(OutputStream out, String status, long contentLength,
                String contentRange, String... extraHeaders) throws IOException {
            StringBuilder sb = new StringBuilder();
            sb.append("HTTP/1.1 ").append(status).append("\r\n");
            sb.append("Content-Length: ").append(contentLength).append("\r\n");
            for (String h : extraHeaders) {
                if (h != null) sb.append(h).append("\r\n");
            }
            if (emitEtag) {
                sb.append("ETag: ").append(etag()).append("\r\n");
            }
            sb.append("Accept-Ranges: bytes\r\n");
            if (contentRange != null) {
                sb.append("Content-Range: ").append(contentRange).append("\r\n");
            }
            sb.append("Connection: close\r\n\r\n");
            out.write(sb.toString().getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
        }
    }
}
