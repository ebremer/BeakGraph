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
        """;

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
            assertEquals(3, afterScan, "three blocks -> three range requests");

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
     * support).
     */
    static final class RangeServer implements AutoCloseable {

        private static final Pattern RANGE = Pattern.compile("bytes=(\\d+)-(\\d+)");

        private final ServerSocket server;
        private final byte[] content;
        final List<String> requests = Collections.synchronizedList(new ArrayList<>());
        volatile boolean rejectHead;
        volatile boolean ignoreRanges;

        RangeServer(byte[] content) throws IOException {
            this.content = content;
            this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread.ofVirtual().start(this::acceptLoop);
        }

        URI uri(String path) {
            return URI.create("http://127.0.0.1:" + server.getLocalPort() + path);
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
                for (int i = 1; i < head.size(); i++) {
                    int colon = head.get(i).indexOf(':');
                    if (colon > 0 && head.get(i).substring(0, colon).trim().equalsIgnoreCase("Range")) {
                        range = head.get(i).substring(colon + 1).trim();
                    }
                }
                requests.add(method + " " + path + (range == null ? "" : " " + range));
                OutputStream out = socket.getOutputStream();
                if (method.equals("HEAD")) {
                    if (rejectHead) {
                        writeHead(out, "405 Method Not Allowed", 0, null);
                    } else {
                        writeHead(out, "200 OK", content.length, null);
                    }
                    return;
                }
                if (range != null && !ignoreRanges) {
                    Matcher m = RANGE.matcher(range);
                    if (m.matches()) {
                        long from = Long.parseLong(m.group(1));
                        long to = Math.min(Long.parseLong(m.group(2)), content.length - 1L);
                        if (from <= to && from < content.length) {
                            int len = (int) (to - from + 1);
                            writeHead(out, "206 Partial Content", len,
                                    "bytes " + from + "-" + to + "/" + content.length);
                            out.write(content, (int) from, len);
                            out.flush();
                            return;
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

        private static void writeHead(OutputStream out, String status, long contentLength,
                String contentRange) throws IOException {
            StringBuilder sb = new StringBuilder();
            sb.append("HTTP/1.1 ").append(status).append("\r\n");
            sb.append("Content-Length: ").append(contentLength).append("\r\n");
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
