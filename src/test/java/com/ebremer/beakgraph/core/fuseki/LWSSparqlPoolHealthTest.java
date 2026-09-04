package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.beakgraph.pool.BeakGraphKeyedPool;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.rdf.model.Model;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-35: the LWS SPARQL path used to treat every failure
 * that was not a parse error as a broken reader and invalidate the pooled
 * BeakGraph - a client closing the connection mid-result, a query with an
 * unregistered function, or a timeout firing after the response had
 * committed. Each of those cost a full HDF5 re-open with cold caches for the
 * next request, and a looping client could force one per call. Runs a real
 * Jetty server so the failures come from the container, not a mock, and
 * watches the pool: the same reader instance must still be idle afterwards.
 */
class LWSSparqlPoolHealthTest {

    private static final int TRIPLES = 60_000;

    @TempDir
    static Path dir;
    private static Server server;
    private static int port;
    private static String base;
    private static URI poolKey;
    private static final HttpClient http = HttpClient.newHttpClient();
    private static final BeakGraphKeyedPool pool = BeakGraphPool.getPool();

    @BeforeAll
    static void startServer() throws Exception {
        Path root = Files.createDirectories(dir.resolve("storage"));
        Path nt = dir.resolve("data.nt");
        try (BufferedWriter w = Files.newBufferedWriter(nt, StandardCharsets.UTF_8)) {
            for (int i = 0; i < TRIPLES; i++) {
                w.write("<http://ex.org/s" + i + "> <http://ex.org/p> \"value number " + i
                        + " padded so that a full dump is several megabytes\" .\n");
            }
        }
        Path h5 = root.resolve("data.h5");
        HDF5Writer.Builder().setSource(nt.toFile()).setDestination(h5.toFile())
                .setSpatial(false).setFeatures(false).build().write();

        Model model = LWSMetadataGenerator.generateLWSModel(root);
        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder(new LWSStorageServlet(model)), "/*");
        server.setHandler(ctx);
        server.start();
        port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        base = "http://localhost:" + port + "/";
        LWSStorageServlet.setBase(base);
        LWSStorageServlet.setStorageRoot(root);
        // The servlet keys the pool by the resolved file URI.
        poolKey = LWSStorageServlet.resolveWithin(root, "data.h5").toUri();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) server.stop();
    }

    private static String queryUrl(String sparql) {
        return base + "data.h5?query=" + URLEncoder.encode(sparql, StandardCharsets.UTF_8);
    }

    private static HttpResponse<String> get(String sparql) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(queryUrl(sparql)))
                .header("Accept", "application/sparql-results+json").GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    /** A successful query, so the pool holds exactly one idle reader for the file. */
    private static void warmUp() throws Exception {
        HttpResponse<String> r = get("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"" + TRIPLES + "\""), r.body());
        awaitReturned();
        assertEquals(1, pool.getNumIdle(poolKey), "warm-up must leave one idle reader");
    }

    /** Waits for the servlet's finally-block to return or invalidate its reader. */
    private static void awaitReturned() throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (pool.getNumActive(poolKey) != 0) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("pooled reader still active after 30 s");
            }
            Thread.sleep(50);
        }
    }

    private static void assertSameReaderStillPooled(long createdBefore, String what) throws Exception {
        awaitReturned();
        assertEquals(createdBefore, pool.getCreatedCount(), what + ": no reader may have been re-created");
        assertEquals(1, pool.getNumIdle(poolKey), what + ": the reader must be back in the pool, not invalidated");
    }

    /** Sends a raw GET and returns the socket once the first response bytes have arrived. */
    private static Socket openStreamingQuery(String sparql) throws IOException {
        Socket s = new Socket("localhost", port);
        String path = "/data.h5?query=" + URLEncoder.encode(sparql, StandardCharsets.UTF_8);
        s.getOutputStream().write(("GET " + path + " HTTP/1.1\r\nHost: localhost:" + port
                + "\r\nAccept: application/sparql-results+json\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
        s.getOutputStream().flush();
        byte[] buf = new byte[1024];
        int n = s.getInputStream().read(buf);
        assertTrue(n > 0, "server must have started streaming the response");
        String head = new String(buf, 0, n, StandardCharsets.US_ASCII);
        assertTrue(head.startsWith("HTTP/1.1 200"), head);
        return s;
    }

    @Test
    void queryLevelErrorIsA400AndKeepsThePooledReader() throws Exception {
        warmUp();
        long created = pool.getCreatedCount();
        // Parses fine; fails during execution with a QueryException (here
        // QueryExceptionHTTP: the federated endpoint refuses the connection).
        // Federation is a deliberate feature of this endpoint, so a dead remote
        // is an everyday query-level error, not a reader fault.
        HttpResponse<String> r = get("SELECT * WHERE { SERVICE <http://127.0.0.1:1/sparql> { ?s ?p ?o } }");
        assertEquals(400, r.statusCode(), r.body());
        assertSameReaderStillPooled(created, "failed SERVICE call");
        // And the reader keeps answering.
        assertEquals(200, get("ASK { ?s ?p ?o }").statusCode());
    }

    @Test
    void unregisteredFunctionIsAnEvaluationErrorNotAFailure() throws Exception {
        // Documents the Jena 6.2 behaviour the fix relies on: an unknown function
        // URI makes the FILTER false (ExprEvalException) rather than raising, so
        // the query completes with no rows and the reader is untouched.
        warmUp();
        long created = pool.getCreatedCount();
        HttpResponse<String> r = get("SELECT * WHERE { ?s ?p ?o FILTER(<http://no.such/fn>(?o)) }");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().replaceAll("\\s+", "").contains("\"bindings\":[]"), r.body());
        assertSameReaderStillPooled(created, "unregistered function");
    }

    @Test
    void clientDisconnectMidStreamKeepsThePooledReader() throws Exception {
        warmUp();
        long created = pool.getCreatedCount();
        try (Socket s = openStreamingQuery("SELECT * WHERE { ?s ?p ?o }")) {
            // Reset the connection (SO_LINGER 0) with megabytes still unsent: the
            // server's next write fails with Jetty's EofException.
            s.setSoLinger(true, 0);
        }
        assertSameReaderStillPooled(created, "client disconnect");
        assertEquals(200, get("ASK { ?s ?p ?o }").statusCode());
    }

    @Test
    void timeoutAfterCommitKeepsThePooledReader() throws Exception {
        warmUp();
        long created = pool.getCreatedCount();
        String previous = System.getProperty("beakgraph.query.timeout.seconds");
        System.setProperty("beakgraph.query.timeout.seconds", "1");
        try {
            // A cross product streams rows immediately (committing the response)
            // and cannot finish before the 1 s limit fires mid-body.
            long bytes = 0;
            try (Socket s = openStreamingQuery("SELECT * WHERE { ?a ?b ?c . ?d ?e ?f }")) {
                InputStream in = s.getInputStream();
                byte[] buf = new byte[65536];
                try {
                    for (int n; (n = in.read(buf)) != -1;) bytes += n;
                } catch (IOException aborted) {
                    // the server aborted the transfer - expected
                }
            }
            assertTrue(bytes > 0, "the response must have been streaming when the timeout fired");
        } finally {
            if (previous == null) System.clearProperty("beakgraph.query.timeout.seconds");
            else System.setProperty("beakgraph.query.timeout.seconds", previous);
        }
        assertSameReaderStillPooled(created, "post-commit timeout");
        assertEquals(200, get("ASK { ?s ?p ?o }").statusCode());
    }
}
