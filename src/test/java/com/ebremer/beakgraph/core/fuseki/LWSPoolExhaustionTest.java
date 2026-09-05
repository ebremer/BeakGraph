package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.beakgraph.pool.BeakGraphKeyedPool;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-2: the reader pool allowed two readers per store
 * and blocked 60 seconds when both were busy, so the third concurrent query
 * on one file stalled for a minute and then failed with a 500 - although a
 * reader is safe for concurrent use and the cap protected nothing. The cap
 * is now sized for parallelism (8 per store by default, tunable), the wait
 * is short, and an exhausted pool answers 503 with Retry-After.
 */
@Timeout(120)
class LWSPoolExhaustionTest {

    @TempDir
    static Path dir;
    private static Server server;
    private static String base;
    private static URI poolKey;
    private static final HttpClient http = HttpClient.newHttpClient();
    private static final BeakGraphKeyedPool pool = BeakGraphPool.getPool();

    @BeforeAll
    static void startServer() throws Exception {
        Path root = Files.createDirectories(dir.resolve("storage"));
        Path nt = dir.resolve("busy.nt");
        Files.writeString(nt, "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o> .\n", StandardCharsets.UTF_8);
        Path h5 = root.resolve("busy.h5");
        HDF5Writer.Builder().setSource(nt.toFile()).setDestination(h5.toFile())
                .setSpatial(false).setFeatures(false).build().write();
        Model model = LWSMetadataGenerator.generateLWSModel(root);
        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder(new LWSStorageServlet(model)), "/*");
        server.setHandler(ctx);
        server.start();
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        base = "http://localhost:" + port + "/";
        LWSStorageServlet.setBase(base);
        LWSStorageServlet.setStorageRoot(root);
        poolKey = LWSStorageServlet.resolveWithin(root, "busy.h5").toUri();
    }

    @AfterAll
    static void stopServer() throws Exception {
        pool.clear(poolKey);
        if (server != null) server.stop();
    }

    private static HttpResponse<String> query() throws Exception {
        String url = base + "busy.h5?query=" + URLEncoder.encode("SELECT * WHERE { ?s ?p ?o }", StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("Accept", "application/sparql-results+json").GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void moreThanTwoConcurrentBorrowersAreServed() throws Exception {
        // Hold three readers at once, as three in-flight queries would; a
        // fourth query must still be answered promptly rather than waiting
        // for a slot behind a 60 s timeout.
        List<BeakGraph> held = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) held.add(pool.borrowObject(poolKey));
            long t0 = System.nanoTime();
            HttpResponse<String> r = query();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(200, r.statusCode(), r.body());
            assertTrue(ms < 4_000, "the fourth query must not wait for a slot: took " + ms + " ms");
        } finally {
            for (BeakGraph bg : held) pool.returnObject(poolKey, bg);
        }
    }

    @Test
    void exhaustedPoolAnswers503WithRetryAfter() throws Exception {
        int cap = pool.getMaxTotalPerKey();
        assertTrue(cap >= 3, "per-key cap must allow real parallelism, was " + cap);
        List<BeakGraph> held = new ArrayList<>();
        try {
            for (int i = 0; i < cap; i++) held.add(pool.borrowObject(poolKey));
            long t0 = System.nanoTime();
            HttpResponse<String> r = query();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(503, r.statusCode(), r.body());
            assertNotNull(r.headers().firstValue("Retry-After").orElse(null), "503 must carry Retry-After");
            assertTrue(ms < 30_000, "an exhausted pool must fail fast, not stall for a minute: " + ms + " ms");
        } finally {
            for (BeakGraph bg : held) pool.returnObject(poolKey, bg);
        }
        // With the readers back, the store answers again.
        assertEquals(200, query().statusCode());
    }
}
