package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.beakgraph.lws.LWSMetadataRefresher;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;
import java.util.zip.GZIPInputStream;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-403: the LWS metadata was generated (or loaded from
 * beakgraph.ttl.gz) once and never refreshed, so a file copied into the served
 * directory 404'd, a deleted file stayed listed with an unchanging container
 * ETag, and a replaced file kept its stale size. The refresher must pick up
 * all three - added files immediately on first request, the rest on its
 * periodic scan - and must not regenerate perpetually because rewriting the
 * cache file itself touches the directory.
 */
class LWSMetadataRefreshTest {

    @TempDir
    static Path dir;
    private static Path root;
    private static Server server;
    private static String base;
    private static LWSMetadataRefresher refresher;
    private static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() throws Exception {
        root = Files.createDirectories(dir.resolve("storage"));
        Files.writeString(root.resolve("data.txt"), "v1", StandardCharsets.UTF_8);
        Model initial = LWSMetadataGenerator.generateLWSModel(root);
        LWSMetadataGenerator.writeModelToGZ(initial, root.resolve(LWSMetadataGenerator.CACHE_FILE_NAME));
        refresher = new LWSMetadataRefresher(root, initial);
        refresher.start(1);

        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder(new LWSStorageServlet(refresher)), "/*");
        server.setHandler(ctx);
        server.start();
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        base = "http://localhost:" + port + "/";
        LWSStorageServlet.setBase(base);
        LWSStorageServlet.setStorageRoot(root);
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (refresher != null) refresher.close();
        if (server != null) server.stop();
    }

    private static HttpResponse<String> get(String path, String accept) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + path)).header("Accept", accept).GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static String listing() throws Exception {
        HttpResponse<String> r = get("", "application/json");
        assertEquals(200, r.statusCode(), r.body());
        return r.body();
    }

    private static long describedSize(String name) throws Exception {
        HttpResponse<String> r = get(name + "?format=turtle", "text/turtle");
        assertEquals(200, r.statusCode(), r.body());
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new StringReader(r.body()), null, Lang.TURTLE);
        return m.getResource(base + name)
                .getProperty(ResourceFactory.createProperty("https://schema.org/size")).getLong();
    }

    private static void awaitUntil(BooleanSupplier condition, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(200);
        }
    }

    private static Model loadCache(Path cache) throws Exception {
        Model m = ModelFactory.createDefaultModel();
        try (InputStream in = new GZIPInputStream(Files.newInputStream(cache))) {
            RDFDataMgr.read(m, in, Lang.TURTLE);
        }
        return m;
    }

    @Test
    void signaturesAgreeForAFreshTreeAndDivergeAfterAChange() throws Exception {
        Path r2 = Files.createDirectories(dir.resolve("sig"));
        Files.writeString(r2.resolve("a.txt"), "a", StandardCharsets.UTF_8);
        Files.createDirectories(r2.resolve("sub"));
        Files.writeString(r2.resolve("sub").resolve("b.txt"), "bb", StandardCharsets.UTF_8);
        Model m = LWSMetadataGenerator.generateLWSModel(r2);
        Path cache = r2.resolve(LWSMetadataGenerator.CACHE_FILE_NAME);
        LWSMetadataGenerator.writeModelToGZ(m, cache);
        // The cache write (and the temp file it goes through) must not perturb the fingerprint.
        assertEquals(LWSMetadataGenerator.treeSignature(r2), LWSMetadataGenerator.modelSignature(m));
        // The cache round-trips to the same fingerprint (that is what validates it at startup).
        assertEquals(LWSMetadataGenerator.modelSignature(m), LWSMetadataGenerator.modelSignature(loadCache(cache)));
        Files.writeString(r2.resolve("a.txt"), "a changed", StandardCharsets.UTF_8);
        assertNotEquals(LWSMetadataGenerator.treeSignature(r2), LWSMetadataGenerator.modelSignature(m));
    }

    @Test
    void fileAddedAfterStartupIsServedAtOnceAndCached() throws Exception {
        Files.writeString(root.resolve("added.txt"), "hello", StandardCharsets.UTF_8);
        HttpResponse<String> r = get("added.txt", "*/*");
        assertEquals(200, r.statusCode(), "an added file must be found on its first request, not after the next scan");
        assertEquals("hello", r.body());
        assertTrue(listing().contains("added.txt"), "the container listing must include the added file");
        Model cached = loadCache(root.resolve(LWSMetadataGenerator.CACHE_FILE_NAME));
        assertTrue(cached.containsResource(cached.createResource(LWSMetadataGenerator.CANONICAL_BASE + "/added.txt")),
                "the cache file must be rewritten after a refresh");
    }

    @Test
    void deletedFileDisappearsFromListingAndIs404() throws Exception {
        Files.writeString(root.resolve("gone.txt"), "x", StandardCharsets.UTF_8);
        assertEquals(200, get("gone.txt", "*/*").statusCode());
        Files.delete(root.resolve("gone.txt"));
        awaitUntil(() -> {
            try { return !listing().contains("gone.txt"); } catch (Exception e) { throw new RuntimeException(e); }
        }, "the periodic scan to drop gone.txt from the listing");
        assertEquals(404, get("gone.txt", "*/*").statusCode());
    }

    @Test
    void replacedFileUpdatesItsSizeAndTheContainerEtag() throws Exception {
        Files.writeString(root.resolve("replace.txt"), "v1", StandardCharsets.UTF_8);
        assertEquals(200, get("replace.txt", "*/*").statusCode());
        assertEquals(2L, describedSize("replace.txt"));
        String etagBefore = get("", "application/json").headers().firstValue("ETag").orElse("");
        String longer = "version two, quite a bit longer";
        Files.writeString(root.resolve("replace.txt"), longer, StandardCharsets.UTF_8);
        awaitUntil(() -> {
            try { return describedSize("replace.txt") == longer.length(); } catch (Exception e) { throw new RuntimeException(e); }
        }, "the periodic scan to pick up the replaced file's size");
        assertEquals(longer, get("replace.txt", "*/*").body());
        String etagAfter = get("", "application/json").headers().firstValue("ETag").orElse("");
        assertNotEquals(etagBefore, etagAfter, "the container ETag must change when a member changes");
    }

    @Test
    void rewritingTheCacheDoesNotTriggerAnotherRefresh() throws Exception {
        refresher.refreshIfChanged();                       // settle whatever other tests left behind
        assertFalse(refresher.refreshIfChanged(), "steady state must not regenerate");
        Files.writeString(root.resolve("loop.txt"), "l", StandardCharsets.UTF_8);
        assertTrue(refresher.refreshIfChanged(), "a new file must regenerate");
        assertFalse(refresher.refreshIfChanged(), "the cache write itself must not count as a change");
    }
}
