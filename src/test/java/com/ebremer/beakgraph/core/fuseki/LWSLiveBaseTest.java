package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
 * Regression test for BG-36 / BG-227 / BG-443: every absolute URI the LWS
 * servlet advertised (container and item ids, first/last/next/prev, up,
 * linkset and storage-description links, HTML anchors) and the base against
 * which SPARQL results resolve document-relative IRIs were minted on a fixed
 * {@code http://localhost:<port>/}, although the server listens on all
 * interfaces - so every remote client was pointed at its own loopback. The
 * base must follow the request (including a reverse proxy's forwarded
 * headers), and an explicit configured base must override it. Requests are
 * sent over a raw socket because the JDK HttpClient refuses a custom Host.
 */
class LWSLiveBaseTest {

    private static final String RELATIVE_TTL = """
        <> <http://ex.org/p> <sibling> .
        """;

    @TempDir
    static Path dir;
    private static Server server;
    private static int port;

    private record Response(int status, Map<String, List<String>> headers, String body) {
        List<String> links() {
            return headers.getOrDefault("link", List.of());
        }
    }

    @BeforeAll
    static void startServer() throws Exception {
        Path root = Files.createDirectories(dir.resolve("storage"));
        for (int i = 0; i < LWSStorageServlet.PAGE_SIZE + 2; i++) {   // enough to paginate
            Files.writeString(root.resolve(String.format("f%02d.txt", i)), "f" + i, StandardCharsets.UTF_8);
        }
        File ttl = dir.resolve("doc.ttl").toFile();
        Files.writeString(ttl.toPath(), RELATIVE_TTL, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(ttl).setDestination(root.resolve("doc.h5").toFile())
                .setSpatial(false).setFeatures(false).build().write();

        Model model = LWSMetadataGenerator.generateLWSModel(root);
        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder(new LWSStorageServlet(model)), "/*");
        server.setHandler(ctx);
        LWSStorageServlet.honourForwardedHeaders(server);
        server.start();
        port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        LWSStorageServlet.setBase(null);          // derive per request
        LWSStorageServlet.setStorageRoot(root);
    }

    @AfterAll
    static void stopServer() throws Exception {
        LWSStorageServlet.setBase(null);
        if (server != null) server.stop();
    }

    /** HTTP/1.0 GET over a raw socket: any Host header, no chunking to parse. */
    private static Response get(String path, String... headers) throws IOException {
        try (Socket s = new Socket("localhost", port)) {
            StringBuilder req = new StringBuilder("GET " + path + " HTTP/1.0\r\n");
            boolean hostGiven = false;
            for (int i = 0; i < headers.length; i += 2) {
                req.append(headers[i]).append(": ").append(headers[i + 1]).append("\r\n");
                if ("host".equalsIgnoreCase(headers[i])) hostGiven = true;
            }
            if (!hostGiven) req.append("Host: localhost:").append(port).append("\r\n");
            req.append("\r\n");
            s.getOutputStream().write(req.toString().getBytes(StandardCharsets.US_ASCII));
            s.getOutputStream().flush();
            String text = new String(s.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int sep = text.indexOf("\r\n\r\n");
            String[] head = text.substring(0, sep).split("\r\n");
            int status = Integer.parseInt(head[0].split(" ")[1]);
            Map<String, List<String>> hdrs = new HashMap<>();
            for (int i = 1; i < head.length; i++) {
                int c = head[i].indexOf(':');
                hdrs.computeIfAbsent(head[i].substring(0, c).toLowerCase(Locale.ROOT), k -> new ArrayList<>())
                        .add(head[i].substring(c + 1).strip());
            }
            return new Response(status, hdrs, text.substring(sep + 4));
        }
    }

    private static void assertAllLinksOn(Response r, String base) {
        assertEquals(200, r.status, r.body);
        assertTrue(r.body.contains("\"id\":\"" + base + "\"") || r.body.contains("\"id\": \"" + base + "\""),
                "container id must be on " + base + ": " + r.body);
        assertFalse(r.body.contains("localhost"), "no localhost URI may leak into the body: " + r.body);
        List<String> links = r.links();
        assertTrue(links.stream().anyMatch(l -> l.contains("<" + base + "description>")),
                "storageDescription link must be on " + base + ": " + links);
        assertTrue(links.stream().anyMatch(l -> l.contains("<" + base + ".meta>")),
                "linkset link must be on " + base + ": " + links);
        assertTrue(links.stream().anyMatch(l -> l.contains("<" + base + "?page=2>") && l.contains("rel=\"next\"")),
                "next page link must be on " + base + ": " + links);
        assertFalse(links.stream().anyMatch(l -> l.contains("localhost")), "no localhost link: " + links);
    }

    @Test
    void linksFollowTheHostTheClientUsed() throws Exception {
        Response r = get("/", "Host", "example.test:9999", "Accept", "application/lws+json");
        assertAllLinksOn(r, "http://example.test:9999/");
        Response d = get("/description", "Host", "example.test:9999", "Accept", "application/lws+json");
        assertEquals(200, d.status);
        assertTrue(d.body.contains("http://example.test:9999/"), d.body);
        assertFalse(d.body.contains("localhost"), d.body);
    }

    @Test
    void defaultPortIsOmitted() throws Exception {
        assertAllLinksOn(get("/", "Host", "example.test", "Accept", "application/lws+json"), "http://example.test/");
    }

    @Test
    void forwardedHeadersGiveThePublicOrigin() throws Exception {
        Response r = get("/", "Accept", "application/lws+json",
                "X-Forwarded-Proto", "https", "X-Forwarded-Host", "public.example.org");
        assertAllLinksOn(r, "https://public.example.org/");
    }

    @Test
    void configuredBaseOverridesTheRequest() throws Exception {
        LWSStorageServlet.setBase("https://cfg.example/root");   // normalized to end with '/'
        try {
            assertAllLinksOn(get("/", "Host", "example.test:9999", "Accept", "application/lws+json"),
                    "https://cfg.example/root/");
        } finally {
            LWSStorageServlet.setBase(null);
        }
    }

    @Test
    void sparqlResultsResolveAgainstTheSameBase() throws Exception {
        String q = URLEncoder.encode("SELECT ?s ?o WHERE { ?s <http://ex.org/p> ?o }", StandardCharsets.UTF_8);
        Response r = get("/doc.h5?query=" + q, "Host", "example.test:9999", "Accept", "application/sparql-results+json");
        assertEquals(200, r.status, r.body);
        assertTrue(r.body.contains("http://example.test:9999/doc.h5"), "<> must resolve on the request base: " + r.body);
        assertTrue(r.body.contains("http://example.test:9999/sibling"), "<sibling> must resolve on the request base: " + r.body);
        assertFalse(r.body.contains("localhost"), r.body);
    }
}
