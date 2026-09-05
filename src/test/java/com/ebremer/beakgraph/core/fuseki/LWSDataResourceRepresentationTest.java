package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.ns.LWS;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-33: GET on an LWS data resource must return the
 * STORED representation. The servlet used to answer any Accept header that
 * mentioned turtle or json with the resource's RDF metadata description
 * instead, so a stored .ttl or .json file could not be fetched by a client
 * asking for exactly that media type - Jena's own default Accept header
 * included. The description remains available on explicit request only.
 */
class LWSDataResourceRepresentationTest {

    private static final String JENA_DEFAULT_ACCEPT =
            "text/turtle,application/n-triples;q=0.9,application/ld+json;q=0.8,application/rdf+xml;q=0.7,*/*;q=0.3";

    private static final String GRAPH_TTL = """
        @prefix ex: <http://ex.org/> .
        ex:a ex:knows ex:b .
        ex:b ex:knows ex:c .
        ex:c ex:name "c" .
        """;
    private static final String DATA_JSON = "{\"answer\": 42, \"list\": [1, 2, 3]}\n";

    @TempDir
    static Path dir;
    private static Server server;
    private static String base;
    private static byte[] blob;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(10)).build();

    @BeforeAll
    static void startServer() throws Exception {
        Path root = Files.createDirectories(dir.resolve("storage"));
        Files.writeString(root.resolve("graph.ttl"), GRAPH_TTL, StandardCharsets.UTF_8);
        Files.writeString(root.resolve("data.json"), DATA_JSON, StandardCharsets.UTF_8);
        blob = new byte[4096];
        new Random(7).nextBytes(blob);
        Files.write(root.resolve("blob.bin"), blob);

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
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) server.stop();
    }

    private static HttpResponse<byte[]> get(String path, String accept) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(java.time.Duration.ofSeconds(60));
        if (accept != null) b.header("Accept", accept);
        return http.send(b.GET().build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static String contentType(HttpResponse<?> r) {
        return r.headers().firstValue("Content-Type").orElse("");
    }

    @Test
    void storedTurtleIsServedWhenTurtleIsRequested() throws Exception {
        for (String accept : new String[] {"text/turtle", JENA_DEFAULT_ACCEPT, "*/*", null}) {
            HttpResponse<byte[]> r = get("graph.ttl", accept);
            assertEquals(200, r.statusCode(), "Accept: " + accept);
            assertTrue(contentType(r).startsWith("text/turtle"), "Accept: " + accept + " -> " + contentType(r));
            assertArrayEquals(GRAPH_TTL.getBytes(StandardCharsets.UTF_8), r.body(),
                    "Accept: " + accept + " must yield the stored bytes, not the metadata description");
        }
    }

    @Test
    void jenaCanLoadAStoredGraphStraightFromTheServlet() {
        // RDFDataMgr sends Jena's default RDF Accept header - exactly the case
        // that used to receive a 3-triple description of graph.ttl instead of it.
        Model expected = ModelFactory.createDefaultModel();
        RDFDataMgr.read(expected, new StringReader(GRAPH_TTL), null, Lang.TURTLE);
        Model loaded = RDFDataMgr.loadModel(base + "graph.ttl");
        assertTrue(loaded.isIsomorphicWith(expected), "loaded graph differs from the stored file");
    }

    @Test
    void storedJsonIsServedWhenJsonIsRequested() throws Exception {
        for (String accept : new String[] {"application/json", "application/ld+json", "application/json, */*;q=0.1"}) {
            HttpResponse<byte[]> r = get("data.json", accept);
            assertEquals(200, r.statusCode(), "Accept: " + accept);
            assertTrue(contentType(r).startsWith("application/json"), "Accept: " + accept + " -> " + contentType(r));
            assertArrayEquals(DATA_JSON.getBytes(StandardCharsets.UTF_8), r.body(), "Accept: " + accept);
        }
    }

    @Test
    void binaryIsServedWhateverTheAcceptHeaderSays() throws Exception {
        // A single-representation resource is not subject to content negotiation
        // (RFC 9110): the bytes are served rather than a 406 or a description.
        HttpResponse<byte[]> r = get("blob.bin", "text/turtle");
        assertEquals(200, r.statusCode());
        assertTrue(contentType(r).startsWith("application/octet-stream"), contentType(r));
        assertArrayEquals(blob, r.body());
    }

    @Test
    void descriptionIsAvailableOnlyOnExplicitRequest() throws Exception {
        HttpResponse<byte[]> ttl = get("graph.ttl?format=turtle", "text/turtle");
        assertEquals(200, ttl.statusCode());
        assertTrue(contentType(ttl).startsWith("text/turtle"), contentType(ttl));
        Model desc = ModelFactory.createDefaultModel();
        RDFDataMgr.read(desc, new StringReader(new String(ttl.body(), StandardCharsets.UTF_8)), null, Lang.TURTLE);
        var self = ResourceFactory.createResource(base + "graph.ttl");
        assertTrue(desc.contains(self, RDF.type, LWS.DataResource), "description must type the resource");
        assertTrue(desc.contains(self, ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#mediaType"),
                ResourceFactory.createPlainLiteral("text/turtle")), "description must carry the media type");
        assertFalse(desc.contains(ResourceFactory.createResource("http://ex.org/a"), null, (org.apache.jena.rdf.model.RDFNode) null),
                "description must not contain the stored graph's own triples");

        HttpResponse<byte[]> ld = get("graph.ttl?format=jsonld", "text/turtle");
        assertEquals(200, ld.statusCode());
        assertTrue(contentType(ld).startsWith("application/ld+json"), contentType(ld));
    }
}
