package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The directory-mode (LWS) endpoint as the CLI starts it, over HTTP:
 * BG-402 - the {@code -timeout} limit applies to the Fuseki-served /rdf
 * metadata dataset (a cross-product query is cancelled and answered 503);
 * BG-427 - the JSON-LD profile frame is applied to SPARQL responses only,
 * never to LWS metadata; BG-37 - the storage description advertises the real
 * SPARQL endpoint and the bare /sparql page URL redirects to the directory
 * form so its relative assets load; BG-38 - an unreadable metadata cache is
 * regenerated instead of served empty.
 */
class SPARQLEndPointDirectoryModeTest {

    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:a ex:p ex:b . ex:a ex:name "A" .
        """;

    @TempDir
    static Path dir;
    private static Path root;
    private static SPARQLEndPoint endpoint;
    private static String base;
    private static String oldTimeout;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeAll
    static void startEndpoint() throws Exception {
        root = Files.createDirectories(dir.resolve("storage"));
        // Enough metadata triples that a four-way cross product cannot finish in a second.
        for (int i = 0; i < 60; i++) {
            Files.write(root.resolve(String.format(java.util.Locale.ROOT, "f%02d.txt", i)), ("file " + i).getBytes(StandardCharsets.UTF_8));
        }
        Path trig = dir.resolve("data.trig");
        Files.writeString(trig, TRIG, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(trig.toFile()).setDestination(root.resolve("data.h5").toFile())
                .setSpatial(false).setFeatures(false).build().write();
        oldTimeout = System.getProperty("beakgraph.query.timeout.seconds");
        System.setProperty("beakgraph.query.timeout.seconds", "1");
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Parameters params = new Parameters();
        params.sparqlendpoint = root.toFile();
        params.port = port;
        endpoint = SPARQLEndPoint.getSPARQLEndPoint(params);
        base = "http://localhost:" + port + "/";
    }

    @AfterAll
    static void stopEndpoint() {
        if (endpoint != null) endpoint.shutdown();
        if (oldTimeout == null) System.clearProperty("beakgraph.query.timeout.seconds");
        else System.setProperty("beakgraph.query.timeout.seconds", oldTimeout);
    }

    private static HttpResponse<String> get(String path, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(120));
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return http.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(String path, String body, String contentType, String accept) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(120))
                .header("Content-Type", contentType).POST(HttpRequest.BodyPublishers.ofString(body));
        if (accept != null) b.header("Accept", accept);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String contentType(HttpResponse<?> r) {
        return r.headers().firstValue("Content-Type").orElse("");
    }

    @Test
    void metadataEndpointHonoursTheQueryTimeout() throws Exception {
        String cross = "SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i . ?j ?k ?l }";
        long start = System.nanoTime();
        HttpResponse<String> r = post("rdf/query", cross, "application/sparql-query", "application/sparql-results+json");
        assertEquals(503, r.statusCode(), "Fuseki answers a cancelled query with 503: " + r.body());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).getSeconds() < 60);
        // A cheap query is unaffected.
        HttpResponse<String> ok = get("rdf?query=" + enc("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"), "Accept", "application/sparql-results+json");
        assertEquals(200, ok.statusCode(), ok.body());
        assertFalse(ok.body().contains("\"value\" : \"0\""), ok.body());
    }

    @Test
    void profileFrameStaysOnSparqlResponses() throws Exception {
        String profile = "application/ld+json;profile=user-profile";
        // LWS metadata with the profile Accept: the resource's own description, not an empty framed document.
        HttpResponse<String> meta = get("data.h5?format=jsonld", "Accept", profile);
        assertEquals(200, meta.statusCode(), meta.body());
        assertTrue(contentType(meta).startsWith("application/ld+json"), contentType(meta));
        assertTrue(meta.body().contains("DataResource"), "the metadata triples must survive: " + meta.body());
        assertTrue(meta.body().contains("data.h5"), meta.body());
        HttpResponse<String> container = get("?format=jsonld", "Accept", profile);
        assertEquals(200, container.statusCode(), container.body());
        assertTrue(container.body().contains("Container"), container.body());

        // The SPARQL endpoints keep the frame: /rdf CONSTRUCT and the per-file query path.
        HttpResponse<String> rdf = post("rdf/query", "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 5",
                "application/sparql-query", profile);
        assertEquals(200, rdf.statusCode(), rdf.body());
        assertTrue(rdf.body().contains("@context"), rdf.body());
        assertTrue(rdf.body().contains("\"geo\""), "the user frame's context is applied on /rdf: " + rdf.body());
        HttpResponse<String> perFile = get("data.h5?query=" + enc("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), "Accept", profile);
        assertEquals(200, perFile.statusCode(), perFile.body());
        assertTrue(perFile.body().contains("\"geo\""), "the frame is applied on the per-file SPARQL path: " + perFile.body());
    }

    @Test
    void storageDescriptionAdvertisesAWorkingSparqlEndpoint() throws Exception {
        HttpResponse<String> d = get("description", "Accept", "application/ld+json");
        assertEquals(200, d.statusCode(), d.body());
        JsonObject doc;
        try (JsonReader r = Json.createReader(new StringReader(d.body()))) {
            doc = r.readObject();
        }
        String sparql = doc.getJsonArray("service").getJsonObject(1).getString("serviceEndpoint");
        assertEquals(base + "rdf", sparql);
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(sparql)).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/sparql-query").header("Accept", "application/sparql-results+json")
                .POST(HttpRequest.BodyPublishers.ofString("SELECT * WHERE { ?s ?p ?o } LIMIT 1")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), "the advertised endpoint must accept a protocol POST: " + r.body());
        assertTrue(contentType(r).startsWith("application/sparql-results+json"), contentType(r));
    }

    @Test
    void sparqlPageRedirectsToItsDirectoryForm() throws Exception {
        HttpResponse<String> bare = get("sparql");
        assertTrue(bare.statusCode() == 302 || bare.statusCode() == 303 || bare.statusCode() == 301, "redirect, got " + bare.statusCode());
        assertTrue(bare.headers().firstValue("Location").orElse("").endsWith("/sparql/"), bare.headers().toString());
        HttpResponse<String> page = get("sparql/");
        assertEquals(200, page.statusCode());
        assertTrue(contentType(page).startsWith("text/html"));
        assertTrue(page.body().contains("yasgui.min.js"));
        assertEquals(200, get("sparql/yasgui.min.js").statusCode(), "relative assets resolve under /sparql/");
        assertEquals(200, get("sparql/beakgraph.png").statusCode());
        assertEquals(405, http.send(HttpRequest.newBuilder(URI.create(base + "sparql/")).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/sparql-query")
                .POST(HttpRequest.BodyPublishers.ofString("ASK {}")).build(), HttpResponse.BodyHandlers.ofString()).statusCode(),
                "the page is not the SPARQL endpoint");
    }

    @Test
    void unreadableCacheIsRegenerated() throws Exception {
        Path other = Files.createDirectories(dir.resolve("other"));
        Files.write(other.resolve("a.h5"), new byte[]{1, 2, 3});
        Path cache = other.resolve(LWSMetadataGenerator.CACHE_FILE_NAME);
        Files.write(cache, new byte[]{0x1f, (byte) 0x8b, 8, 0, 0, 0}); // a gzip header with nothing behind it
        Model m = SPARQLEndPoint.loadOrGenerate(other, cache);
        assertTrue(m.containsResource(m.createResource(LWSMetadataGenerator.CANONICAL_BASE + "/a.h5")),
                "the truncated cache must be replaced by a fresh generation");
        assertTrue(Files.size(cache) > 6, "the regenerated cache is written back");
        Model again = SPARQLEndPoint.loadOrGenerate(other, cache);
        assertTrue(again.containsResource(again.createResource(LWSMetadataGenerator.CANONICAL_BASE + "/a.h5")), "and loads cleanly");
        assertFalse(Files.exists(other.resolve(LWSMetadataGenerator.CACHE_FILE_NAME + ".tmp")), "no temp file is left behind");
    }
}
