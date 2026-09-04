package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.cmdline.Parameters;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-218: in directory (LWS) mode the {@code /rdf} dataset
 * used to be registered with Fuseki's default {@code allowUpdate=true}, exposing
 * SPARQL Update and Graph Store writes - over the very model the LWS servlet
 * uses as its allow-list of servable files - to unauthenticated clients. The
 * endpoint is read-only by design: every write route must be refused and the
 * served model must stay untouched, while queries keep working.
 */
class SPARQLEndPointReadOnlyTest {

    private static final String INJECTED = "<urn:x-test:injected> <urn:x-test:p> <urn:x-test:o>";
    private static final Pattern ASK_FALSE = Pattern.compile("\"boolean\"\\s*:\\s*false");

    @TempDir
    static Path dir;
    private static Path root;
    private static SPARQLEndPoint endpoint;
    private static String base;
    private static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void startEndpoint() throws Exception {
        root = Files.createDirectories(dir.resolve("storage"));
        Files.write(root.resolve("hello.txt"), "hello".getBytes(StandardCharsets.UTF_8));
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
    }

    private static HttpResponse<String> send(HttpRequest req) throws Exception {
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> ask() throws Exception {
        String q = "ASK { " + INJECTED + " }";
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "rdf?query="
                + URLEncoder.encode(q, StandardCharsets.UTF_8)))
                .header("Accept", "application/sparql-results+json").GET().build();
        return send(req);
    }

    private static void assertRefused(HttpResponse<String> r, String what) {
        assertTrue(r.statusCode() >= 400, what + " must be refused, got " + r.statusCode());
    }

    @Test
    void fileAddedAfterStartupIsServedThroughTheEndpoint() throws Exception {
        // BG-403: the endpoint wires the metadata refresher, so a file copied into
        // the served directory after start-up is servable on its first request.
        Files.write(root.resolve("later.txt"), "later".getBytes(StandardCharsets.UTF_8));
        HttpResponse<String> r = send(HttpRequest.newBuilder(URI.create(base + "later.txt"))
                .header("Accept", "*/*").GET().build());
        assertEquals(200, r.statusCode(), r.body());
        assertEquals("later", r.body());
    }

    @Test
    void queriesWorkButWritesAreRefusedAndHaveNoEffect() throws Exception {
        HttpResponse<String> before = ask();
        assertEquals(200, before.statusCode(), before.body());
        assertTrue(ASK_FALSE.matcher(before.body()).find(), before.body());

        // SPARQL Update over the dedicated update service and over the dataset root.
        String update = "INSERT DATA { " + INJECTED + " }";
        for (String path : new String[] {"rdf/update", "rdf"}) {
            HttpResponse<String> r = send(HttpRequest.newBuilder(URI.create(base + path))
                    .header("Content-Type", "application/sparql-update")
                    .POST(HttpRequest.BodyPublishers.ofString(update)).build());
            assertRefused(r, "SPARQL Update via " + path);
            r = send(HttpRequest.newBuilder(URI.create(base + path))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString("update="
                            + URLEncoder.encode(update, StandardCharsets.UTF_8))).build());
            assertRefused(r, "form-encoded SPARQL Update via " + path);
        }

        // Graph Store Protocol writes.
        String turtle = INJECTED + " .";
        for (String path : new String[] {"rdf?default", "rdf/data?default"}) {
            HttpResponse<String> put = send(HttpRequest.newBuilder(URI.create(base + path))
                    .header("Content-Type", "text/turtle")
                    .PUT(HttpRequest.BodyPublishers.ofString(turtle)).build());
            assertRefused(put, "GSP PUT via " + path);
            HttpResponse<String> post = send(HttpRequest.newBuilder(URI.create(base + path))
                    .header("Content-Type", "text/turtle")
                    .POST(HttpRequest.BodyPublishers.ofString(turtle)).build());
            assertRefused(post, "GSP POST via " + path);
            HttpResponse<String> delete = send(HttpRequest.newBuilder(URI.create(base + path))
                    .DELETE().build());
            assertRefused(delete, "GSP DELETE via " + path);
        }

        HttpResponse<String> after = ask();
        assertEquals(200, after.statusCode(), after.body());
        assertTrue(ASK_FALSE.matcher(after.body()).find(),
                "the served model must be unchanged after every write attempt: " + after.body());

        // Reads are still the whole point: the LWS metadata is queryable.
        String count = "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }";
        HttpResponse<String> r = send(HttpRequest.newBuilder(URI.create(base + "rdf?query="
                + URLEncoder.encode(count, StandardCharsets.UTF_8)))
                .header("Accept", "application/sparql-results+json").GET().build());
        assertEquals(200, r.statusCode(), r.body());
        assertFalse(r.body().contains("\"value\" : \"0\""), "metadata model must not be empty: " + r.body());
    }
}
