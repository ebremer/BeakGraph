package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.DatasetDescription;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-344 (c): the SPARQL 1.1 Protocol dataset parameters
 * ({@code default-graph-uri}, {@code named-graph-uri}) used to be silently
 * ignored by the BeakGraph-served endpoints (the LWS per-file endpoint and
 * the single-file /rdf servlet, which share BGSparqlService). They now
 * override the query's own FROM / FROM NAMED, as the protocol requires
 * (section 2.1.4). End-to-end through a real Jetty container for the LWS
 * servlet; the shared parsing/override helpers are pinned directly.
 */
class SparqlProtocolDatasetTest {

    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:s ex:p ex:o .
        ex:s ex:shared "both" .
        ex:g1 { ex:s ex:shared "both" . ex:s ex:only "g1" . ex:t ex:only "g1" . }
        ex:g2 { ex:s ex:shared "both" . ex:s ex:only "g2" . }
        """;
    private static final Pattern COUNT = Pattern.compile("\"n\"\\s*:\\s*\\{[^}]*\"value\"\\s*:\\s*\"(\\d+)\"");

    @TempDir
    static Path dir;
    private static Server server;
    private static String base;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeAll
    static void startServer() throws Exception {
        Path root = Files.createDirectories(dir.resolve("storage"));
        Path trig = dir.resolve("data.trig");
        Files.writeString(trig, TRIG, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(trig.toFile()).setDestination(root.resolve("data.h5").toFile())
                .setSpatial(false).setFeatures(false).build().write();
        Model model = LWSMetadataGenerator.generateLWSModel(root);
        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder(new LWSStorageServlet(model, null, root)), "/*");
        server.setHandler(ctx);
        server.start();
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        base = "http://localhost:" + port + "/";
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) server.stop();
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static HttpResponse<String> get(String sparql, String extraParams) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "data.h5?query=" + enc(sparql) + extraParams))
                .timeout(Duration.ofSeconds(60))
                .header("Accept", "application/sparql-results+json").GET().build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> postForm(String sparql, String extraParams) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "data.h5"))
                .timeout(Duration.ofSeconds(60))
                .header("Accept", "application/sparql-results+json")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("query=" + enc(sparql) + extraParams)).build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static long count(HttpResponse<String> r) {
        assertEquals(200, r.statusCode(), r.body());
        Matcher m = COUNT.matcher(r.body());
        assertTrue(m.find(), "no count in " + r.body());
        return Long.parseLong(m.group(1));
    }

    private static final String COUNT_ALL = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }";
    private static final String COUNT_NAMED = "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }";

    @Test
    void defaultGraphUriSelectsTheDefaultGraph() throws Exception {
        assertEquals(2, count(get(COUNT_ALL, "")), "no parameter: the store's own default graph");
        assertEquals(3, count(get(COUNT_ALL, "&default-graph-uri=" + enc("http://ex.org/g1"))));
        assertEquals(2, count(get(COUNT_ALL, "&default-graph-uri=" + enc("http://ex.org/g2"))));
        assertEquals(4, count(get(COUNT_ALL, "&default-graph-uri=" + enc("http://ex.org/g1") + "&default-graph-uri=" + enc("http://ex.org/g2"))),
                "two default-graph-uri values form a set union (the shared triple counts once)");
        assertEquals(0, count(get(COUNT_ALL, "&default-graph-uri=" + enc("http://ex.org/absent"))), "an absent graph is empty, not an error");
        assertEquals(3, count(postForm(COUNT_ALL, "&default-graph-uri=" + enc("http://ex.org/g1"))), "form-encoded POST carries the parameter too");
    }

    @Test
    void namedGraphUriRestrictsGraphPatterns() throws Exception {
        assertEquals(5, count(get(COUNT_NAMED, "")), "no parameter: every named graph");
        assertEquals(2, count(get(COUNT_NAMED, "&named-graph-uri=" + enc("http://ex.org/g2"))));
        assertEquals(0, count(get(COUNT_NAMED, "&named-graph-uri=" + enc("http://ex.org/absent"))));
        // named-graph-uri alone leaves the default graph EMPTY (protocol dataset replaces the store's).
        assertEquals(0, count(get(COUNT_ALL, "&named-graph-uri=" + enc("http://ex.org/g1"))));
    }

    @Test
    void protocolDatasetOverridesTheQuerysFromClauses() throws Exception {
        String withFrom = "SELECT (COUNT(*) AS ?n) FROM <http://ex.org/g2> WHERE { ?s ?p ?o }";
        assertEquals(2, count(get(withFrom, "")), "FROM in the query is honoured on its own");
        assertEquals(3, count(get(withFrom, "&default-graph-uri=" + enc("http://ex.org/g1"))), "the protocol's dataset wins over FROM");
    }

    // --- the shared helpers, pinned directly (the single-file /rdf servlet uses the same two) ---

    @Test
    void datasetDescriptionParsing() {
        assertNull(BGSparqlService.datasetDescription(null, null), "no parameters: the query's own dataset (or the whole store)");
        assertNull(BGSparqlService.datasetDescription(new String[0], new String[0]));
        DatasetDescription d = BGSparqlService.datasetDescription(new String[]{"http://ex.org/g1", "http://ex.org/g2"}, new String[]{"http://ex.org/g3"});
        assertEquals(List.of("http://ex.org/g1", "http://ex.org/g2"), d.getDefaultGraphURIs());
        assertEquals(List.of("http://ex.org/g3"), d.getNamedGraphURIs());
    }

    @Test
    void applyingTheProtocolDatasetReplacesFromAndFromNamed() {
        Query q = QueryFactory.create("SELECT * FROM <http://ex.org/q1> FROM NAMED <http://ex.org/q2> WHERE { ?s ?p ?o }");
        BGSparqlService.applyProtocolDataset(q, null);
        assertEquals(List.of("http://ex.org/q1"), q.getGraphURIs(), "no protocol dataset: the query's clauses stand");
        BGSparqlService.applyProtocolDataset(q, BGSparqlService.datasetDescription(new String[]{"http://ex.org/p1"}, null));
        assertEquals(List.of("http://ex.org/p1"), q.getGraphURIs());
        assertEquals(List.of(), q.getNamedGraphURIs(), "FROM NAMED is replaced too: the protocol's dataset is the whole description");
        assertTrue(q.hasDatasetDescription());
        Query plain = QueryFactory.create("SELECT * WHERE { ?s ?p ?o }");
        assertFalse(plain.hasDatasetDescription());
        BGSparqlService.applyProtocolDataset(plain, BGSparqlService.datasetDescription(null, new String[]{"http://ex.org/p2"}));
        assertEquals(List.of("http://ex.org/p2"), plain.getNamedGraphURIs());
    }
}
