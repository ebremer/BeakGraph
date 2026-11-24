package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
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
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The result cache ({@link ResultCache}) through the single-file endpoint: a query asked again is answered from the
 * cache, byte for byte what the first run sent; the format, the protocol's dataset and the store are part of the key;
 * answers that can change between runs, refused queries and answers over the size limit are never kept.
 */
class ResultCacheTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> ";
    private static final String TRIG = """
            @prefix ex: <http://ex.org/> .
            ex:a ex:name "A" . ex:a ex:p ex:b . ex:b ex:name "B" . ex:c ex:p ex:a .
            ex:g1 { ex:x ex:in "one" . }
            ex:g2 { ex:y ex:in "two" . }
            """;
    private static final String NAME_OF_A = PREFIX + "SELECT ?n WHERE { ex:a ex:name ?n }";
    private static final String JSON = "application/sparql-results+json";

    @TempDir
    static Path dir;
    static Path h5;
    private static SPARQLEndPoint endpoint;
    private static String base;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeAll
    static void startEndpoint() throws Exception {
        Path src = dir.resolve("cache.trig");
        Files.writeString(src, TRIG, StandardCharsets.UTF_8);
        h5 = dir.resolve("cache.h5");
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5.toFile())
                .setSpatial(false).setFeatures(false).build().write();
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Parameters params = new Parameters();
        params.sparqlendpoint = h5.toFile();
        params.port = port;
        endpoint = SPARQLEndPoint.getSPARQLEndPoint(params);
        base = "http://localhost:" + port + "/";
        ResultCache.configure(64L << 20, 16L << 20);
    }

    @AfterAll
    static void stopEndpoint() {
        if (endpoint != null) endpoint.shutdown();
        ResultCache.configureFromProperties();
    }

    @BeforeEach
    void empty() {
        ResultCache.configure(64L << 20, 16L << 20);
    }

    private static HttpResponse<String> query(String q, String accept, String... params) throws Exception {
        StringBuilder url = new StringBuilder(base).append("rdf?query=").append(enc(q));
        for (int i = 0; i < params.length; i += 2) {
            url.append('&').append(params[i]).append('=').append(enc(params[i + 1]));
        }
        HttpRequest r = HttpRequest.newBuilder(URI.create(url.toString())).timeout(Duration.ofSeconds(60))
                .header("Accept", accept).GET().build();
        return http.send(r, HttpResponse.BodyHandlers.ofString());
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String cache(HttpResponse<String> r) {
        return r.headers().firstValue(ResultCache.HEADER).orElse(null);
    }

    private static String type(HttpResponse<String> r) {
        return r.headers().firstValue("Content-Type").orElse(null);
    }

    @Test
    void aRepeatIsAnsweredFromTheCacheAsTheFirstRunSentIt() throws Exception {
        HttpResponse<String> first = query(NAME_OF_A, JSON);
        assertEquals(200, first.statusCode(), first.body());
        assertEquals("miss", cache(first));
        assertTrue(first.body().contains("\"A\""), first.body());
        HttpResponse<String> again = query(NAME_OF_A, JSON);
        assertEquals(200, again.statusCode());
        assertEquals("hit", cache(again));
        assertEquals(first.body(), again.body());
        assertEquals(type(first), type(again));
        assertEquals(String.valueOf(again.body().getBytes(StandardCharsets.UTF_8).length),
                again.headers().firstValue("Content-Length").orElse(null), "a kept answer is sent with its length");
        // Comments and spacing don't make it another query.
        HttpResponse<String> respelled = query("# asked again\n" + PREFIX + "SELECT   ?n\nWHERE {\n  ex:a ex:name ?n\n}", JSON);
        assertEquals("hit", cache(respelled));
        assertEquals(first.body(), respelled.body());
    }

    @Test
    void anotherQueryIsRunItself() throws Exception {
        query(NAME_OF_A, JSON);
        HttpResponse<String> other = query(PREFIX + "SELECT ?n WHERE { ex:b ex:name ?n }", JSON);
        assertEquals("miss", cache(other));
        assertTrue(other.body().contains("\"B\""), other.body());
    }

    @Test
    void eachFormatIsKeptApart() throws Exception {
        for (String accept : new String[] {JSON, "application/sparql-results+xml", "text/csv"}) {
            HttpResponse<String> first = query(NAME_OF_A, accept);
            assertEquals("miss", cache(first), accept);
            HttpResponse<String> again = query(NAME_OF_A, accept);
            assertEquals("hit", cache(again), accept);
            assertEquals(first.body(), again.body(), accept);
            assertEquals(type(first), type(again), accept);
        }
        assertTrue(query(NAME_OF_A, "application/sparql-results+xml").body().contains("<sparql"));
        assertTrue(query(NAME_OF_A, "text/csv").body().startsWith("n"));
    }

    @Test
    void askAndConstructAreKeptToo() throws Exception {
        String ask = PREFIX + "ASK { ex:a ex:p ex:b }";
        assertEquals("miss", cache(query(ask, JSON)));
        HttpResponse<String> askAgain = query(ask, JSON);
        assertEquals("hit", cache(askAgain));
        assertTrue(askAgain.body().replace(" ", "").contains("\"boolean\":true"), askAgain.body());
        String construct = PREFIX + "CONSTRUCT { ?s ex:q ?o } WHERE { ?s ex:p ?o }";
        HttpResponse<String> first = query(construct, "text/turtle");
        assertEquals("miss", cache(first));
        HttpResponse<String> again = query(construct, "text/turtle");
        assertEquals("hit", cache(again));
        assertEquals(first.body(), again.body());
        assertTrue(type(again).startsWith("text/turtle"), type(again));
        HttpResponse<String> nt = query(construct, "application/n-triples");
        assertEquals("miss", cache(nt), "N-Triples is another format");
        assertTrue(type(nt).startsWith("application/n-triples"), type(nt));
    }

    @Test
    void theProtocolDatasetIsPartOfTheKey() throws Exception {
        String q = "SELECT ?v WHERE { ?s <http://ex.org/in> ?v }";
        HttpResponse<String> one = query(q, JSON, "default-graph-uri", "http://ex.org/g1");
        assertEquals("miss", cache(one));
        assertTrue(one.body().contains("\"one\"") && !one.body().contains("\"two\""), one.body());
        HttpResponse<String> two = query(q, JSON, "default-graph-uri", "http://ex.org/g2");
        assertEquals("miss", cache(two));
        assertTrue(two.body().contains("\"two\"") && !two.body().contains("\"one\""), two.body());
        HttpResponse<String> oneAgain = query(q, JSON, "default-graph-uri", "http://ex.org/g1");
        assertEquals("hit", cache(oneAgain));
        assertEquals(one.body(), oneAgain.body());
    }

    /** Each asked twice: run both times, never kept. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT (NOW() AS ?t) WHERE { }",
        "SELECT ?r WHERE { BIND(RAND() AS ?r) }",
        "SELECT ?u WHERE { BIND(UUID() AS ?u) }",
        "SELECT (SAMPLE(STRUUID()) AS ?u) WHERE { ?s ?p ?o }",
        "SELECT ?b WHERE { BIND(BNODE() AS ?b) }",
        "SELECT ?t WHERE { BIND(<http://jena.apache.org/ARQ/function#now>() AS ?t) }",
        "SELECT ?s WHERE { ?s ?p ?o FILTER EXISTS { BIND(RAND() AS ?r) FILTER(?r < 2) } }",
        "SELECT ?s WHERE { { SELECT ?s WHERE { ?s ?p ?o } ORDER BY RAND() LIMIT 1 } }",
        "SELECT ?s WHERE { ?s ?p ?o } ORDER BY RAND()",
        "SELECT ?g (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY (FLOOR(RAND() * 2) AS ?g)",
        "SELECT ?s ?o WHERE { ?s ?p ?x OPTIONAL { ?x ?q ?o FILTER(RAND() < 2) } }",
        "SELECT ?s WHERE { ?s ?p ?o } GROUP BY ?s HAVING (NOW() > \"2000-01-01T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)"
    })
    void answersThatCanChangeAreNeverKept(String q) throws Exception {
        assertFalse(ResultCache.stable(QueryFactory.create(q)), q);
        for (int i = 0; i < 2; i++) {
            HttpResponse<String> r = query(q, JSON);
            assertEquals(200, r.statusCode(), q + ": " + r.body());
            assertEquals("bypass", cache(r), q);
        }
    }

    @Test
    void anUnstableExpressionInsideUnnestOrUnfoldIsNeverKept() {
        // RDFARRY.md §3.7: the two operators hold an expression Jena's walker does not visit.
        assertFalse(ResultCache.stable(QueryFactory.create("SELECT ?m WHERE { UNNEST(SEQ(RAND(), 1) AS ?m) }",
                org.apache.jena.query.Syntax.syntaxARQ)));
        assertFalse(ResultCache.stable(QueryFactory.create("SELECT ?m WHERE { UNFOLD(STRUUID() AS ?m) }",
                org.apache.jena.query.Syntax.syntaxARQ)));
        assertTrue(ResultCache.stable(QueryFactory.create("SELECT ?m WHERE { UNNEST(SEQ(1, 2) AS ?m) }",
                org.apache.jena.query.Syntax.syntaxARQ)));
    }

    @Test
    void aServiceCallIsNeverKept() {
        assertFalse(ResultCache.stable(QueryFactory.create("SELECT * WHERE { SERVICE <http://example.org/sparql> { ?s ?p ?o } }")));
        assertFalse(ResultCache.stable(QueryFactory.create(
                "SELECT * WHERE { ?s ?p ?o OPTIONAL { SERVICE <http://example.org/sparql> { ?s ?q ?v } } }")));
    }

    @Test
    void deterministicFunctionsAreStable() {
        for (String q : new String[] {
            NAME_OF_A,
            PREFIX + "SELECT (STR(?n) AS ?s) (CONCAT(?n, \"!\") AS ?c) WHERE { ex:a ex:name ?n }",
            "SELECT (<http://www.w3.org/2001/XMLSchema#integer>(\"7\") AS ?i) WHERE { }",
            PREFIX + "SELECT ?s (COUNT(*) AS ?n) WHERE { ?s ?p ?o FILTER EXISTS { ?s ex:p ?x } } GROUP BY ?s ORDER BY DESC(?n)",
            PREFIX + "DESCRIBE ex:a",
        }) {
            assertTrue(ResultCache.stable(QueryFactory.create(q)), q);
        }
    }

    @Test
    void aQueryCutOffByTheTimeLimitIsNeverKept() throws Exception {
        // Twelve unconnected patterns: millions of rows to count, far past a one-second limit.
        StringBuilder q = new StringBuilder("SELECT (COUNT(*) AS ?n) WHERE {");
        for (int i = 0; i < 12; i++) q.append(" ?s").append(i).append(" ?p").append(i).append(" ?o").append(i).append(" .");
        q.append(" }");
        String before = System.getProperty("beakgraph.query.timeout.seconds");
        System.setProperty("beakgraph.query.timeout.seconds", "1");
        try {
            for (int i = 0; i < 2; i++) {
                HttpResponse<String> r = query(q.toString(), JSON);
                assertEquals(503, r.statusCode(), r.body());
                assertNotEquals("hit", cache(r));
            }
        } finally {
            if (before == null) System.clearProperty("beakgraph.query.timeout.seconds");
            else System.setProperty("beakgraph.query.timeout.seconds", before);
        }
        assertEquals(400, query("SELECT WHERE {", JSON).statusCode(), "a parse error is refused each time");
        assertEquals(400, query("SELECT WHERE {", JSON).statusCode());
    }

    @Test
    void anUnknownFunctionGivesTheSameAnswerEveryTime() throws Exception {
        // Jena leaves the value unbound: an answer like any other, and kept.
        String q = "SELECT (<http://ex.org/no-such-function>(?o) AS ?x) WHERE { ?s ?p ?o }";
        HttpResponse<String> first = query(q, JSON);
        assertEquals(200, first.statusCode(), first.body());
        HttpResponse<String> again = query(q, JSON);
        assertEquals("hit", cache(again));
        assertEquals(first.body(), again.body());
    }

    @Test
    void anAnswerOverTheEntryLimitIsNotKept() throws Exception {
        ResultCache.configure(64L << 20, 1024);
        String big = "SELECT * WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i }";
        HttpResponse<String> first = query(big, "application/sparql-results+xml");
        assertTrue(first.body().length() > 1024, "a result of " + first.body().length() + " characters");
        assertEquals("miss", cache(first));
        HttpResponse<String> again = query(big, "application/sparql-results+xml");
        assertEquals("miss", cache(again), "too large to keep");
        assertEquals(first.body(), again.body());
        query(NAME_OF_A, JSON);
        assertEquals("hit", cache(query(NAME_OF_A, JSON)), "a small one still is");
    }

    @Test
    void offMeansEveryQueryIsRun() throws Exception {
        ResultCache.configure(0, 0);
        assertEquals("bypass", cache(query(NAME_OF_A, JSON)));
        assertEquals("bypass", cache(query(NAME_OF_A, JSON)));
    }

    @Test
    void theOpenStoreIsPartOfTheKey() throws Exception {
        Query q = QueryFactory.create(NAME_OF_A);
        try (BeakGraph one = BG.getBeakGraph(h5.toFile()); BeakGraph two = BG.getBeakGraph(h5.toFile())) {
            ResultCache.Key a = ResultCache.key(one.getDataset(), q, base, "json");
            assertNotNull(a);
            assertEquals(a, ResultCache.key(one.getDataset(), q, base, "json"), "the same store, a new Dataset wrapper");
            assertNotEquals(a.store(), ResultCache.key(two.getDataset(), q, base, "json").store(), "the file opened again");
            assertNotEquals(a, ResultCache.key(one.getDataset(), q, base + "other", "json"), "another base");
        }
        assertNull(ResultCache.key(DatasetFactory.create(), q, base, "json"), "not a BeakGraph store: never kept");
    }
}
