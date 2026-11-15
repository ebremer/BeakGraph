package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
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
 * The single-file endpoint ({@code -endpoint file.h5}) over HTTP - BG-179's
 * smoke test of the HDF5SparqlServlet - and BG-45: a {@code beakgraph.ttl.gz}
 * left in the parent directory by an earlier directory-mode run must not
 * turn the LWS surface into a listing of the parent tree whose every data GET
 * answers 500. Single-file mode serves one store at /rdf; LWS paths are 404.
 */
class SPARQLEndPointSingleFileTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        <> a geo:FeatureCollection ; ex:name "doc" .
        ex:a ex:p ex:b . ex:a ex:name "A" .
        """;

    @TempDir
    static Path dir;
    private static SPARQLEndPoint endpoint;
    private static String base;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeAll
    static void startEndpoint() throws Exception {
        Path parent = Files.createDirectories(dir.resolve("data"));
        Path ttl = dir.resolve("doc.ttl");
        Files.writeString(ttl, TTL, StandardCharsets.UTF_8);
        Path h5 = parent.resolve("big.h5");
        HDF5Writer.Builder().setSource(ttl.toFile()).setDestination(h5.toFile())
                .setSpatial(false).setFeatures(false).build().write();
        Files.write(parent.resolve("hello.txt"), "hello".getBytes(StandardCharsets.UTF_8));
        // The leftover of "-endpoint data/": a cache listing the whole directory.
        Model tree = LWSMetadataGenerator.generateLWSModel(parent);
        LWSMetadataGenerator.writeModelToGZ(tree, parent.resolve(LWSMetadataGenerator.CACHE_FILE_NAME));
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Parameters params = new Parameters();
        params.sparqlendpoint = h5.toFile();
        params.port = port;
        endpoint = SPARQLEndPoint.getSPARQLEndPoint(params);
        base = "http://localhost:" + port + "/";
    }

    @AfterAll
    static void stopEndpoint() {
        if (endpoint != null) endpoint.shutdown();
    }

    private static HttpResponse<String> get(String path, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(60));
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return http.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    @Test
    void siblingCacheDoesNotExposeTheParentDirectory() throws Exception {
        for (String path : new String[]{"", "hello.txt", "big.h5", "big.h5?format=turtle"}) {
            HttpResponse<String> r = get(path, "Accept", "*/*");
            assertEquals(404, r.statusCode(), "LWS path '" + path + "' in single-file mode: " + r.body());
        }
        HttpResponse<String> listing = get("", "Accept", "application/lws+json");
        assertEquals(404, listing.statusCode(), "no container listing of the parent tree");
        assertFalse(listing.body().contains("hello.txt"));
    }

    @Test
    void rdfServesTheStoreWithTheServedUrlAsBase() throws Exception {
        String prefixes = "PREFIX ex: <http://ex.org/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
        HttpResponse<String> r = get("rdf?query=" + enc(prefixes + "SELECT ?n WHERE { ex:a ex:name ?n }"), "Accept", "application/sparql-results+json");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"A\""), r.body());
        // <> names the store, served at /rdf.
        HttpResponse<String> self = get("rdf?query=" + enc(prefixes + "ASK { <> a geo:FeatureCollection }"), "Accept", "application/sparql-results+json");
        assertEquals(200, self.statusCode(), self.body());
        assertTrue(self.body().replace(" ", "").contains("\"boolean\":true"), self.body());
        HttpResponse<String> abs = get("rdf?query=" + enc(prefixes + "SELECT ?s WHERE { ?s a geo:FeatureCollection }"), "Accept", "application/sparql-results+json");
        assertTrue(abs.body().contains(base + "rdf"), "the stored <> is served as the endpoint URL: " + abs.body());
        HttpResponse<String> post = http.send(HttpRequest.newBuilder(URI.create(base + "rdf/query")).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/sparql-query").header("Accept", "application/sparql-results+json")
                .POST(HttpRequest.BodyPublishers.ofString(prefixes + "SELECT ?n WHERE { ex:a ex:name ?n }")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, post.statusCode(), post.body());
    }

    /**
     * The response buffer is one Jetty's pool keeps (64 KiB), not Fuseki's
     * 5 MiB, which was allocated and zero-filled anew for every response; and
     * a result several times the buffer still arrives whole.
     */
    @Test
    void responsesAreGatheredInABufferThePoolKeeps() throws Exception {
        if (System.getProperty("beakgraph.http.output.buffer") == null) {
            assertEquals(java.util.List.of(SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER), SPARQLEndPoint.outputBufferSizes(),
                    "every HTTP connector of the endpoint");
        }
        // 4 triples, to the fourth power: 256 rows of twelve terms each, several buffers of XML.
        String product = "SELECT * WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i . ?j ?k ?l }";
        HttpResponse<String> r = get("rdf?query=" + enc(product), "Accept", "application/sparql-results+xml");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().length() > 2 * SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER, "a result of " + r.body().length() + " characters");
        assertEquals(256, r.body().split("<result>", -1).length - 1, "every row of the result");
        assertTrue(r.body().stripTrailing().endsWith("</sparql>"), "the result is whole");
        // A small one fits the buffer and is sent with its length, as a response held back whole is.
        HttpResponse<String> small = get("rdf?query=" + enc("ASK { }"), "Accept", "application/sparql-results+json");
        assertEquals(200, small.statusCode(), small.body());
        assertTrue(small.headers().firstValue("Content-Length").isPresent(), "a small response carries its length: " + small.headers().map());
    }

    @Test
    void oversizedBodyIsRefused() throws Exception {
        byte[] big = new byte[BGSparqlService.MAX_QUERY_BODY_BYTES + 1];
        java.util.Arrays.fill(big, (byte) ' ');
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base + "rdf")).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/sparql-query")
                .POST(HttpRequest.BodyPublishers.ofByteArray(big)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(413, r.statusCode());
    }

    @Test
    void thereIsNoStorageDescriptionWithoutAStorage() throws Exception {
        // Single-file mode serves no LWS storage, so there is no storage
        // description: its mandatory StorageRoot service would name a root
        // container that answers 404. The SPARQL endpoint stays at /rdf, where
        // the directory-mode description advertises it (BG-37).
        HttpResponse<String> d = get("description", "Accept", "application/ld+json");
        assertEquals(404, d.statusCode(), d.body());
        assertEquals(404, get("description.meta").statusCode());
    }
}
