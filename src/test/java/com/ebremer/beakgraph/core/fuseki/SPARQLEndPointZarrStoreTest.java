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
import org.junit.jupiter.api.parallel.Isolated;

/**
 * {@code -endpoint store.zarr}: a Zarr store is a directory, and it is served
 * as the ONE store it is, at /rdf, as an HDF5 file is - not as LWS storage of
 * the files it is made of.
 */
// Starts the JVM's one endpoint: never interleave with the other endpoint tests (BG-189).
@Isolated
class SPARQLEndPointZarrStoreTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        <> a geo:FeatureCollection ; ex:name "doc" .
        ex:a ex:p ex:b . ex:a ex:name "A" .
        """;

    @TempDir
    static Path dir;
    private static Path parent;
    private static SPARQLEndPoint endpoint;
    private static String base;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeAll
    static void startEndpoint() throws Exception {
        parent = Files.createDirectories(dir.resolve("data"));
        Path ttl = dir.resolve("doc.ttl");
        Files.writeString(ttl, TTL, StandardCharsets.UTF_8);
        Path zarr = parent.resolve("big.zarr");
        HDF5Writer.Builder().setSource(ttl.toFile()).setDestination(zarr.toFile())
                .setSpatial(false).setFeatures(false).build().write();
        assertTrue(Files.isRegularFile(zarr.resolve("zarr.json")), "a Zarr store was written");
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Parameters params = new Parameters();
        params.sparqlendpoint = zarr.toFile();
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
    void rdfServesTheZarrStore() throws Exception {
        String prefixes = "PREFIX ex: <http://ex.org/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
        HttpResponse<String> r = get("rdf?query=" + enc(prefixes + "SELECT ?n WHERE { ex:a ex:name ?n }"), "Accept", "application/sparql-results+json");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"A\""), r.body());
        // <> names the store, served at /rdf, as for an HDF5 file.
        HttpResponse<String> abs = get("rdf?query=" + enc(prefixes + "SELECT ?s WHERE { ?s a geo:FeatureCollection }"), "Accept", "application/sparql-results+json");
        assertEquals(200, abs.statusCode(), abs.body());
        assertTrue(abs.body().contains(base + "rdf"), "the stored <> is served as the endpoint URL: " + abs.body());
        HttpResponse<String> count = get("rdf?query=" + enc("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"), "Accept", "text/csv");
        assertEquals(200, count.statusCode(), count.body());
        assertTrue(count.body().contains("4"), count.body());
    }

    @Test
    void theStoresOwnFilesAreNotServed() throws Exception {
        // Single-store mode, not LWS storage of the directory: no listing, and none of the files it is made of.
        for (String path : new String[] {"", "zarr.json", ".BG/zarr.json", ".BG/GSPO/So/c/0", "big.zarr", "big.zarr/zarr.json"}) {
            HttpResponse<String> r = get(path, "Accept", "*/*");
            assertEquals(404, r.statusCode(), "path '" + path + "' in single-store mode: " + r.body());
        }
        HttpResponse<String> listing = get("", "Accept", "application/lws+json");
        assertEquals(404, listing.statusCode());
        assertFalse(listing.body().contains("zarr.json"));
    }

    @Test
    void aStorageDirectoryListsAZarrStoreAsOneStore() throws Exception {
        // Directory (LWS) mode serves a Zarr store as the store it is (LWSZarrStoreTest): one entry of the tree it is in.
        Model tree = LWSMetadataGenerator.generateLWSModel(parent);
        assertFalse(tree.isEmpty());
        String ttl = new java.io.StringWriter() {{ tree.write(this, "TURTLE"); }}.toString();
        assertTrue(ttl.contains("big.zarr"), ttl);
        assertFalse(ttl.contains("big.zarr/"), "what the store holds is not listed: " + ttl);
    }
}
