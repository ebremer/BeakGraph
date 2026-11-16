package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.ns.LWS;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * A storage directory (LWS mode) serves a Zarr store as the ONE store it is,
 * as it serves an .h5 file: listed as one data resource, queried with SPARQL
 * at its URL, and its keys served by range so that it can be read where it
 * lies - and a Zarr store in a ZIP archive likewise.
 */
// Starts the JVM's one endpoint: never interleave with the other endpoint tests (BG-189).
@Isolated
class LWSZarrStoreTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:a ex:p ex:b . ex:a ex:name "A" . ex:b ex:name "B" . ex:b ex:p ex:c .
        """;
    private static final String NAMES = "SELECT ?s ?n WHERE { ?s <http://ex.org/name> ?n }";

    @TempDir
    static Path dir;
    private static Path root;
    private static Path zarr;
    private static SPARQLEndPoint endpoint;
    private static String base;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static void build(Path destination) throws Exception {
        Path ttl = dir.resolve("doc.ttl");
        Files.writeString(ttl, TTL, StandardCharsets.UTF_8);
        Files.createDirectories(destination.getParent());
        HDF5Writer.Builder().setSource(ttl.toFile()).setDestination(destination.toFile())
                .setSpatial(false).setFeatures(false).build().write();
    }

    @BeforeAll
    static void startEndpoint() throws Exception {
        root = Files.createDirectories(dir.resolve("storage"));
        zarr = root.resolve("sub/doc.zarr");
        build(zarr);
        build(root.resolve("doc.h5"));
        build(root.resolve("packed.zarr.zip"));
        build(root.resolve(".hidden/secret.zarr"));
        Files.createDirectories(root.resolve("empty.zarr")); // a directory so named, and no store
        Files.writeString(root.resolve("empty.zarr/readme.txt"), "not a store");
        // What is not a key of the store, lying in its directory.
        Files.writeString(zarr.resolve("notes.txt"), "private");
        Files.writeString(zarr.resolve(".BG/extra.bin"), "private");
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

    private static HttpResponse<byte[]> send(String method, String path, String body, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(60));
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static HttpResponse<byte[]> get(String path, String... headers) throws Exception {
        return send("GET", path, null, headers);
    }

    private static String text(HttpResponse<byte[]> r) {
        return new String(r.body(), StandardCharsets.UTF_8);
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    @Test
    void aZarrStoreIsListedAsOneStore() throws Exception {
        Model tree = LWSMetadataGenerator.generateLWSModel(root);
        List<String> uris = new ArrayList<>();
        tree.listSubjects().forEachRemaining(s -> { if (s.isURIResource()) uris.add(s.getURI()); });
        Collections.sort(uris);
        Resource store = tree.listSubjects().filterKeep(s -> s.isURIResource() && s.getURI().endsWith("/sub/doc.zarr")).next();
        assertTrue(store.hasProperty(RDF.type, LWS.DataResource), "a store, as an .h5 file is");
        assertFalse(store.hasProperty(RDF.type, LWS.Container));
        assertEquals(Files.size(zarr.resolve("zarr.json")), store.getProperty(LWS.size).getLong(), "described by the document it is opened from");
        assertEquals(LWSMetadataGenerator.ZARR_STORE_MEDIA_TYPE, store.getProperty(LWS.format).getString());
        assertTrue(uris.stream().noneMatch(u -> u.contains("doc.zarr/")), "what the store holds is not listed: " + uris);
        assertTrue(uris.stream().noneMatch(u -> u.contains("hidden")), uris.toString());
        Resource plain = tree.listSubjects().filterKeep(s -> s.isURIResource() && s.getURI().endsWith("/empty.zarr")).next();
        assertTrue(plain.hasProperty(RDF.type, LWS.Container), "a directory named .zarr that is no store is a directory");
        assertTrue(uris.stream().anyMatch(u -> u.endsWith("/empty.zarr/readme.txt")));
        // The scan and the model agree, or the refresher would regenerate for ever.
        assertEquals(LWSMetadataGenerator.treeSignature(root), LWSMetadataGenerator.modelSignature(tree));

        HttpResponse<byte[]> listing = get("sub/", "Accept", "application/lws+json");
        assertEquals(200, listing.statusCode(), text(listing));
        assertTrue(text(listing).contains("doc.zarr") && text(listing).contains("DataResource"), text(listing));
        assertFalse(text(listing).contains("zarr.json") || text(listing).contains(".BG"), text(listing));
    }

    @Test
    void aStoreAnswersSparqlAtItsUrlWhicheverContainerItIsIn() throws Exception {
        for (String store : new String[] {"sub/doc.zarr", "doc.h5", "packed.zarr.zip"}) {
            HttpResponse<byte[]> r = get(store + "?query=" + enc(NAMES), "Accept", "text/csv");
            assertEquals(200, r.statusCode(), store + ": " + text(r));
            List<String> rows = new ArrayList<>(Arrays.asList(text(r).trim().split("\\R")));
            rows.remove(0);
            Collections.sort(rows);
            assertEquals(List.of("http://ex.org/a,A", "http://ex.org/b,B"), rows, store);
            HttpResponse<byte[]> post = send("POST", store, NAMES, "Content-Type", "application/sparql-query", "Accept", "text/csv");
            assertEquals(200, post.statusCode(), store + ": " + text(post));
            assertTrue(text(post).contains("http://ex.org/b,B"), store);
            HttpResponse<byte[]> options = send("OPTIONS", store, null);
            assertEquals(LWSStorageServlet.ALLOW_QUERYABLE, options.headers().firstValue("Allow").orElse(""), store);
            HttpResponse<byte[]> put = send("PUT", store, "x");
            assertEquals(405, put.statusCode(), store);
        }
        // A directory that is no store takes no query, and neither does what is hidden.
        assertEquals(LWSStorageServlet.ALLOW_READ, send("OPTIONS", "empty.zarr", null).headers().firstValue("Allow").orElse(""));
        assertEquals(405, send("POST", "empty.zarr", NAMES, "Content-Type", "application/sparql-query").statusCode());
        assertEquals(404, get(".hidden/secret.zarr?query=" + enc(NAMES)).statusCode());
    }

    @Test
    void aGetOfAZarrStoreIsTheDocumentItIsOpenedFrom() throws Exception {
        byte[] document = Files.readAllBytes(zarr.resolve("zarr.json"));
        HttpResponse<byte[]> r = get("sub/doc.zarr", "Accept", "text/turtle");
        assertEquals(200, r.statusCode(), text(r));
        assertArrayEquals(document, r.body());
        assertEquals("application/json", r.headers().firstValue("Content-Type").orElse(""));
        assertTrue(r.headers().allValues("Link").stream().anyMatch(l -> l.contains(LWS.DataResource.getURI())), r.headers().allValues("Link").toString());
        HttpResponse<byte[]> head = send("HEAD", "sub/doc.zarr", null);
        assertEquals(200, head.statusCode());
        assertEquals(Integer.toString(document.length), head.headers().firstValue("Content-Length").orElse(""));
        // Its description, on request, as for any data resource.
        HttpResponse<byte[]> description = get("sub/doc.zarr?format=turtle");
        assertEquals(200, description.statusCode(), text(description));
        assertTrue(text(description).contains("doc.zarr") && !text(description).contains("file:///"), text(description));
    }

    @Test
    void theKeysOfAStoreAreServedAndNothingElseInItsDirectory() throws Exception {
        byte[] chunk = Files.readAllBytes(zarr.resolve(".BG/GSPO/So/c/0"));
        HttpResponse<byte[]> whole = get("sub/doc.zarr/.BG/GSPO/So/c/0");
        assertEquals(200, whole.statusCode());
        assertArrayEquals(chunk, whole.body());
        assertEquals("application/octet-stream", whole.headers().firstValue("Content-Type").orElse(""));
        String etag = whole.headers().firstValue("ETag").orElseThrow();
        assertEquals("bytes", whole.headers().firstValue("Accept-Ranges").orElse(""));
        HttpResponse<byte[]> range = get("sub/doc.zarr/.BG/GSPO/So/c/0", "Range", "bytes=1-3", "If-Range", etag);
        assertEquals(206, range.statusCode());
        assertArrayEquals(Arrays.copyOfRange(chunk, 1, 4), range.body());
        assertEquals("bytes 1-3/" + chunk.length, range.headers().firstValue("Content-Range").orElse(""));
        HttpResponse<byte[]> head = send("HEAD", "sub/doc.zarr/.BG/GSPO/So/c/0", null);
        assertEquals(200, head.statusCode());
        assertEquals(Integer.toString(chunk.length), head.headers().firstValue("Content-Length").orElse(""));
        assertEquals(etag, head.headers().firstValue("ETag").orElse(""));
        for (String document : new String[] {"sub/doc.zarr/zarr.json", "sub/doc.zarr/.BG/zarr.json", "sub/doc.zarr/.BG/GSPO/So/zarr.json"}) {
            HttpResponse<byte[]> r = get(document);
            assertEquals(200, r.statusCode(), document);
            assertEquals("application/json", r.headers().firstValue("Content-Type").orElse(""), document);
            assertArrayEquals(Files.readAllBytes(root.resolve(document)), r.body(), document);
        }
        // Not keys of a store: files someone left in its directory, a chunk that is not there, a directory,
        // and all of a store the listing does not have.
        for (String not : new String[] {"sub/doc.zarr/notes.txt", "sub/doc.zarr/.BG/extra.bin", "sub/doc.zarr/.BG/GSPO/So/c/7",
                "sub/doc.zarr/.BG", "sub/doc.zarr/.BG/GSPO/So/c", "sub/doc.zarr/.BG/GSPO/So/c/0/zarr.json/x",
                ".hidden/secret.zarr/zarr.json", ".hidden/secret.zarr/.BG/GSPO/So/c/0", "empty.zarr/zarr.json"}) {
            HttpResponse<byte[]> r = get(not);
            assertEquals(404, r.statusCode(), not + ": " + text(r));
            assertFalse(text(r).contains("private"), not);
        }
        // A way out of the store through a key: the server refuses the path, or finds no key by it.
        HttpResponse<byte[]> out = get("sub/doc.zarr/..%2F..%2F.hidden%2Fsecret.zarr/zarr.json");
        assertTrue(out.statusCode() == 400 || out.statusCode() == 404, out.statusCode() + ": " + text(out));
        assertFalse(text(out).contains("zarr_format"), text(out));
        assertEquals(405, send("POST", "sub/doc.zarr/zarr.json", NAMES, "Content-Type", "application/sparql-query").statusCode());
        assertEquals(405, send("PUT", "sub/doc.zarr/.BG/GSPO/So/c/0", "x").statusCode());
    }

    @Test
    void aStoreInStorageIsReadWhereItLiesAsAZarrStore() throws Exception {
        try (BeakGraph remote = BG.getBeakGraph(URI.create(base + "sub/doc.zarr")); BeakGraph local = BG.getBeakGraph(zarr)) {
            for (String query : new String[] {NAMES, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"}) {
                List<String> expected = new ArrayList<>(StoreParity.select(local.getDataset(), query));
                List<String> answered = new ArrayList<>(StoreParity.select(remote.getDataset(), query));
                Collections.sort(expected);
                Collections.sort(answered);
                assertFalse(expected.isEmpty());
                assertEquals(expected, answered, query);
            }
        }
    }

    @Test
    void aStoreCopiedInWhileTheServerRunsIsServed() throws Exception {
        build(root.resolve("late/new.zarr"));
        build(root.resolve("late/new.zarr.zip"));
        for (String store : new String[] {"late/new.zarr", "late/new.zarr.zip"}) {
            HttpResponse<byte[]> r = get(store + "?query=" + enc(NAMES), "Accept", "text/csv");
            assertEquals(200, r.statusCode(), store + ": " + text(r));
            assertTrue(text(r).contains("http://ex.org/a,A"), text(r));
        }
        assertEquals(200, get("late/new.zarr/.BG/GSPO/So/c/0").statusCode());
    }
}
