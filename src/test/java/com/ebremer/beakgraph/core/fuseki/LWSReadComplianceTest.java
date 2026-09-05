package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * HTTP-level conformance of the unauthenticated read surface against the LWS
 * Protocol 1.0 draft (w3c.github.io/lws-protocol/lws10-core/): container
 * representation shape and media-type negotiation, link-based pagination
 * (first/last/next/prev in Link headers, full totalItems with page-scoped
 * items), mandatory Link relations (type / up / linkset / lws#storageDescription),
 * validators + conditional requests on containers, and single-range requests
 * on data resources. Runs a real Jetty server so header semantics are the
 * container's, not a mock's.
 */
class LWSReadComplianceTest {

    @TempDir
    static Path dir;

    private static Server server;
    private static String base;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(10)).build();
    private static final int FILES = 45;
    /** Enough members to span 3+ pages at any PAGE_SIZE the guard admits. */
    private static final int BIGSUB_FILES = LWSStorageServlet.PAGE_SIZE * 2 + 2;
    /** Root membership: FILES + the "sub" and "big sub" directories. */
    private static final int ROOT_ITEMS = FILES + 2;

    @BeforeAll
    static void startServer() throws Exception {
        Path root = Files.createDirectories(dir.resolve("storage"));
        for (int i = 0; i < FILES; i++) {
            Files.write(root.resolve(String.format("f%02d.txt", i)),
                    ("content-of-file-" + i).getBytes(StandardCharsets.UTF_8));
        }
        Path sub = Files.createDirectories(root.resolve("sub"));
        Files.write(sub.resolve("a.txt"), "aaa".getBytes(StandardCharsets.UTF_8));
        Files.write(sub.resolve("b.txt"), "bbb".getBytes(StandardCharsets.UTF_8));
        // A PAGINATING subcontainer whose name needs percent-encoding: pagination
        // must work below the root, and its page URIs must be valid (my%20slides,
        // not a raw space).
        Path bigsub = Files.createDirectories(root.resolve("big sub"));
        for (int i = 0; i < BIGSUB_FILES; i++) {
            Files.write(bigsub.resolve(String.format("g%02d.txt", i)),
                    ("g-" + i).getBytes(StandardCharsets.UTF_8));
        }

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

    private static HttpResponse<String> get(String path, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(java.time.Duration.ofSeconds(60));
        for (int i = 0; i < headers.length; i += 2) {
            b.header(headers[i], headers[i + 1]);
        }
        return http.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonObject parse(String json) {
        try (JsonReader r = Json.createReader(new StringReader(json))) {
            return r.readObject();
        }
    }

    private static boolean hasLink(HttpResponse<?> resp, String relFragment) {
        return resp.headers().allValues("Link").stream().anyMatch(v -> v.contains(relFragment));
    }

    private static String linkTarget(HttpResponse<?> resp, String relFragment) {
        return resp.headers().allValues("Link").stream()
                .filter(v -> v.contains(relFragment))
                .map(v -> v.substring(v.indexOf('<') + 1, v.indexOf('>')))
                .findFirst().orElse(null);
    }

    @Test
    void containerRepresentationHasTheLwsShape() throws Exception {
        HttpResponse<String> resp = get("", "Accept", "application/lws+json");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.headers().firstValue("Content-Type").orElse("").startsWith("application/lws+json"),
                "requested lws+json must be echoed");
        assertTrue(resp.headers().firstValue("ETag").isPresent(), "containers must carry an ETag");
        assertTrue(resp.headers().firstValue("Last-Modified").isPresent(), "containers must carry Last-Modified");
        assertTrue(hasLink(resp, "rel=\"type\""), "rel=type link required");
        assertTrue(hasLink(resp, "https://www.w3.org/ns/lws#Container"), "type link must name lws#Container");
        assertTrue(hasLink(resp, "rel=\"linkset\""), "rel=linkset link required");
        assertTrue(hasLink(resp, LWSStorageServlet.REL_STORAGE_DESCRIPTION),
                "storage-description discovery link required on all storage responses");
        assertFalse(hasLink(resp, "rel=\"up\""), "the root container has no parent");

        JsonObject doc = parse(resp.body());
        assertEquals("https://www.w3.org/ns/lws/v1", doc.getString("@context"));
        assertEquals(base, doc.getString("id"), "id must be the container URI, not a page URI");
        assertEquals("Container", doc.getString("type"));
        assertEquals(ROOT_ITEMS, doc.getInt("totalItems"), "totalItems reflects the FULL membership");
        assertEquals(LWSStorageServlet.PAGE_SIZE, doc.getJsonArray("items").size(),
                "a large container's bare GET is the first page");
        // Item order is URI-sorted, so scan for one of each kind rather than
        // assuming positions.
        JsonObject file = null;
        JsonObject folder = null;
        for (jakarta.json.JsonValue v : doc.getJsonArray("items")) {
            JsonObject o = v.asJsonObject();
            assertTrue(o.containsKey("id"));
            if ("DataResource".equals(o.getString("type")) && file == null) file = o;
            if ("Container".equals(o.getString("type")) && folder == null) folder = o;
        }
        assertNotNull(file, "page 1 must contain a DataResource");
        assertEquals("text/plain", file.getString("mediaType"), "mediaType is required for DataResources");
        assertTrue(file.containsKey("size"));
        assertTrue(file.containsKey("modified"));
        assertNotNull(folder, "page 1 must contain the 'big sub' Container (URI sort puts it first)");
        assertFalse(folder.containsKey("mediaType"), "containers carry no mediaType");
    }

    @Test
    void subcontainersPaginateWithEncodedPageUris() throws Exception {
        int pages = (BIGSUB_FILES + LWSStorageServlet.PAGE_SIZE - 1) / LWSStorageServlet.PAGE_SIZE;
        assertTrue(pages >= 3, "bigsub must span at least 3 pages");

        HttpResponse<String> p1 = get("big%20sub", "Accept", "application/lws+json");
        assertEquals(200, p1.statusCode());
        assertTrue(hasLink(p1, "rel=\"first\""), "subcontainers must paginate exactly like the root");
        assertTrue(hasLink(p1, "rel=\"next\""));
        assertFalse(hasLink(p1, "rel=\"prev\""));

        String next = linkTarget(p1, "rel=\"next\"");
        assertNotNull(next);
        assertTrue(next.contains("big%20sub"),
                "page URIs must percent-encode the container path, got: " + next);
        assertEquals(URI.create(next).toString(), next, "the next target must be a valid URI");

        JsonObject d1 = parse(p1.body());
        assertEquals(BIGSUB_FILES, d1.getInt("totalItems"));
        assertEquals(LWSStorageServlet.PAGE_SIZE, d1.getJsonArray("items").size());
        assertTrue(d1.containsKey("as:next"), "subcontainer bodies embed navigation too");
        assertEquals(next, d1.getJsonObject("as:next").getString("id"));

        // Follow the served link (opaque-URI navigation, as a client would).
        HttpResponse<String> p2 = get(next.substring(base.length()), "Accept", "application/lws+json");
        assertEquals(200, p2.statusCode());
        assertTrue(hasLink(p2, "rel=\"prev\""));
        assertEquals(BIGSUB_FILES, parse(p2.body()).getInt("totalItems"));

        HttpResponse<String> pLast = get("big%20sub?page=" + pages, "Accept", "application/lws+json");
        assertEquals(200, pLast.statusCode());
        assertFalse(hasLink(pLast, "rel=\"next\""), "no next on the subcontainer's last page");
        assertEquals(BIGSUB_FILES - (pages - 1) * LWSStorageServlet.PAGE_SIZE,
                parse(pLast.body()).getJsonArray("items").size());

        // Listings must revalidate rather than replay from heuristic cache.
        assertEquals("no-cache", p1.headers().firstValue("Cache-Control").orElse(""));
    }

    @Test
    void jsonFlavorsNegotiateContentTypeWithIdenticalBodies() throws Exception {
        HttpResponse<String> lws = get("", "Accept", "application/lws+json");
        HttpResponse<String> ld = get("", "Accept", "application/ld+json");
        HttpResponse<String> plain = get("", "Accept", "application/json");
        assertTrue(ld.headers().firstValue("Content-Type").orElse("").startsWith("application/ld+json"));
        assertTrue(plain.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        assertEquals(lws.body(), ld.body(), "the payload must be identical across the JSON flavors");
        assertEquals(lws.body(), plain.body());
    }

    @Test
    void paginationTravelsInLinkHeaders() throws Exception {
        // Page-size agnostic: whatever PAGE_SIZE is configured, the root must
        // paginate consistently (the storage tree guarantees > 2 pages).
        int total = ROOT_ITEMS;
        int pageSize = LWSStorageServlet.PAGE_SIZE;
        int pages = (total + pageSize - 1) / pageSize;
        assertTrue(pages >= 3, "test data must span at least 3 pages (adjust FILES if PAGE_SIZE grew)");

        HttpResponse<String> p1 = get("", "Accept", "application/lws+json");
        assertTrue(hasLink(p1, "rel=\"first\""), "first MUST be present on paginated responses");
        assertTrue(hasLink(p1, "rel=\"next\""), "next MUST be present when more pages exist");
        assertTrue(hasLink(p1, "rel=\"last\""));
        assertFalse(hasLink(p1, "rel=\"prev\""), "prev MUST be omitted on the first page");

        String next = linkTarget(p1, "rel=\"next\"");
        assertNotNull(next);

        // The body mirrors the Link-header navigation (as: node references).
        JsonObject d1 = parse(p1.body());
        assertTrue(d1.containsKey("as:first"), "paginated bodies embed as:first");
        assertTrue(d1.containsKey("as:next"), "paginated bodies embed as:next");
        assertFalse(d1.containsKey("as:prev"), "no as:prev on the first page");
        assertEquals(next, d1.getJsonObject("as:next").getString("id"),
                "the body's as:next must equal the Link header target");
        HttpResponse<String> p2 = get(next.substring(base.length()), "Accept", "application/lws+json");
        assertEquals(200, p2.statusCode());
        assertTrue(hasLink(p2, "rel=\"prev\""));
        assertTrue(hasLink(p2, "rel=\"next\""));
        JsonObject d2 = parse(p2.body());
        assertEquals(total, d2.getInt("totalItems"), "totalItems stays the full count on every page");
        assertEquals(base, d2.getString("id"), "the body id stays the container URI on every page");
        assertEquals(pageSize, d2.getJsonArray("items").size());

        HttpResponse<String> pLast = get("?page=" + pages, "Accept", "application/lws+json");
        assertEquals(200, pLast.statusCode());
        assertFalse(hasLink(pLast, "rel=\"next\""), "next MUST be omitted on the last page");
        assertTrue(hasLink(pLast, "rel=\"prev\""));
        JsonObject dLast = parse(pLast.body());
        assertEquals(total - (pages - 1) * pageSize, dLast.getJsonArray("items").size());
        assertFalse(dLast.containsKey("as:next"), "no as:next on the last page");
        assertTrue(dLast.containsKey("as:prev"), "the last page embeds as:prev");

        assertEquals(404, get("?page=" + (pages + 1), "Accept", "application/lws+json").statusCode());
        assertEquals(400, get("?page=0", "Accept", "application/lws+json").statusCode());
        assertEquals(400, get("?page=x", "Accept", "application/lws+json").statusCode());
    }

    @Test
    void smallContainersAreNotPaginated() throws Exception {
        HttpResponse<String> resp = get("sub", "Accept", "application/lws+json");
        assertEquals(200, resp.statusCode());
        assertFalse(hasLink(resp, "rel=\"first\""), "below the threshold there is no pagination");
        JsonObject doc = parse(resp.body());
        assertEquals(2, doc.getInt("totalItems"));
        assertEquals(2, doc.getJsonArray("items").size());
        assertFalse(doc.containsKey("as:first"), "unpaginated bodies carry no navigation");
        assertFalse(doc.containsKey("as:next"));
        assertEquals(base, linkTarget(resp, "rel=\"up\""), "non-root resources must link rel=up to their parent");
    }

    @Test
    void containerConditionalRequestsAnswer304() throws Exception {
        HttpResponse<String> resp = get("", "Accept", "application/lws+json");
        String etag = resp.headers().firstValue("ETag").orElseThrow();
        HttpResponse<String> cond = get("", "Accept", "application/lws+json", "If-None-Match", etag);
        assertEquals(304, cond.statusCode());
    }

    @Test
    void dataResourcesServeRanges() throws Exception {
        String content = "content-of-file-7";
        HttpResponse<String> full = get("f07.txt", "Accept", "text/plain");
        assertEquals(200, full.statusCode());
        assertEquals(content, full.body());
        assertEquals("bytes", full.headers().firstValue("Accept-Ranges").orElse(""));
        assertTrue(hasLink(full, "https://www.w3.org/ns/lws#DataResource"), "rel=type must name lws#DataResource");
        assertEquals(base, linkTarget(full, "rel=\"up\""));

        HttpResponse<String> part = get("f07.txt", "Accept", "text/plain", "Range", "bytes=2-6");
        assertEquals(206, part.statusCode());
        assertEquals("bytes 2-6/" + content.length(),
                part.headers().firstValue("Content-Range").orElse(""));
        assertEquals(content.substring(2, 7), part.body());

        HttpResponse<String> suffix = get("f07.txt", "Accept", "text/plain", "Range", "bytes=-4");
        assertEquals(206, suffix.statusCode());
        assertEquals(content.substring(content.length() - 4), suffix.body());

        HttpResponse<String> bad = get("f07.txt", "Accept", "text/plain", "Range", "bytes=999-");
        assertEquals(416, bad.statusCode());
        assertEquals("bytes */" + content.length(),
                bad.headers().firstValue("Content-Range").orElse(""));
    }

    @Test
    void headReturnsHeadersWithoutBody() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base)).timeout(java.time.Duration.ofSeconds(60))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .header("Accept", "application/lws+json").build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.headers().firstValue("ETag").isPresent());
        assertTrue(hasLink(resp, "rel=\"type\""));
        assertEquals("", resp.body(), "HEAD must not carry a body");
    }

    @Test
    void htmlViewOffersAParentLink() throws Exception {
        HttpResponse<String> sub = get("sub", "Accept", "text/html");
        assertEquals(200, sub.statusCode());
        assertTrue(sub.headers().firstValue("Content-Type").orElse("").startsWith("text/html"));
        assertTrue(sub.body().contains("Parent</a>"), "subcontainer HTML must offer a parent link");
        assertTrue(sub.body().contains("href=\"" + base + "\""), "the parent link must target the parent container");

        HttpResponse<String> root = get("", "Accept", "text/html");
        assertEquals(200, root.statusCode());
        assertFalse(root.body().contains("Parent</a>"), "the root has no parent to link");
    }

    @Test
    void storageDescriptionUsesTheLwsMediaType() throws Exception {
        HttpResponse<String> resp = get("description");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.headers().firstValue("Content-Type").orElse("").startsWith("application/lws+json"),
                "the storage description defaults to application/lws+json");
        JsonObject doc = parse(resp.body());
        assertEquals(base, doc.getString("id"));
        assertEquals("Storage", doc.getString("type"));
        assertTrue(hasLink(resp, LWSStorageServlet.REL_STORAGE_DESCRIPTION));

        HttpResponse<String> ld = get("description", "Accept", "application/ld+json");
        assertTrue(ld.headers().firstValue("Content-Type").orElse("").startsWith("application/ld+json"));
        assertEquals(resp.body(), ld.body(), "negotiated flavors share one payload");
    }

    @Test
    void advertisedIdentifiersAreValidIris() throws Exception {
        // BG-40: "big sub" must be advertised as big%20sub everywhere - JSON-LD
        // ids, Link targets and Turtle IRIs - not with a raw space.
        HttpResponse<String> root = get("", "Accept", "application/lws+json");
        assertEquals(200, root.statusCode());
        boolean sawBigSub = false;
        for (jakarta.json.JsonValue v : parse(root.body()).getJsonArray("items")) {
            String id = v.asJsonObject().getString("id");
            assertEquals(id, java.net.URI.create(id).toString(), "item id must be a valid IRI");
            assertFalse(id.contains(" "), id);
            if (id.endsWith("big%20sub")) sawBigSub = true;
        }
        assertTrue(sawBigSub, "the encoded 'big sub' container is listed on page 1");

        HttpResponse<String> p1 = get("big%20sub", "Accept", "application/lws+json");
        assertEquals(200, p1.statusCode());
        assertEquals(base + "big%20sub", parse(p1.body()).getString("id"));
        String linkset = linkTarget(p1, "rel=\"linkset\"");
        assertNotNull(linkset);
        assertTrue(linkset.contains("big%20sub.meta"), linkset);
        assertFalse(linkset.contains(" "), linkset);
        HttpResponse<String> child = get("big%20sub/g00.txt?format=turtle", "Accept", "text/turtle");
        assertEquals(200, child.statusCode(), child.body());
        String up = linkTarget(child, "rel=\"up\"");
        assertEquals(base + "big%20sub", up);
        org.apache.jena.rdf.model.Model desc = org.apache.jena.rdf.model.ModelFactory.createDefaultModel();
        org.apache.jena.riot.RDFDataMgr.read(desc, new java.io.ByteArrayInputStream(child.body().getBytes(StandardCharsets.UTF_8)),
                org.apache.jena.riot.Lang.TURTLE);
        assertTrue(desc.containsResource(desc.createResource(base + "big%20sub/g00.txt")), child.body());

        HttpResponse<String> ttl = get("big%20sub", "Accept", "text/turtle");
        assertEquals(200, ttl.statusCode(), ttl.body());
        org.apache.jena.rdf.model.Model container = org.apache.jena.rdf.model.ModelFactory.createDefaultModel();
        org.apache.jena.riot.RDFDataMgr.read(container, new java.io.ByteArrayInputStream(ttl.body().getBytes(StandardCharsets.UTF_8)),
                org.apache.jena.riot.Lang.TURTLE);
        assertTrue(container.containsResource(container.createResource(base + "big%20sub")), ttl.body());
        assertTrue(container.containsResource(container.createResource(base + "big%20sub/g00.txt")), ttl.body());
        HttpResponse<String> html = get("big%20sub", "Accept", "text/html");
        assertEquals(200, html.statusCode());
        assertTrue(html.body().contains("href=\"" + base + "\""), "the parent link of a top-level subcontainer is the root");
    }
}
