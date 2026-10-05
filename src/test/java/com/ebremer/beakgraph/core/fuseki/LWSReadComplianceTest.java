package com.ebremer.beakgraph.core.fuseki;

import org.junit.jupiter.api.parallel.Isolated;
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
 * Protocol 1.0 editor's draft of 2026-10-05 (w3c.github.io/lws-protocol/lws10-core/):
 * storage discovery (rel=lws#storage, the application/lws+cid storage
 * description), container representation shape and media-type negotiation,
 * link-based pagination (first/last/next/prev in Link headers, full totalItems
 * with page-scoped items), mandatory Link relations (type / up / linkset /
 * storage), RFC 9264 linksets, validators + conditional requests, single-range
 * requests on data resources, and the read-only method surface (405 + Allow,
 * RFC 9457 problem details). Runs a real Jetty server so header semantics are
 * the container's, not a mock's.
 */
// Mutates JVM-global state (system properties / ARQ modes / a shared server):
// never interleave with other classes should parallel execution be enabled (BG-189).
@Isolated
class LWSReadComplianceTest {

    @TempDir
    static Path dir;

    private static Server server;
    private static String base;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(10)).build();
    private static final String STORAGE_REL = "rel=\"" + LWSStorageServlet.REL_STORAGE + "\"";
    private static final int FILES = 45;
    /** Enough members to span 3+ pages at any PAGE_SIZE the guard admits. */
    private static final int BIGSUB_FILES = LWSStorageServlet.PAGE_SIZE * 2 + 2;
    /** Root membership: FILES + the "sub", "big sub", "live" and "HalcyonStorageArchive" directories. */
    private static final int ROOT_ITEMS = FILES + 4;
    private static Path root;

    @BeforeAll
    static void startServer() throws Exception {
        root = Files.createDirectories(dir.resolve("storage"));
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

        // A file whose bytes a test replaces after the model is generated (BG-388).
        Files.write(Files.createDirectories(root.resolve("live")).resolve("x.txt"), "v1".getBytes(StandardCharsets.UTF_8));
        // An entry literally named *.meta, and a directory sharing the alias's prefix (BG-47).
        Files.write(root.resolve("live").resolve("build.meta"), "meta-file".getBytes(StandardCharsets.UTF_8));
        Files.write(Files.createDirectories(root.resolve("HalcyonStorageArchive")).resolve("z.txt"), "zzz".getBytes(StandardCharsets.UTF_8));
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
        assertEquals(base + "description", linkTarget(resp, STORAGE_REL),
                "every GET/HEAD response links the storage URI with rel=lws#storage");
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
        assertEquals("text/plain", file.getString("format"), "format is required for DataResources");
        assertFalse(file.containsKey("mediaType"), "the LWS property is format, not mediaType");
        assertTrue(file.containsKey("size"));
        assertTrue(file.containsKey("modified"));
        assertNotNull(folder, "page 1 must contain the 'big sub' Container (URI sort puts it first)");
        assertFalse(folder.containsKey("format"), "containers carry no format");
        // The body is exactly the container representation: navigation is in
        // the Link headers, and the LWS context defines no term for it.
        assertEquals(java.util.Set.of("@context", "id", "type", "totalItems", "items"), doc.keySet());
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

        // Pagination is link-based: the JSON body carries no navigation of its own.
        JsonObject d1 = parse(p1.body());
        assertFalse(d1.containsKey("as:first") || d1.containsKey("as:next") || d1.containsKey("next"), p1.body());
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
        // RFC 9110: a list, a weak validator and * all match (BG-49); an unrelated tag does not.
        assertEquals(304, get("", "Accept", "application/lws+json", "If-None-Match", "\"nope\", " + etag).statusCode());
        assertEquals(304, get("", "Accept", "application/lws+json", "If-None-Match", "W/" + etag).statusCode());
        assertEquals(304, get("", "Accept", "application/lws+json", "If-None-Match", "*").statusCode());
        assertEquals(200, get("", "Accept", "application/lws+json", "If-None-Match", "\"nope\"").statusCode());
    }

    @Test
    void aReplacedFileIsServedWithNewBytesAndANewValidator() throws Exception {
        HttpResponse<String> before = get("live/x.txt", "Accept", "text/plain");
        assertEquals(200, before.statusCode());
        assertEquals("v1", before.body());
        String oldEtag = before.headers().firstValue("ETag").orElseThrow();
        assertEquals("no-cache", before.headers().firstValue("Cache-Control").orElse(""), "data responses must be revalidated (BG-389)");
        Files.write(root.resolve("live").resolve("x.txt"), "version-two".getBytes(StandardCharsets.UTF_8));

        HttpResponse<String> after = get("live/x.txt", "Accept", "text/plain");
        assertEquals("version-two", after.body());
        assertFalse(oldEtag.equals(after.headers().firstValue("ETag").orElse("")), "the validator follows the bytes");
        // A range conditional on the OLD validator gets the whole new representation, never mixed bytes.
        HttpResponse<String> ranged = get("live/x.txt", "Accept", "text/plain", "Range", "bytes=0-1", "If-Range", oldEtag);
        assertEquals(200, ranged.statusCode());
        assertEquals("version-two", ranged.body());
        HttpResponse<String> part = get("live/x.txt", "Accept", "text/plain", "Range", "bytes=0-6");
        assertEquals(206, part.statusCode());
        assertEquals("version", part.body());
        assertEquals("no-cache", part.headers().firstValue("Cache-Control").orElse(""));
        // The listing reports the file's LIVE size (BG-388), even though the model still holds the old one.
        JsonObject listing = parse(get("live", "Accept", "application/lws+json").body());
        JsonObject item = listing.getJsonArray("items").getValuesAs(JsonObject.class).stream()
                .filter(o -> o.getString("id").endsWith("/x.txt")).findFirst().orElseThrow();
        assertEquals("version-two".length(), item.getInt("size"));
    }

    @Test
    void literalMetaNamesAndAliasPrefixedNamesAreReachable() throws Exception {
        HttpResponse<String> metaFile = get("live/build.meta", "Accept", "text/plain");
        assertEquals(200, metaFile.statusCode());
        assertEquals("meta-file", metaFile.body(), "a stored entry named *.meta is that entry, not a linkset");
        HttpResponse<String> linkset = get("sub.meta");
        assertEquals(200, linkset.statusCode());
        assertTrue(contentType(linkset).startsWith("application/linkset+json"), "an absent *.meta name is still the linkset");
        HttpResponse<String> archive = get("HalcyonStorageArchive", "Accept", "application/lws+json");
        assertEquals(200, archive.statusCode(), archive.body());
        assertEquals(1, parse(archive.body()).getInt("totalItems"), "a name sharing the alias prefix is its own container");
        assertEquals("zzz", get("HalcyonStorageArchive/z.txt", "Accept", "text/plain").body());
        assertEquals("content-of-file-7", get("HalcyonStorage/f07.txt", "Accept", "text/plain").body(), "the exact alias still works");
    }

    @Test
    void contentDispositionCarriesBothForms() throws Exception {
        HttpResponse<String> r = get("f07.txt", "Accept", "text/plain");
        String cd = r.headers().firstValue("Content-Disposition").orElse("");
        assertTrue(cd.contains("filename=\"f07.txt\"") && cd.contains("filename*=UTF-8''f07.txt"), cd);
    }

    private static String contentType(HttpResponse<?> r) {
        return r.headers().firstValue("Content-Type").orElse("");
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
    void theStorageUriServesTheStorageDescription() throws Exception {
        // Discovery: follow the rel=lws#storage link from any resource.
        String storage = linkTarget(get("sub", "Accept", "application/lws+json"), STORAGE_REL);
        assertEquals(base + "description", storage);
        HttpResponse<String> resp = get(storage.substring(base.length()));
        assertEquals(200, resp.statusCode());
        assertEquals("application/lws+cid", contentType(resp), "the storage description defaults to application/lws+cid");
        JsonObject doc = parse(resp.body());
        assertEquals("https://www.w3.org/ns/cid/v1", doc.getJsonArray("@context").getString(0));
        assertEquals("https://www.w3.org/ns/lws/v1", doc.getJsonArray("@context").getString(1));
        assertEquals(storage, doc.getString("id"), "id is the storage URI");
        assertEquals("Storage", doc.getString("type"));
        JsonObject root = doc.getJsonArray("service").getValuesAs(JsonObject.class).stream()
                .filter(s -> "StorageRoot".equals(s.getString("type"))).findFirst().orElseThrow();
        assertEquals(base, root.getString("serviceEndpoint"), "StorageRoot names the root container");
        assertEquals(storage, linkTarget(resp, STORAGE_REL), "the description links the storage too");
        assertTrue(hasLink(resp, "https://www.w3.org/ns/lws#Storage"), "rel=type names lws#Storage");
        String etag = resp.headers().firstValue("ETag").orElseThrow(() -> new AssertionError("GET responses carry an ETag"));
        assertTrue(resp.headers().firstValue("Last-Modified").isPresent());
        assertEquals(304, get("description", "If-None-Match", etag).statusCode());

        HttpResponse<String> ld = get("description", "Accept", "application/ld+json");
        assertEquals("application/ld+json", contentType(ld));
        assertEquals(resp.body(), ld.body(), "negotiated flavors share one payload");
        assertFalse(etag.equals(ld.headers().firstValue("ETag").orElse("")), "each representation has its own validator");
        assertEquals("application/lws+cid", contentType(get("description", "Accept", "application/lws+json")),
                "a client accepting none of the description's types still gets the LWS type");
    }

    @Test
    void linksetsAreRfc9264DocumentsWithValidators() throws Exception {
        HttpResponse<String> file = get("sub/a.txt", "Accept", "text/plain");
        String linksetUri = linkTarget(file, "rel=\"linkset\"");
        assertEquals(base + "sub/a.txt.meta", linksetUri);
        HttpResponse<String> ls = get("sub/a.txt.meta");
        assertEquals(200, ls.statusCode(), ls.body());
        assertEquals("application/linkset+json", contentType(ls));
        assertEquals("GET, HEAD, OPTIONS", ls.headers().firstValue("Allow").orElse(""), "read-only: no PATCH is advertised");
        assertEquals(base + "description", linkTarget(ls, STORAGE_REL));
        String etag = ls.headers().firstValue("ETag").orElseThrow(() -> new AssertionError("linksets carry an ETag"));
        assertEquals(304, get("sub/a.txt.meta", "If-None-Match", etag).statusCode());

        JsonObject link = parse(ls.body()).getJsonArray("linkset").getJsonObject(0);
        assertEquals(base + "sub/a.txt", link.getString("anchor"));
        assertEquals("https://www.w3.org/ns/lws#DataResource", link.getJsonArray("type").getJsonObject(0).getString("href"));
        assertEquals(base + "sub", link.getJsonArray("up").getJsonObject(0).getString("href"));
        assertEquals(linksetUri, link.getJsonArray("linkset").getJsonObject(0).getString("href"));
        assertEquals(base + "description", link.getJsonArray(LWSStorageServlet.REL_STORAGE).getJsonObject(0).getString("href"));
        for (String notALink : new String[] {"size", "updated", "mediaType"}) {
            assertFalse(link.containsKey(notALink), notALink + " is not a link relation");
        }

        JsonObject rootLink = parse(get(".meta").body()).getJsonArray("linkset").getJsonObject(0);
        assertEquals(base, rootLink.getString("anchor"));
        assertFalse(rootLink.containsKey("up"), "the storage root has no parent");
        JsonObject storageLink = parse(get("description.meta").body()).getJsonArray("linkset").getJsonObject(0);
        assertEquals("https://www.w3.org/ns/lws#Storage", storageLink.getJsonArray("type").getJsonObject(0).getString("href"));
        assertFalse(storageLink.containsKey("up"), "the storage is not a contained resource");
    }

    @Test
    void writesAre405WithAnAllowHeader() throws Exception {
        String[][] attempts = {
            {"PUT", "f07.txt"}, {"DELETE", "f07.txt"}, {"PATCH", "f07.txt"},
            {"POST", ""}, {"POST", "sub"}, {"POST", "f07.txt"}, {"PUT", "sub/new.txt"},
            {"PATCH", "f07.txt.meta"}, {"PUT", "f07.txt.meta"}, {"DELETE", "sub"},
        };
        for (String[] a : attempts) {
            HttpResponse<String> r = send(a[0], a[1], "Content-Type", "application/json-patch+json");
            assertEquals(405, r.statusCode(), a[0] + " " + a[1]);
            assertEquals("GET, HEAD, OPTIONS", r.headers().firstValue("Allow").orElse(""), a[0] + " " + a[1] + ": RFC 9110 requires Allow on 405");
            assertEquals("application/problem+json", contentType(r), a[0] + " " + a[1]);
        }
        assertEquals("content-of-file-7", get("f07.txt").body(), "nothing was written");

        HttpResponse<String> options = send("OPTIONS", "sub");
        assertEquals(200, options.statusCode());
        assertEquals("GET, HEAD, OPTIONS", options.headers().firstValue("Allow").orElse(""));
        HttpResponse<String> trace = send("TRACE", "f07.txt");
        assertEquals(405, trace.statusCode(), "TRACE must not echo the request");
    }

    @Test
    void errorsAreProblemDetails() throws Exception {
        HttpResponse<String> bad = get("?page=x", "Accept", "application/lws+json");
        assertEquals(400, bad.statusCode());
        assertEquals("application/problem+json", contentType(bad));
        JsonObject problem = parse(bad.body());
        assertEquals(400, problem.getInt("status"));
        assertEquals("Bad Request", problem.getString("title"));
        assertTrue(problem.getString("detail").contains("page"), bad.body());

        HttpResponse<String> missing = get("no-such-file.txt");
        assertEquals(404, missing.statusCode());
        assertEquals("application/problem+json", contentType(missing));
        assertFalse(missing.body().contains("HalcyonStorage") || missing.body().contains("localhost:8888"),
                "the canonical model URI must not leak: " + missing.body());
    }

    @Test
    void theLwsProfileOfJsonLdIsHonoured() throws Exception {
        String profiled = "application/ld+json; profile=\"https://www.w3.org/ns/lws/v1\"";
        HttpResponse<String> r = get("sub", "Accept", profiled);
        assertEquals(200, r.statusCode());
        assertEquals(profiled, contentType(r), "the requested media type is the response's Content-Type");
        assertEquals(get("sub", "Accept", "application/lws+json").body(), r.body());
        assertEquals("application/lws+json", contentType(get("sub", "Accept", "application/lws+json")),
                "no charset parameter is appended to the echoed type");
        assertTrue(r.headers().allValues("Vary").stream().anyMatch(v -> v.contains("Accept")));
    }

    @Test
    void preconditionsFailWith412() throws Exception {
        HttpResponse<String> listing = get("sub", "Accept", "application/lws+json");
        String etag = listing.headers().firstValue("ETag").orElseThrow();
        assertEquals(200, get("sub", "Accept", "application/lws+json", "If-Match", etag).statusCode());
        assertEquals(412, get("sub", "Accept", "application/lws+json", "If-Match", "\"stale\"").statusCode());
        assertEquals(412, get("sub", "Accept", "application/lws+json", "If-Unmodified-Since", "Sun, 06 Nov 1994 08:49:37 GMT").statusCode());
        assertEquals(200, get("sub", "Accept", "application/lws+json", "If-Modified-Since", "not a date").statusCode(),
                "an invalid date is ignored, not a 500");
        HttpResponse<String> data = get("sub/a.txt");
        assertEquals(412, get("sub/a.txt", "If-Match", "W/" + data.headers().firstValue("ETag").orElseThrow()).statusCode(),
                "If-Match compares strongly");
        // HTML and JSON listings of one container are distinct representations.
        assertFalse(etag.equals(get("sub", "Accept", "text/html").headers().firstValue("ETag").orElse("")));
    }

    private static HttpResponse<String> send(String method, String path, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(java.time.Duration.ofSeconds(60));
        for (int i = 0; i < headers.length; i += 2) {
            b.header(headers[i], headers[i + 1]);
        }
        HttpRequest.BodyPublisher body = method.equals("OPTIONS") || method.equals("TRACE") || method.equals("DELETE")
                ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString("[]");
        return http.send(b.method(method, body).build(), HttpResponse.BodyHandlers.ofString());
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
