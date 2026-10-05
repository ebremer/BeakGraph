package com.ebremer.beakgraph.core.fuseki;

import org.junit.jupiter.api.io.TempDir;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Security-focused tests for the LWS storage servlet: JSON values must be escaped (not concatenated)
 * and request paths must stay inside the storage root. The JSON tests also confirm a JSON-P provider
 * (Parsson) is on the runtime classpath, since the servlet now builds responses with jakarta.json.
 */
class LWSStorageServletTest {

    private static JsonObject parse(String json) {
        try (JsonReader r = Json.createReader(new StringReader(json))) {
            return r.readObject();
        }
    }

    @Test
    void linksetJsonEscapesValuesAndIsWellFormed() {
        // A value carrying characters that would break naive string interpolation / inject JSON.
        String hostile = "http://h/12\" } , \n \\ \t inject";
        String json = LWSStorageServlet.linksetJson(
            hostile, "https://www.w3.org/ns/lws#DataResource", "http://h/", "http://h/r.meta", "http://h/description");

        JsonObject link = parse(json).getJsonArray("linkset").getJsonObject(0); // throws if malformed
        // The hostile value survives a full write -> parse round-trip unchanged: it was escaped, not injected.
        assertEquals(hostile, link.getString("anchor"));
        assertEquals("https://www.w3.org/ns/lws#DataResource", link.getJsonArray("type").getJsonObject(0).getString("href"));
        assertEquals("http://h/", link.getJsonArray("up").getJsonObject(0).getString("href"));
        JsonObject self = link.getJsonArray("linkset").getJsonObject(0);
        assertEquals("http://h/r.meta", self.getString("href"));
        assertEquals("application/linkset+json", self.getString("type"));
        assertEquals("http://h/description",
            link.getJsonArray("https://www.w3.org/ns/lws#storage").getJsonObject(0).getString("href"));
        // RFC 9264: every member but the anchor is a link relation - no size,
        // updated or mediaType pseudo-relations with non-URI "hrefs".
        assertEquals(java.util.Set.of("anchor", "type", "up", "linkset", "https://www.w3.org/ns/lws#storage"), link.keySet());
        assertFalse(parse(LWSStorageServlet.linksetJson("http://h/", "https://www.w3.org/ns/lws#Container", null,
                "http://h/.meta", "http://h/description")).getJsonArray("linkset").getJsonObject(0).containsKey("up"),
                "the root has no parent");
    }

    @Test
    void descriptionJsonFollowsTheStorageDescriptionDataModel() {
        JsonObject doc = parse(LWSStorageServlet.descriptionJson("https://base.example/"));
        // LWS Discovery: @context is an array starting with the CID and LWS contexts.
        assertEquals(java.util.List.of("https://www.w3.org/ns/cid/v1", "https://www.w3.org/ns/lws/v1"),
            doc.getJsonArray("@context").getValuesAs(jakarta.json.JsonString.class).stream()
                .map(jakarta.json.JsonString::getString).toList());
        // id is the storage URI - per CID also the URL the document is served at.
        assertEquals("https://base.example/description", doc.getString("id"));
        assertEquals("Storage", doc.getString("type"));
        JsonObject root = doc.getJsonArray("service").getJsonObject(0);
        assertEquals("StorageRoot", root.getString("type"), "the StorageRoot service is mandatory");
        assertEquals("https://base.example/", root.getString("serviceEndpoint"), "it names the root container");
        JsonObject sparql = doc.getJsonArray("service").getJsonObject(1);
        assertEquals("http://www.w3.org/ns/sparql-service-description#Service", sparql.getString("type"));
        assertEquals("https://base.example/rdf", sparql.getString("serviceEndpoint"));
    }

    @Test
    void theLwsProfileOfJsonLdIsEchoed() {
        String profiled = "application/ld+json; profile=\"https://www.w3.org/ns/lws/v1\"";
        assertTrue(LWSStorageServlet.requestsLwsProfile(profiled));
        assertTrue(LWSStorageServlet.requestsLwsProfile("text/html;q=0.5, application/ld+json;profile=https://www.w3.org/ns/lws/v1"));
        assertTrue(LWSStorageServlet.requestsLwsProfile("application/ld+json;profile=\"http://ex.org/a https://www.w3.org/ns/lws/v1\""),
                "profile is a space-separated list");
        assertFalse(LWSStorageServlet.requestsLwsProfile("application/ld+json"));
        assertFalse(LWSStorageServlet.requestsLwsProfile("application/ld+json;profile=user-profile"));
        assertFalse(LWSStorageServlet.requestsLwsProfile("application/ld+json;profile=\"https://www.w3.org/ns/lws/v1\";q=0"),
                "q=0 excludes the range");
        assertFalse(LWSStorageServlet.requestsLwsProfile(null));
        assertEquals(profiled, LWSStorageServlet.containerContentType("application/ld+json", profiled));
        assertEquals("application/ld+json", LWSStorageServlet.containerContentType("application/ld+json", "application/ld+json"));
        assertEquals("application/lws+json", LWSStorageServlet.containerContentType("application/lws+json", profiled + ", application/lws+json"));
    }

    @Test
    void ifMatchComparesStrongly() {
        assertTrue(LWSStorageServlet.ifMatchMatches(java.util.List.of("\"a\""), "\"a\""));
        assertTrue(LWSStorageServlet.ifMatchMatches(java.util.List.of("\"x\", \"a\""), "\"a\""));
        assertTrue(LWSStorageServlet.ifMatchMatches(java.util.List.of("*"), "\"a\""));
        assertFalse(LWSStorageServlet.ifMatchMatches(java.util.List.of("W/\"a\""), "\"a\""), "a weak tag never matches If-Match");
        assertFalse(LWSStorageServlet.ifMatchMatches(java.util.List.of("\"b\""), "\"a\""));
    }

    @Test
    void entityTagsDifferPerRepresentation() {
        byte[] body = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertFalse(LWSStorageServlet.contentEtag("application/lws+cid", body)
                .equals(LWSStorageServlet.contentEtag("application/json", body)), "same bytes, different representation");
        assertEquals(LWSStorageServlet.contentEtag("application/json", body), LWSStorageServlet.contentEtag("application/json", body));
        assertFalse(LWSStorageServlet.representationEtag("\"v1\"", "text/html")
                .equals(LWSStorageServlet.representationEtag("\"v1\"", "application/lws+json")));
        assertFalse(LWSStorageServlet.representationEtag("\"v1\"", "application/lws+json")
                .equals(LWSStorageServlet.representationEtag("\"v2\"", "application/lws+json")), "the listing version still counts");
    }

    @Test
    void parseRangeHandlesTheSingleRangeForms() {
        long size = 100;
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{2, 5},
            LWSStorageServlet.parseRange("bytes=2-5", size));
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{90, 99},
            LWSStorageServlet.parseRange("bytes=90-", size), "open-ended range runs to EOF");
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{90, 99},
            LWSStorageServlet.parseRange("bytes=-10", size), "suffix range takes the last N bytes");
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{0, 99},
            LWSStorageServlet.parseRange("bytes=0-500", size), "end clamps to size-1");
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{-1, -1},
            LWSStorageServlet.parseRange("bytes=100-", size), "start at/after EOF is unsatisfiable");
        org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{-1, -1},
            LWSStorageServlet.parseRange("bytes=-0", size), "empty suffix is unsatisfiable");
        assertNull(LWSStorageServlet.parseRange(null, size), "no header -> full response");
        assertNull(LWSStorageServlet.parseRange("bytes=0-1,5-9", size), "multi-range is ignored");
        assertNull(LWSStorageServlet.parseRange("items=0-5", size), "non-bytes unit is ignored");
        assertNull(LWSStorageServlet.parseRange("bytes=x-y", size), "garbage is ignored");
        assertNull(LWSStorageServlet.parseRange("bytes=5-2", size), "inverted range is ignored");
    }

    @Test
    void resolveWithinRejectsTraversalAndAbsolutePaths(@TempDir Path tmp) throws Exception {
        Path root = tmp.toRealPath();
        Path base = root.toAbsolutePath().normalize();

        // A normal relative path resolves inside the root.
        Path ok = LWSStorageServlet.resolveWithin(root, "sub/file.h5");
        assertEquals(base.resolve("sub/file.h5"), ok);
        assertTrue(ok.startsWith(base));
        // The empty path is the root itself (callers then reject directories).
        assertEquals(base, LWSStorageServlet.resolveWithin(root, ""));

        // Traversal and absolute paths are rejected.
        assertNull(LWSStorageServlet.resolveWithin(root, "../../../../etc/passwd"));
        assertNull(LWSStorageServlet.resolveWithin(root, "sub/../../escape"));
        assertNull(LWSStorageServlet.resolveWithin(root, base.getParent().resolve("evil.h5").toString()));
        // No storage root configured -> no local file.
        assertNull(LWSStorageServlet.resolveWithin(null, "anything"));
    }

    @Test
    void escapeHtmlNeutralizesMarkupAndAttributeBreakouts() {
        assertEquals("&lt;script&gt;alert(1)&lt;/script&gt;",
            LWSStorageServlet.escapeHtml("<script>alert(1)</script>"));
        assertEquals("a&quot; onmouseover=&quot;x", LWSStorageServlet.escapeHtml("a\" onmouseover=\"x"));
        assertEquals("a&amp;b&#39;c", LWSStorageServlet.escapeHtml("a&b'c"));
        assertEquals("plain-name.h5", LWSStorageServlet.escapeHtml("plain-name.h5"));
    }

    @Test
    void encodeHrefPercentEncodesUnsafeCharactersButKeepsSlashes() {
        assertEquals("/sub/a%20b.h5", LWSStorageServlet.encodeHref("/sub/a b.h5"));
        assertEquals("/x%22y", LWSStorageServlet.encodeHref("/x\"y"));
        assertEquals("/plain/file.h5", LWSStorageServlet.encodeHref("/plain/file.h5"));
    }

    @Test
    void toLiveUriMapsCanonicalUrisWithoutDoubledSlashes() {
        String canonicalRoot = com.ebremer.beakgraph.lws.LWSMetadataGenerator.CANONICAL_BASE;
        // Live bases end with '/'; canonical remainders start with '/': the old
        // concatenation minted "http://host//name" URIs that 404 when followed.
        assertEquals("http://example:9999/sub/file.h5",
            LWSStorageServlet.toLiveUri(canonicalRoot + "/sub/file.h5", "http://example:9999/"));
        assertEquals("http://example:9999/",
            LWSStorageServlet.toLiveUri(canonicalRoot, "http://example:9999/"));
    }

    @Test
    void decodePathDecodesPercentEncodingAndRejectsGarbage() {
        // getRequestURI() is undecoded; the metadata model and the filesystem
        // hold raw names, so "a b.h5" must be reachable via its encoded link.
        assertEquals("/a b.h5", LWSStorageServlet.decodePath("/a%20b.h5"));
        assertEquals("/x/y.h5", LWSStorageServlet.decodePath("/x/y.h5"));
        // '+' is a literal plus in a path (URLDecoder would have eaten it).
        assertEquals("/a+b.h5", LWSStorageServlet.decodePath("/a+b.h5"));
        assertNull(LWSStorageServlet.decodePath("/bad%zz"));
    }

    @Test
    void wildcardAcceptHeadersGetARepresentationNotA406() {
        assertTrue(LWSStorageServlet.acceptsHtmlRepresentation(""));      // no Accept at all
        assertTrue(LWSStorageServlet.acceptsHtmlRepresentation("*/*"));   // curl's default
        assertTrue(LWSStorageServlet.acceptsHtmlRepresentation("text/*"));
        assertTrue(LWSStorageServlet.acceptsHtmlRepresentation("text/html,application/xhtml+xml"));
        assertFalse(LWSStorageServlet.acceptsHtmlRepresentation("application/ld+json"));
        assertFalse(LWSStorageServlet.acceptsHtmlRepresentation("text/turtle"));
    }

    @Test
    void negotiationHonoursQualityAndWildcards() {
        java.util.List<String> offered = LWSStorageServlet.CONTAINER_TYPES;
        assertEquals("text/html", LWSStorageServlet.negotiate(null, offered), "no Accept: the first offered type");
        assertEquals("text/html", LWSStorageServlet.negotiate("*/*", offered), "curl's default");
        assertEquals("application/ld+json", LWSStorageServlet.negotiate("text/turtle;q=0.1, application/ld+json", offered));
        assertEquals("application/json", LWSStorageServlet.negotiate("application/json", offered),
                "a plain application/json request is answered as application/json, not ld+json");
        assertEquals("application/ld+json", LWSStorageServlet.negotiate("application/ld+json;profile=user-profile", offered),
                "parameters other than q are ignored");
        assertNull(LWSStorageServlet.negotiate("text/turtle;q=0", java.util.List.of("text/turtle")), "q=0 excludes");
        assertEquals("text/html", LWSStorageServlet.negotiate("text/turtle;q=0, */*", offered), "excluded, then anything else");
        assertEquals("text/turtle", LWSStorageServlet.negotiate("text/turtle, */*;q=0.1", offered), "Jena's shape: the exact type wins over */*");
        assertEquals("text/turtle", LWSStorageServlet.negotiate("text/*", java.util.List.of("application/json", "text/turtle")));
        assertNull(LWSStorageServlet.negotiate("image/png", offered), "nothing acceptable: 406");
        assertEquals("application/lws+json", LWSStorageServlet.negotiate("application/json;q=0.5, application/lws+json;q=0.9", offered));
    }

    @Test
    void contentDispositionEscapesAndEncodes() {
        assertEquals("attachment; filename=\"a\\\\b\\\"c.txt\"; filename*=UTF-8''a%5Cb%22c.txt",
                LWSStorageServlet.contentDisposition("a\\b\"c.txt"));
        assertEquals("attachment; filename=\"_bersicht.pdf\"; filename*=UTF-8''%C3%9Cbersicht.pdf",
                LWSStorageServlet.contentDisposition("\u00dcbersicht.pdf"));
        assertEquals("attachment; filename=\"trailing\\\\\"; filename*=UTF-8''trailing%5C",
                LWSStorageServlet.contentDisposition("trailing\\"));
        assertEquals("attachment; filename=\"plain file.txt\"; filename*=UTF-8''plain%20file.txt",
                LWSStorageServlet.contentDisposition("plain file.txt"));
    }

    @Test
    void ifNoneMatchIsAWeakListComparison() {
        String etag = "\"abc\"";
        assertTrue(LWSStorageServlet.ifNoneMatchMatches(java.util.List.of("\"abc\""), etag));
        assertTrue(LWSStorageServlet.ifNoneMatchMatches(java.util.List.of("\"nope\", \"abc\""), etag), "a list");
        assertTrue(LWSStorageServlet.ifNoneMatchMatches(java.util.List.of("W/\"abc\""), etag), "a weak validator");
        assertTrue(LWSStorageServlet.ifNoneMatchMatches(java.util.List.of("*"), etag), "the wildcard");
        assertTrue(LWSStorageServlet.ifNoneMatchMatches(java.util.List.of("\"x\"", "\"abc\""), etag), "a repeated field");
        assertFalse(LWSStorageServlet.ifNoneMatchMatches(java.util.List.of("\"nope\""), etag));
        assertFalse(LWSStorageServlet.ifNoneMatchMatches(java.util.List.of(""), etag));
    }

    @Test
    void aliasIsOnlyTheExactSegment() {
        assertEquals("", LWSStorageServlet.stripAlias("HalcyonStorage"));
        assertEquals("x/y", LWSStorageServlet.stripAlias("HalcyonStorage/x/y"));
        assertEquals("HalcyonStorageArchive/z", LWSStorageServlet.stripAlias("HalcyonStorageArchive/z"), "a name sharing the prefix is not the alias");
        assertEquals("plain", LWSStorageServlet.stripAlias("plain"));
    }

    @Test
    void configuredBaseIsNormalizedPerInstance() {
        assertNull(LWSStorageServlet.normalizeBase(null));
        assertNull(LWSStorageServlet.normalizeBase("  "));
        assertEquals("https://x.example/", LWSStorageServlet.normalizeBase("https://x.example"));
        assertEquals("https://x.example/", LWSStorageServlet.normalizeBase("https://x.example/"));
    }

    @Test
    void fileUrisAreNotExposableToClients() {
        org.apache.jena.rdf.model.Model m = org.apache.jena.rdf.model.ModelFactory.createDefaultModel();
        assertFalse(LWSStorageServlet.exposableToClient(
            m.createResource("file:///C:/secret/storage/data.h5")), "server paths must be redacted");
        assertTrue(LWSStorageServlet.exposableToClient(m.createResource("https://example.org/r")));
        assertTrue(LWSStorageServlet.exposableToClient(m.createLiteral("file:///looks-like-but-is-a-literal")));
        assertTrue(LWSStorageServlet.exposableToClient(m.createResource())); // bnode
    }

    @Test
    void sparqlBodyReadIsBounded() throws Exception {
        String small = "SELECT * WHERE { ?s ?p ?o }";
        assertEquals(small, BGSparqlService.readBody(
            new java.io.ByteArrayInputStream(small.getBytes(java.nio.charset.StandardCharsets.UTF_8)), 1024));
        byte[] huge = new byte[2048];
        java.util.Arrays.fill(huge, (byte) 'x');
        org.junit.jupiter.api.Assertions.assertThrows(
            BGSparqlService.QueryBodyTooLargeException.class,
            () -> BGSparqlService.readBody(new java.io.ByteArrayInputStream(huge), 1024));
    }

    @Test
    void resolveWithinRejectsSymlinkEscape(@TempDir Path tmp) throws Exception {
        // Both inside the @TempDir (cleaned up by JUnit, BG-187), the file
        // OUTSIDE the servlet root so the containment check is still exercised.
        Path root = Files.createDirectories(tmp.resolve("root")).toRealPath();
        Path outside = Files.writeString(tmp.resolve("outside.txt"), "");
        Path link = root.resolve("escape");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (IOException | UnsupportedOperationException e) {
            Assumptions.abort("symbolic links not supported in this environment: " + e.getMessage());
        }
        // The link is lexically inside the root but really points outside, so it must be rejected.
        assertNull(LWSStorageServlet.resolveWithin(root, "escape"));
    }

    @Test
    void toLiveUriPercentEncodesRawNames() {
        // BG-40: the canonical model holds raw file names; every advertised
        // id, Link target and RDF subject must be a valid IRI.
        String canonicalRoot = com.ebremer.beakgraph.lws.LWSMetadataGenerator.CANONICAL_BASE;
        assertEquals("http://example:9999/big%20sub/a%20b.h5",
                LWSStorageServlet.toLiveUri(canonicalRoot + "/big sub/a b.h5", "http://example:9999/"));
        assertEquals("http://example:9999/100%25%20sure%23tag",
                LWSStorageServlet.toLiveUri(canonicalRoot + "/100% sure#tag", "http://example:9999/"));
        assertEquals("http://example:9999/plain/file.h5",
                LWSStorageServlet.toLiveUri(canonicalRoot + "/plain/file.h5", "http://example:9999/"), "nothing to encode: unchanged");
    }
}
