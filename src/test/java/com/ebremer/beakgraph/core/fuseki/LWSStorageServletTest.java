package com.ebremer.beakgraph.core.fuseki;

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
        String hostile = "12\" } , \n \\ \t inject";
        String json = LWSStorageServlet.linksetJson(
            "http://h/r", "https://halcyon.is/ns/DataResource", "http://h/", "text/plain", hostile, null);

        JsonObject link = parse(json).getJsonArray("linkset").getJsonObject(0); // throws if malformed
        assertEquals("http://h/r", link.getString("anchor"));
        assertEquals("text/plain", link.getJsonArray("mediaType").getJsonObject(0).getString("href"));
        // The hostile value survives a full write -> parse round-trip unchanged: it was escaped, not injected.
        assertEquals(hostile, link.getJsonArray("size").getJsonObject(0).getString("href"));
        assertFalse(link.containsKey("updated"), "null relation must be omitted");
    }

    @Test
    void descriptionJsonIsWellFormed() {
        JsonObject doc = parse(LWSStorageServlet.descriptionJson("https://base.example/"));
        assertEquals("Storage", doc.getString("type"));
        // LWS Discovery data model: the storage is identified by "id" (the lws/v1
        // context maps it to @id), and the StorageDescription service is mandatory.
        assertEquals("https://base.example/", doc.getString("id"));
        assertEquals("StorageDescription",
            doc.getJsonArray("service").getJsonObject(0).getString("type"));
        assertEquals("https://base.example/description",
            doc.getJsonArray("service").getJsonObject(0).getString("serviceEndpoint"));
        assertEquals("https://base.example/rdf",
            doc.getJsonArray("service").getJsonObject(1).getString("serviceEndpoint"));
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
    void resolveWithinRejectsTraversalAndAbsolutePaths() throws Exception {
        Path root = Files.createTempDirectory("lws-test").toRealPath();
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
    void resolveWithinRejectsSymlinkEscape() throws Exception {
        Path root = Files.createTempDirectory("lws-sym").toRealPath();
        Path outside = Files.createTempFile("lws-outside", ".txt");   // a real file outside the root
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
