package com.ebremer.beakgraph.core.fuseki;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
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
        assertEquals("https://base.example/", doc.getString("@id"));
        assertEquals("https://base.example/sparql",
            doc.getJsonArray("service").getJsonObject(1).getString("serviceEndpoint"));
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
}
