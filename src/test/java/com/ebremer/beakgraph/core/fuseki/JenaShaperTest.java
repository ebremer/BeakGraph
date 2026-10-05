package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFWriterRegistry;
import org.apache.jena.riot.RiotException;
import org.junit.jupiter.api.Test;

/**
 * The user-profile JSON-LD framing: applied by the endpoint's response filter
 * to a buffered response, never through Jena's global writer registry
 * (BG-216), never fetching a context (BG-431), never closing the caller's
 * stream (BG-430).
 */
class JenaShaperTest {

    private static final String DOC = """
        {"@context": {"geo": "http://www.opengis.net/ont/geosparql#", "dct": "http://purl.org/dc/terms/"},
         "@id": "http://ex.org/fc", "@type": "geo:FeatureCollection", "dct:title": "A collection"}
        """;

    @Test
    void theEndpointLeavesJenasJsonLdWritersAlone() throws Exception {
        Class.forName("com.ebremer.beakgraph.core.fuseki.SPARQLEndPoint"); // class init used to register the hook
        for (RDFFormat f : List.of(RDFFormat.JSONLD, RDFFormat.JSONLD_PRETTY, RDFFormat.JSONLD11_PRETTY)) {
            assertTrue(RDFWriterRegistry.getWriterGraphFactory(f).getClass().getName().startsWith("org.apache.jena"),
                    "graph writer for " + f + " must be Jena's own");
            assertTrue(RDFWriterRegistry.getWriterDatasetFactory(f).getClass().getName().startsWith("org.apache.jena"),
                    "dataset writer for " + f + " must be Jena's own");
        }
    }

    @Test
    void framingAppliesTheProfileContext() {
        String framed = JenaShaper.frame(DOC);
        assertTrue(framed.contains("\"geo\""), framed);
        assertTrue(framed.contains("\"title\""), "dct:title is compacted to the frame's term: " + framed);
    }

    @Test
    void aFrameNamingARemoteContextFailsWithoutFetching() {
        String frame = "{\"@context\": \"http://127.0.0.1:1/ctx.jsonld\", \"@type\": \"geo:FeatureCollection\"}";
        RiotException ex = assertThrows(RiotException.class, () -> JenaShaper.frame(DOC, frame));
        boolean refused = false;
        for (Throwable c = ex; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains("Remote contexts are disabled")) refused = true;
        }
        assertTrue(refused, "the loader must refuse, not fetch: " + ex);
    }

    @Test
    void frameToFlushesButNeverClosesTheCallersStream() throws Exception {
        boolean[] closed = {false};
        boolean[] flushed = {false};
        ByteArrayOutputStream out = new ByteArrayOutputStream() {
            @Override public void close() { closed[0] = true; }
            @Override public void flush() { flushed[0] = true; }
        };
        JenaShaper.frameTo(DOC.getBytes(StandardCharsets.UTF_8), out);
        assertTrue(out.size() > 0);
        assertTrue(flushed[0], "flushed, like Jena's writers");
        assertFalse(closed[0], "the servlet's stream is the container's to close");
    }
}
