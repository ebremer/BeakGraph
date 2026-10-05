package com.ebremer.beakgraph.core.fuseki;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.JsonLdErrorCode;
import com.apicatalog.jsonld.document.JsonDocument;
import com.apicatalog.jsonld.loader.DocumentLoader;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.jena.riot.RiotException;

/**
 * JSON-LD framing for the {@code application/ld+json;profile=user-profile}
 * responses of the SPARQL endpoints. The endpoint's response filter buffers a
 * JSON-LD response and hands it here; the framed document replaces it.
 * <p>
 * This used to register itself as the writer for every JSON-LD
 * {@code RDFFormat} in Jena's global registry, consulting a ThreadLocal to
 * decide whether to frame - so merely referencing {@code SPARQLEndPoint}
 * changed how the whole JVM (a library user's own {@code RDFDataMgr.write},
 * a CLI export) serialised JSON-LD (BG-216). Framing never dereferences
 * anything: the frame is a constant and the document loader refuses every
 * reference (BG-431), and nothing here closes a caller's stream (BG-430).
 */
public final class JenaShaper {

    private JenaShaper() {}

    /** The user-profile frame: a geo:FeatureCollection with dct:title / dct:publisher terms. */
    static final String DEFAULT_FRAME = """
            {
              "@context": {
                "geo": "http://www.opengis.net/ont/geosparql#",
                "dct": "http://purl.org/dc/terms/",
                "title": "dct:title",
                "publisher": {
                  "@id": "dct:publisher",
                  "@type": "@id"
                }
              },
              "@type": "geo:FeatureCollection"
            }
            """;

    /** No remote (or local) context is ever loaded while framing. */
    static final DocumentLoader NO_LOADING = (uri, options) -> {
        throw new JsonLdError(JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED, "Remote contexts are disabled: " + uri);
    };

    private static final JsonWriterFactory WRITERS =
            Json.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));

    /** Frames a JSON-LD document with {@link #DEFAULT_FRAME}. */
    public static String frame(String jsonLd) {
        return frame(jsonLd, DEFAULT_FRAME);
    }

    /** Frames a JSON-LD document with the given frame; a reference to any external context fails, no fetch is made. */
    public static String frame(String jsonLd, String frame) {
        try {
            JsonDocument doc = JsonDocument.of(new StringReader(jsonLd));
            JsonDocument frameDoc = JsonDocument.of(new StringReader(frame));
            JsonObject framed = JsonLd.frame(doc, frameDoc).loader(NO_LOADING).get();
            StringWriter sw = new StringWriter();
            JsonWriter writer = WRITERS.createWriter(sw);
            writer.writeObject(framed); // a StringWriter needs no close
            return sw.toString();
        } catch (Exception e) {
            throw new RiotException("Failed to apply JSON-LD Frame", e);
        }
    }

    /** UTF-8 in, UTF-8 out. */
    public static byte[] frame(byte[] jsonLd) {
        return frame(new String(jsonLd, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
    }

    /** Writes the framed document to {@code out} and flushes it; the stream is the caller's and is NOT closed. */
    public static void frameTo(byte[] jsonLd, OutputStream out) throws IOException {
        out.write(frame(jsonLd));
        out.flush();
    }
}
