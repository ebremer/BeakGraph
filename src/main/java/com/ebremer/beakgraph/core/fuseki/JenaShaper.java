package com.ebremer.beakgraph.core.fuseki;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.document.JsonDocument;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;

import org.apache.jena.graph.Graph;
import org.apache.jena.riot.*;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.writer.JsonLD11Writer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.util.Context;

import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;

public class JenaShaper {

    // The secret switch triggered by your Jetty Filter
    public static final ThreadLocal<Boolean> USE_PROFILE = ThreadLocal.withInitial(() -> false);

    public static void init() {
        String userFrame = """
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

        WriterGraphRIOTFactory graphFactory = (RDFFormat format) -> new WriterGraphRIOT() {
            @Override public Lang getLang() { return Lang.JSONLD; }

            @Override
            public void write(OutputStream out, Graph graph, PrefixMap prefixMap, String baseURI, Context context) {
                if (USE_PROFILE.get()) {
                    applyFrame(DatasetGraphFactory.wrap(graph), prefixMap, baseURI, context, userFrame, out, null);
                } else {
                    new JsonLD11Writer(RDFFormat.JSONLD11_PRETTY).write(out, DatasetGraphFactory.wrap(graph), prefixMap, baseURI, context);
                }
            }

            @Override
            public void write(Writer out, Graph graph, PrefixMap prefixMap, String baseURI, Context context) {
                if (USE_PROFILE.get()) {
                    applyFrame(DatasetGraphFactory.wrap(graph), prefixMap, baseURI, context, userFrame, null, out);
                } else {
                    new JsonLD11Writer(RDFFormat.JSONLD11_PRETTY).write(out, DatasetGraphFactory.wrap(graph), prefixMap, baseURI, context);
                }
            }
        };

        WriterDatasetRIOTFactory datasetFactory = (RDFFormat format) -> new WriterDatasetRIOT() {
            @Override public Lang getLang() { return Lang.JSONLD; }

            @Override
            public void write(OutputStream out, DatasetGraph dataset, PrefixMap prefixMap, String baseURI, Context context) {
                if (USE_PROFILE.get()) {
                    applyFrame(dataset, prefixMap, baseURI, context, userFrame, out, null);
                } else {
                    new JsonLD11Writer(RDFFormat.JSONLD11_PRETTY).write(out, dataset, prefixMap, baseURI, context);
                }
            }

            @Override
            public void write(Writer out, DatasetGraph dataset, PrefixMap prefixMap, String baseURI, Context context) {
                if (USE_PROFILE.get()) {
                    applyFrame(dataset, prefixMap, baseURI, context, userFrame, null, out);
                } else {
                    new JsonLD11Writer(RDFFormat.JSONLD11_PRETTY).write(out, dataset, prefixMap, baseURI, context);
                }
            }
        };

        // Overwrite the default Jena JSON-LD writers globally
        RDFWriterRegistry.register(RDFFormat.JSONLD, graphFactory);
        RDFWriterRegistry.register(RDFFormat.JSONLD_PRETTY, graphFactory);
        RDFWriterRegistry.register(RDFFormat.JSONLD11_PRETTY, graphFactory);

        RDFWriterRegistry.register(RDFFormat.JSONLD, datasetFactory);
        RDFWriterRegistry.register(RDFFormat.JSONLD_PRETTY, datasetFactory);
        RDFWriterRegistry.register(RDFFormat.JSONLD11_PRETTY, datasetFactory);

        System.out.println("JenaShaper: Native Titanium framing logic initialized.");
    }

    /**
     * Extracts raw JSON-LD from Jena and manually processes it through Titanium's Frame algorithm.
     */
    private static void applyFrame(DatasetGraph dataset, PrefixMap prefixMap, String baseURI, Context context, String frameStr, OutputStream outStream, Writer outWriter) {
        try {
            // 1. Get plain JSON-LD from Jena
            StringWriter sw = new StringWriter();
            new JsonLD11Writer(RDFFormat.JSONLD11_PLAIN).write(sw, dataset, prefixMap, baseURI, context);
            String rawJson = sw.toString();

            // 2. Parse into Titanium Documents
            JsonDocument doc = JsonDocument.of(new StringReader(rawJson));
            JsonDocument frameDoc = JsonDocument.of(new StringReader(frameStr));

            // 3. Execute W3C JSON-LD 1.1 Framing!
            JsonObject framed = JsonLd.frame(doc, frameDoc).get();

            // 4. Output the beautiful formatted JSON to the client
            JsonWriterFactory writerFactory = Json.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));
            if (outStream != null) {
                try (JsonWriter jsonWriter = writerFactory.createWriter(outStream)) {
                    jsonWriter.writeObject(framed);
                }
            } else {
                try (JsonWriter jsonWriter = writerFactory.createWriter(outWriter)) {
                    jsonWriter.writeObject(framed);
                }
            }
        } catch (Exception e) {
            throw new RiotException("Failed to apply JSON-LD Frame", e);
        }
    }
}