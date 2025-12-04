package com.ebremer.beakgraph.sniff;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.document.JsonDocument;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFWriter;
import org.apache.jena.vocabulary.RDF;

import java.io.StringReader;
import java.util.Collections;
import java.util.Map;

public class JenaJsonLdFrameDemo {

    public static void main(String[] args) throws Exception {
        Model model = ModelFactory.createDefaultModel();
        String ns = "http://schema.org/";
        Resource book1 = model.createResource("http://library.io/book/1");
        Resource author1 = model.createResource("http://library.io/author/jdoe");
        book1.addProperty(RDF.type, model.createResource(ns + "Book"));
        book1.addProperty(model.createProperty(ns + "name"), "Jena for Beginners");
        author1.addProperty(RDF.type, model.createResource(ns + "Person"));
        author1.addProperty(model.createProperty(ns + "name"), "Jane Doe");
        book1.addProperty(model.createProperty(ns + "author"), author1);
        String frameJson = """
        {
          "@context": {
            "name": "http://schema.org/name",
            "author": { "@id": "http://schema.org/author", "@type": "@id" },
            "Book": "http://schema.org/Book",
            "Person": "http://schema.org/Person"
          },
          "@type": "Book",
          "author": {
            "@embed": "@always",
            "@type": "Person"
          }
        }
        """;
        String inputJsonLd = RDFWriter.create()
            .source(model)
            .format(RDFFormat.JSONLD11)
            .asString();
        JsonDocument inputDoc = JsonDocument.of(new StringReader(inputJsonLd));
        JsonDocument frameDoc = JsonDocument.of(new StringReader(frameJson));
        JsonObject framedOutput = JsonLd.frame(inputDoc, frameDoc).get();
        System.out.println("--- Framed JSON-LD Output ---");
        printPretty(framedOutput);
    }

    private static void printPretty(JsonObject json) {
        Map<String, Boolean> config = Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true);
        JsonWriterFactory writerFactory = Json.createWriterFactory(config);
        
        try (JsonWriter jsonWriter = writerFactory.createWriter(System.out)) {
            jsonWriter.writeObject(json);
        }
    }
}