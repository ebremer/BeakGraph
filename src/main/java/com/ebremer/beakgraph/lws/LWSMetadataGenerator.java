package com.ebremer.beakgraph.lws;

import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPOutputStream;

public class LWSMetadataGenerator {

    // LWS Vocab Constants
    private static final String LWS_NS = "https://www.w3.org/ns/lws#";
    private static final String AS_NS = "https://www.w3.org/ns/activitystreams#";
    private static final String SCHEMA_NS = "https://schema.org/";

    public static void main(String[] args) {
        Path rootPath = Paths.get("D:\\HalcyonStorage");
        Path outputPath = Paths.get("D:\\HalcyonStorage\\beakgraph.ttl.gz");

        try {
            Model model = generateLWSModel(rootPath);
            writeModelToGZ(model, outputPath);
            System.out.println("Metadata successfully written to " + outputPath.toAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates a Jena Model representing the LWS Container and all members.
     * @param rootPath
     * @return 
     * @throws java.io.IOException
     */
    public static Model generateLWSModel(Path rootPath) throws IOException {
        Model model = ModelFactory.createDefaultModel();
        model.setNsPrefix("lws", LWS_NS);
        model.setNsPrefix("as", AS_NS);
        model.setNsPrefix("sdo", SCHEMA_NS);
        model.setNsPrefix("xsd", XSD.NS);
        Property items = model.createProperty(LWS_NS, "items");
        Property totalItems = model.createProperty(AS_NS, "totalItems");
        Property mediaType = model.createProperty(AS_NS, "mediaType");
        Property size = model.createProperty(SCHEMA_NS, "size");
        Property modified = model.createProperty(AS_NS, "updated");
        Resource containerType = model.createResource(LWS_NS + "Container");
        Resource dataType = model.createResource(LWS_NS + "DataResource");
        Resource rootResource = model.createResource(rootPath.toUri().toString());
        rootResource.addProperty(RDF.type, containerType);
        Files.walkFileTree(rootPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                processResource(model, file, attrs, dataType, mediaType, size, modified);
                linkToParent(model, file, rootPath, items, totalItems);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (!dir.equals(rootPath)) {
                    processResource(model, dir, attrs, containerType, null, null, modified);
                    linkToParent(model, dir, rootPath, items, totalItems);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return model;
    }

    private static void processResource(Model model, Path path, BasicFileAttributes attrs, Resource type, Property pMediaType, Property pSize, Property pModified) {
        Resource res = model.createResource(path.toUri().toString());
        res.addProperty(RDF.type, type);
        String isoDate = attrs.lastModifiedTime().toInstant()
                .atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ISO_INSTANT);
        res.addProperty(pModified, model.createTypedLiteral(isoDate, XSD.dateTime.getURI()));
        if (pSize != null) {
            res.addProperty(pSize, model.createTypedLiteral(attrs.size(), XSD.integer.getURI()));
        }
        if (pMediaType != null) {
            String contentType = "application/octet-stream";
            try {
                String probed = Files.probeContentType(path);
                if (probed != null) contentType = probed;
            } catch (IOException ignored) {}
            res.addProperty(pMediaType, contentType);
        }
    }

    private static void linkToParent(Model model, Path path, Path rootPath, Property pItems, Property pTotal) {
        Path parentPath = path.getParent();
        if (parentPath != null && path.startsWith(rootPath)) {
            Resource parentRes = model.createResource(parentPath.toUri().toString());
            Resource childRes = model.createResource(path.toUri().toString());
            parentRes.addProperty(pItems, childRes);
            int currentTotal = 0;
            if (parentRes.hasProperty(pTotal)) {
                currentTotal = parentRes.getProperty(pTotal).getInt();
            }
            parentRes.removeAll(pTotal);
            parentRes.addProperty(pTotal, model.createTypedLiteral(currentTotal + 1, XSD.nonNegativeInteger.getURI()));
        }
    }

    private static void writeModelToGZ(Model model, Path outputPath) throws IOException {
        try (OutputStream fos = Files.newOutputStream(outputPath);
            GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
            model.write(gzos, "TURTLE");
        }
    }
}
