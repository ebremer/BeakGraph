package com.ebremer.beakgraph.lws;

import com.ebremer.ns.LWS;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.XSD;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

public class LWSMetadataGenerator {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LWSMetadataGenerator.class);

    /**
     * A tree walk that skips what it cannot read instead of aborting. The
     * inherited {@code visitFileFailed} rethrows, so one unreadable entry (a
     * permission-denied directory, a locked file) used to fail the whole
     * generation - and the endpoint then served an EMPTY model, 404ing every
     * path of a 5,000-file tree for one bad entry (BG-39).
     */
    static class LenientVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            logger.warn("Skipping unreadable entry {} while scanning LWS storage: {}", file, exc.toString());
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            if (exc != null) {
                logger.warn("Directory {} could not be fully read while scanning LWS storage: {}", dir, exc.toString());
            }
            return FileVisitResult.CONTINUE;
        }
    }

    private static final String AS_NS = "https://www.w3.org/ns/activitystreams#";
    private static final String SCHEMA_NS = "https://schema.org/";

    /**
     * Canonical base IRI for generated LWS resources. This is a stable namespace,
     * NOT a real host: {@code LWSStorageServlet} rewrites it to the live host/port
     * at serve time, so the generated metadata is portable across machines.
     */
    public static final String CANONICAL_BASE = "http://localhost:8888/HalcyonStorage";

    /** Default file name used to cache generated metadata inside a storage folder. */
    public static final String CACHE_FILE_NAME = "beakgraph.ttl.gz";

    /**
     * Top-level names the server's fixed routes own: an entry so named in the
     * storage root can never be reached (the route answers first), so it is
     * not indexed either - a listing must not advertise what a GET cannot
     * fetch (BG-413). {@code LWSStorageServlet} and {@code SPARQLEndPoint}
     * are the routes; the alias is the servlet's legacy path prefix.
     */
    public static final Set<String> RESERVED_ROOT_NAMES = Set.of(
            "description", "description.meta", "sparql", "rdf", "HalcyonStorage");

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: LWSMetadataGenerator <storageRootDir> [outputFile]");
            System.err.println("  Generates LWS metadata (" + CACHE_FILE_NAME + ") for the given storage folder.");
            System.exit(1);
        }
        Path rootPath = Paths.get(args[0]);
        Path outputPath = (args.length >= 2)
                ? Paths.get(args[1])
                : rootPath.resolve(CACHE_FILE_NAME);

        try {
            Model model = generateLWSModel(rootPath);
            writeModelToGZ(model, outputPath);
            System.out.println("Metadata successfully written to " + outputPath.toAbsolutePath());
        } catch (IOException e) {
            System.err.println("Failed to generate LWS metadata: " + e.getMessage());
            System.exit(1);
        }
    }

    public static Model generateLWSModel(Path rootPath) throws IOException {
        Model model = ModelFactory.createDefaultModel();
        model.setNsPrefix("lws", LWS.NS);
        model.setNsPrefix("as", AS_NS);
        model.setNsPrefix("sdo", SCHEMA_NS);
        model.setNsPrefix("xsd", XSD.NS);
        model.setNsPrefix("owl", OWL.NS);

        Property items = LWS.items;
        Property totalItems = model.createProperty(AS_NS, "totalItems");
        Property mediaType = model.createProperty(AS_NS, "mediaType");
        Property size = model.createProperty(SCHEMA_NS, "size");
        Property modified = model.createProperty(AS_NS, "updated");

        Resource containerType = LWS.Container;
        Resource dataType = LWS.DataResource;

        Resource rootResource = model.createResource(CANONICAL_BASE);
        rootResource.addProperty(RDF.type, containerType);

        Files.walkFileTree(rootPath, new LenientVisitor() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                // Never index the metadata cache itself (nor the temp file it is
                // written through): on regeneration the previous beakgraph.ttl.gz
                // would become a listed DataResource, making the raw model -
                // including the owl:sameAs file:/// server paths the servlet exists
                // to withhold - downloadable by any client.
                if (isExcluded(rootPath, file, false)) {
                    return FileVisitResult.CONTINUE;
                }
                String httpUri = toHttpUri(rootPath, file);
                processResource(model, httpUri, file, attrs, dataType, mediaType, size, modified);
                linkToParent(model, rootPath, file, items, totalItems);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (!dir.equals(rootPath)) {
                    if (isExcluded(rootPath, dir, true)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    String httpUri = toHttpUri(rootPath, dir);
                    processResource(model, httpUri, dir, attrs, containerType, null, null, modified);
                    linkToParent(model, rootPath, dir, items, totalItems);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        return model;
    }

    /** The cache file, and the temp file it is written through, are never content. */
    static boolean isCacheArtifact(Path file) {
        String name = file.getFileName().toString();
        return CACHE_FILE_NAME.equals(name) || (CACHE_FILE_NAME + ".tmp").equals(name);
    }

    /**
     * What the storage tree does NOT publish: the cache artifacts, hidden
     * entries (a dot-name, or the platform's hidden attribute - {@code .git},
     * {@code .DS_Store}, editor state; BG-221) at any depth, and top-level
     * entries named like a fixed route ({@link #RESERVED_ROOT_NAMES}). The
     * model walk and the {@link #treeSignature} walk apply the SAME rule, or
     * the refresher would see a permanent difference and regenerate forever.
     */
    static boolean isExcluded(Path rootPath, Path path, boolean directory) {
        if (!directory && isCacheArtifact(path)) {
            return true;
        }
        Path fileName = path.getFileName();
        String name = fileName == null ? "" : fileName.toString();
        if (name.startsWith(".")) {
            return true;
        }
        try {
            if (Files.isHidden(path)) {
                return true;
            }
        } catch (IOException ignore) {
            // unknowable: treat as visible
        }
        if (rootPath.relativize(path).getNameCount() == 1 && RESERVED_ROOT_NAMES.contains(name)) {
            logger.warn("{} is shadowed by a fixed route of the server and is not indexed (rename it to serve it)", path);
            return true;
        }
        return false;
    }

    /** The {@code as:updated} lexical form of a file or directory: its mtime as a UTC ISO instant. */
    public static String updatedLiteral(BasicFileAttributes attrs) {
        return attrs.lastModifiedTime().toInstant()
                .atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ISO_INSTANT);
    }

    private static String relative(Path rootPath, Path path) {
        return rootPath.relativize(path).toString().replace('\\', '/');
    }

    private static String toHttpUri(Path rootPath, Path path) {
        if (path.equals(rootPath)) return CANONICAL_BASE;
        return CANONICAL_BASE + "/" + relative(rootPath, path);
    }

    /**
     * Compact fingerprint of the metadata a storage tree would produce: one entry
     * per file ({@code relative/path|size|updated}) and per directory
     * ({@code relative/path|dir}), cache artifacts excluded. It is cheap - no
     * content probing - so a refresher can compare it against
     * {@link #modelSignature} on every poll and regenerate only when the tree
     * really changed. Directory timestamps are deliberately left out: writing
     * the cache file touches the root directory's mtime, which would otherwise
     * make every regeneration look like a change and trigger the next one.
     */
    public static Set<String> treeSignature(Path rootPath) throws IOException {
        Set<String> signature = new HashSet<>();
        Files.walkFileTree(rootPath, new LenientVisitor() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!isExcluded(rootPath, file, false)) {
                    signature.add(relative(rootPath, file) + "|" + attrs.size() + "|" + updatedLiteral(attrs));
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (!dir.equals(rootPath)) {
                    if (isExcluded(rootPath, dir, true)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    signature.add(relative(rootPath, dir) + "|dir");
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return signature;
    }

    /** The same fingerprint as {@link #treeSignature}, read back from a generated or cached model. */
    public static Set<String> modelSignature(Model model) {
        Set<String> signature = new HashSet<>();
        Property size = model.createProperty(SCHEMA_NS, "size");
        Property modified = model.createProperty(AS_NS, "updated");
        String prefix = CANONICAL_BASE + "/";
        model.listSubjectsWithProperty(RDF.type, LWS.DataResource).forEachRemaining(res -> {
            String uri = res.getURI();
            if (uri == null || !uri.startsWith(prefix)) return;
            Statement s = res.getProperty(size);
            Statement u = res.getProperty(modified);
            signature.add(uri.substring(prefix.length())
                    + "|" + (s == null ? "" : s.getLiteral().getLexicalForm())
                    + "|" + (u == null ? "" : u.getLiteral().getLexicalForm()));
        });
        model.listSubjectsWithProperty(RDF.type, LWS.Container).forEachRemaining(res -> {
            String uri = res.getURI();
            if (uri != null && uri.startsWith(prefix)) {
                signature.add(uri.substring(prefix.length()) + "|dir");
            }
        });
        return signature;
    }

    private static void processResource(Model model, String uri, Path realPath, BasicFileAttributes attrs,
                                        Resource type, Property pMediaType, Property pSize, Property pModified) {
        Resource res = model.createResource(uri);
        res.addProperty(RDF.type, type);

        // Link back to original local file URI
        String originalFileUri = "file:///" + realPath.toAbsolutePath().toString().replace("\\", "/");
        res.addProperty(OWL.sameAs, model.createResource(originalFileUri));

        res.addProperty(pModified, model.createTypedLiteral(updatedLiteral(attrs), XSD.dateTime.getURI()));

        if (pSize != null) {
            res.addProperty(pSize, model.createTypedLiteral(attrs.size(), XSD.integer.getURI()));
        }
        if (pMediaType != null) {
            String contentType = "application/octet-stream";
            try {
                String probed = Files.probeContentType(realPath);
                if (probed != null) contentType = probed;
            } catch (IOException ignored) {}
            res.addProperty(pMediaType, contentType);
        }
    }

    private static void linkToParent(Model model, Path rootPath, Path path, Property pItems, Property pTotal) {
        Path parentPath = path.getParent();
        if (parentPath != null && path.startsWith(rootPath)) {
            String parentHttp = toHttpUri(rootPath, parentPath);
            String childHttp = toHttpUri(rootPath, path);

            Resource parentRes = model.createResource(parentHttp);
            Resource childRes = model.createResource(childHttp);

            parentRes.addProperty(pItems, childRes);

            int currentTotal = parentRes.hasProperty(pTotal) ? parentRes.getProperty(pTotal).getInt() : 0;
            parentRes.removeAll(pTotal);
            parentRes.addProperty(pTotal, model.createTypedLiteral(currentTotal + 1, XSD.nonNegativeInteger.getURI()));
        }
    }

    /**
     * Writes the cache through a sibling temp file and moves it into place, so
     * a reader (or a crash mid-write) never sees a half-written cache. Both
     * names are excluded from indexing by {@link #isCacheArtifact}.
     */
    public static void writeModelToGZ(Model model, Path outputPath) throws IOException {
        Path tmp = outputPath.resolveSibling(outputPath.getFileName() + ".tmp");
        try (OutputStream fos = Files.newOutputStream(tmp);
             GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
            model.write(gzos, "TURTLE");
        }
        try {
            Files.move(tmp, outputPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, outputPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
