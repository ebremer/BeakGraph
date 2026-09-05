package com.ebremer.beakgraph.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.riot.system.jsonld.TitaniumJsonLdOptions;
import org.slf4j.Logger;

/**
 * The single home for "what RDF source files does BeakGraph accept and how are
 * they opened". A source is a document in one of the {@link #BASE_EXTENSIONS}
 * syntaxes, optionally compressed as {@code .gz} or {@code .zip} (a zip is the
 * zipped equivalent of ONE document: its first file entry is read). The CLI
 * traversal filter and every writer pipeline (RAM, parallel, huge) go through
 * this class, so a new syntax or compression is added exactly once.
 */
public final class RdfSources {

    /** Accepted (lowercase) RDF syntax extensions, without compression suffixes. */
    public static final Set<String> BASE_EXTENSIONS = Set.of("ttl", "nt", "nq", "trig", "rdf", "jsonld");

    private RdfSources() {}

    /**
     * True when the file name is an accepted RDF source: {@code name.<ext>},
     * {@code name.<ext>.gz}, or {@code name.<ext>.zip} for any accepted
     * extension.
     */
    public static boolean isSupported(String fileName) {
        String n = fileName.toLowerCase(Locale.ROOT);
        if (n.endsWith(".gz")) {
            n = n.substring(0, n.length() - 3);
        } else if (n.endsWith(".zip")) {
            n = n.substring(0, n.length() - 4);
        }
        int dot = n.lastIndexOf('.');
        return dot >= 0 && BASE_EXTENSIONS.contains(n.substring(dot + 1));
    }

    /**
     * A source opened for parsing: the (decompressed) stream plus the syntax
     * detected from the file name - or, for zip archives, from the entry name
     * when it is more specific.
     */
    public record OpenedSource(InputStream stream, Lang lang) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    /** System property: elements per chunk handed from Jena's parser thread to the ingest (Jena's default when unset). */
    public static final String CHUNK_PROPERTY = "beakgraph.parser.chunk";
    /** System property: chunks the parser thread may run ahead of the ingest (Jena's default when unset). */
    public static final String QUEUE_PROPERTY = "beakgraph.parser.queue";

    /**
     * The one parser configuration every ingest pipeline uses (RAM, parallel,
     * ultra, huge, plaid): relative references resolve against {@code base},
     * blank-node labels are kept as written, so per-document scoping and
     * bnode alignment see the source labels, and a JSON-LD document's
     * {@code @context} references load through {@link JsonLdContexts} (from
     * the source tree; remote ones only when enabled). A JSON-LD or RDF/XML
     * option added here reaches all six engines at once (BG-429).
     * <p>
     * The parser runs on its own thread and hands chunks over a bounded
     * queue; {@value #CHUNK_PROPERTY} / {@value #QUEUE_PROPERTY} size them.
     * The caller MUST close the quad stream it obtains (try-with-resources):
     * closing is what aborts and joins that thread when the ingest fails
     * part-way, otherwise it stays parked on the full queue with up to a
     * queue's worth of parsed quads for the life of the process (BG-100).
     *
     * @param input the source file, for the JSON-LD context loader's source tree
     */
    public static AsyncParserBuilder parser(OpenedSource opened, String base, File input) {
        AsyncParserBuilder builder = AsyncParser.of(opened.stream(), opened.lang(), base);
        Integer chunk = Integer.getInteger(CHUNK_PROPERTY);
        if (chunk != null && chunk > 0) {
            builder.setChunkSize(chunk);
        }
        Integer queue = Integer.getInteger(QUEUE_PROPERTY);
        if (queue != null && queue > 0) {
            builder.setQueueSize(queue);
        }
        boolean jsonLd = !isStreaming(opened.lang());
        builder.mutateSources(rdfBuilder -> {
            rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven());
            if (jsonLd) {
                rdfBuilder.set(TitaniumJsonLdOptions.JSONLD_OPTIONS, JsonLdContexts.options(base, input));
            }
        });
        return builder;
    }

    /**
     * False for a syntax Jena cannot parse as a stream. JSON-LD is the one
     * accepted syntax without a streaming parser: Jena hands the WHOLE
     * document to Titanium, which builds the JSON tree and expands it in
     * memory before the first quad is emitted, so heap is proportional to the
     * (decompressed) document, not to any spill batch. The disk-based engines'
     * bounded-RAM promise therefore holds per JSON-LD document, not per quad
     * count (BG-425).
     */
    public static boolean isStreaming(Lang lang) {
        return lang == null || !lang.getName().toUpperCase(Locale.ROOT).contains("JSON-LD");
    }

    /**
     * The syntax {@link #open} will assign from the file name alone
     * ({@code .gz} / {@code .zip} stripped; for a zip the entry name may still
     * refine it once opened).
     */
    public static Lang langOf(File src) {
        String name = src.getName();
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".gz")) {
            return detectLang(name.substring(0, name.length() - 3));
        }
        if (lower.endsWith(".zip")) {
            return detectLang(name.substring(0, name.length() - 4));
        }
        return detectLang(name);
    }

    /**
     * For the disk-based engines: warns that {@code src} is about to be parsed
     * fully in memory when its syntax has no streaming parser
     * ({@link #isStreaming}), naming the file, its size and the way out.
     */
    public static void warnIfMaterialized(File src, Lang lang, Logger log) {
        if (isStreaming(lang)) {
            return;
        }
        String lower = src.getName().toLowerCase(Locale.ROOT);
        boolean compressed = lower.endsWith(".gz") || lower.endsWith(".zip");
        log.warn("{}: {} has no streaming parser - the whole document ({} MB{}) is loaded and expanded in "
                + "memory before its first quad reaches the sorters, so RAM is NOT bounded by the spill "
                + "batches for this file; convert bulk JSON-LD to N-Quads first (riot --output=nq)",
                src, lang.getName(), Math.max(1, src.length() >> 20), compressed ? " compressed" : "");
    }

    /**
     * Opens a source file, transparently decompressing {@code .gz} and
     * {@code .zip}. For zip archives the stream is positioned at the first
     * file entry (the archive is expected to be a zipped single document);
     * reading it ends at that entry's boundary.
     */
    public static OpenedSource open(File src) throws IOException {
        String name = src.getName();
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".gz")) {
            return new OpenedSource(new GZIPInputStream(new FileInputStream(src)),
                    detectLang(name.substring(0, name.length() - 3)));
        }
        if (lower.endsWith(".zip")) {
            ZipInputStream zis = new ZipInputStream(new FileInputStream(src));
            try {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (!entry.isDirectory()) {
                        // Prefer the entry's own extension; fall back to the
                        // archive name minus ".zip" (e.g. data.nt.zip -> data.nt).
                        Lang lang = RDFLanguages.filenameToLang(entry.getName(), null);
                        if (lang == null) {
                            lang = detectLang(name.substring(0, name.length() - 4));
                        }
                        return new OpenedSource(zis, lang);
                    }
                }
            } catch (IOException | RuntimeException ex) {
                zis.close();
                throw ex;
            }
            zis.close();
            throw new IOException("Zip archive contains no file entry: " + src);
        }
        return new OpenedSource(new FileInputStream(src), detectLang(name));
    }

    /**
     * Syntax from a (decompressed) file name; Turtle when unrecognized - this
     * is a quad store, and named graphs can only arrive through a quad-capable
     * syntax the name identifies (TriG, N-Quads).
     */
    private static Lang detectLang(String effectiveName) {
        return RDFLanguages.filenameToLang(effectiveName, Lang.TURTLE);
    }
}
