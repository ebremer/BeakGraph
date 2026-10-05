package com.ebremer.beakgraph.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
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
 * syntaxes, optionally compressed as {@code .gz} or {@code .zip}. A zip is the
 * zipped equivalent of ONE document: the entry read is the archive's single
 * RDF document, chosen by name - directories, {@code __MACOSX/} resource
 * forks and dot-files ({@code ._x.ttl}, {@code .DS_Store}) are skipped, an
 * entry with an accepted RDF extension wins, and otherwise the one remaining
 * file entry is taken (see {@link #open}). The CLI traversal filter and every
 * writer pipeline (RAM, parallel, huge) go through this class, so a new syntax
 * or compression is added exactly once.
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

    /** Read-ahead in front of the parser (and in front of the inflater for compressed sources). */
    private static final int READ_BUFFER = 1 << 20;
    /** GZIPInputStream's inflater buffer; its default is 512 bytes, one native inflate call each (BG-251). */
    private static final int GZIP_BUFFER = 1 << 16;

    /**
     * Opens a source file, transparently decompressing {@code .gz} and
     * {@code .zip}. Every stream is buffered ({@value #READ_BUFFER} bytes of
     * read-ahead; the gzip inflater additionally gets a {@value #GZIP_BUFFER}
     * byte buffer instead of its 512-byte default), and a decompressor that
     * rejects the file (a malformed gzip header, a damaged central directory)
     * releases the file handle before the exception leaves (BG-24).
     * <p>
     * A zip archive is a zipped single document. The entry to parse is chosen
     * by name over the whole archive, not taken first-come: directories,
     * {@code __MACOSX/} resource forks and dot-file entries ({@code ._x.ttl},
     * {@code .DS_Store}) are ignored; if exactly one remaining entry carries
     * an accepted RDF extension it is the document; otherwise, if exactly one
     * file entry remains at all, it is the document and its syntax comes from
     * the archive name ({@code data.nt.zip} -> N-Triples). Zero or several
     * candidates is an error naming the entries (BG-25). Reading the returned
     * stream ends at that entry's boundary; closing it closes the archive.
     */
    public static OpenedSource open(File src) throws IOException {
        String name = src.getName();
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".gz")) {
            FileInputStream fis = new FileInputStream(src);
            try {
                return new OpenedSource(new GZIPInputStream(new BufferedInputStream(fis, READ_BUFFER), GZIP_BUFFER),
                        detectLang(name.substring(0, name.length() - 3)));
            } catch (IOException | RuntimeException ex) {
                fis.close();
                throw ex;
            }
        }
        if (lower.endsWith(".zip")) {
            return openZip(src, name.substring(0, name.length() - 4));
        }
        return new OpenedSource(new BufferedInputStream(new FileInputStream(src), READ_BUFFER), detectLang(name));
    }

    private static OpenedSource openZip(File src, String archiveDocumentName) throws IOException {
        ZipFile zip = new ZipFile(src);
        try {
            List<ZipEntry> files = new ArrayList<>();
            List<ZipEntry> rdf = new ArrayList<>();
            List<String> all = new ArrayList<>();
            for (Enumeration<? extends ZipEntry> e = zip.entries(); e.hasMoreElements();) {
                ZipEntry entry = e.nextElement();
                all.add(entry.getName());
                if (entry.isDirectory() || isMetadataEntry(entry.getName())) {
                    continue;
                }
                files.add(entry);
                if (isSupported(baseName(entry.getName()))) {
                    rdf.add(entry);
                }
            }
            List<ZipEntry> candidates = rdf.size() == 1 ? rdf : files;
            if (candidates.size() != 1) {
                throw new IOException("Zip archive must contain exactly one RDF document, found "
                        + (candidates.isEmpty() ? "none" : candidates.size() + " candidates") + " in " + src
                        + "; entries: " + all);
            }
            ZipEntry chosen = candidates.get(0);
            // Prefer the entry's own extension; fall back to the archive name
            // minus ".zip" (e.g. data.nt.zip -> data.nt).
            Lang lang = RDFLanguages.filenameToLang(chosen.getName(), null);
            if (lang == null) {
                lang = detectLang(archiveDocumentName);
            }
            InputStream entryStream = new BufferedInputStream(zip.getInputStream(chosen), READ_BUFFER);
            ZipFile owned = zip;
            zip = null;   // the stream owns the archive from here
            return new OpenedSource(new FilterInputStream(entryStream) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        owned.close();
                    }
                }
            }, lang);
        } finally {
            if (zip != null) {
                zip.close();
            }
        }
    }

    /** {@code __MACOSX/} resource forks and dot-files anywhere in the archive are never the document. */
    private static boolean isMetadataEntry(String entryName) {
        return entryName.startsWith("__MACOSX/") || baseName(entryName).startsWith(".");
    }

    private static String baseName(String entryName) {
        int slash = entryName.lastIndexOf('/');
        return slash < 0 ? entryName : entryName.substring(slash + 1);
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
