package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.SPARQLEndPoint;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sys.JenaSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line entry point for BeakGraph ("beakgraph" command): converts RDF source trees to
 * HDF5-backed graphs, exports them back to RDF, verifies file integrity (-verify), or starts
 * the SPARQL/LWS endpoint.
 *
 * @author Erich Bremer
 */
public class BeakGraphCLI {
    private static final Logger logger = LoggerFactory.getLogger(BeakGraphCLI.class);
    private static ProgressBar progressBar = null;
    private final FileCounter fc;
    private Parameters params;

    /** Conversion counters for this run (failed conversions drive the exit code). */
    public FileCounter getFileCounter() {
        return fc;
    }

    public BeakGraphCLI(Parameters params) {
        JenaSystem.init();
        if (params.voidExact && params.voidSketch) {
            throw new IllegalArgumentException(
                    "-void and -voidsketch are mutually exclusive: pick exact in-memory statistics "
                    + "(-void) or bounded-memory HyperLogLog statistics (-voidsketch)");
        }
        this.params = params;
        this.fc = new FileCounter();
        ProgressBarStyle style = styleFor(System.getProperty("os.name"));
        // -merge is one big conversion (and -export its own flow), not a stream
        // of per-file tasks; the per-file progress bar would only render empty.
        if (params.status && !params.merge && params.export == null) {
            progressBar = new ProgressBarBuilder()
                .setTaskName("Processing RDF Source Files...")
                .setInitialMax(0)
                .setStyle(style)
                .build();
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    /** ASCII progress bars on Windows consoles; the fold must not depend on the default locale (BG-361). */
    static ProgressBarStyle styleFor(String osName) {
        return osName != null && osName.toLowerCase(java.util.Locale.ROOT).contains("win")
                ? ProgressBarStyle.ASCII : ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
    }

    /**
     * The command line without the process exit: returns the exit code
     * (0 success, 1 bad arguments / missing paths / nothing to do, 2 at least
     * one conversion or verification failed). A bare invocation, and a
     * parseable but mode-less one such as {@code -threads 4} alone, used to
     * exit 0 having printed nothing (BG-278, BG-155).
     */
    static int run(String[] args) throws Exception {
        logger.info(String.format("%s %s", "beakgraph ", Arrays.toString(args)));
        Parameters params = new Parameters();
        JCommander jc = JCommander.newBuilder().addObject(params).build();
        jc.setProgramName("beakgraph");
        if (args.length == 0) {
            System.err.println("Error: no arguments given. Use -src with -dest (convert), -src with -export, "
                    + "-verify, -endpoint, -help or -version.");
            jc.usage();
            return 1;
        }
        try {
            jc.parse(args);
        } catch (ParameterException ex) {
            if (params.version) {
                System.out.println("beakgraph - Version : " + Params.VERSION);
                return 0;
            }
            // Bad arguments are an error: say so on stderr and exit non-zero
            // (scripts used to see a successful exit 0 for a failed run).
            System.err.println(ex.getMessage());
            jc.usage();
            return 1;
        }
        if (params.voidExact && params.voidSketch) {
            System.err.println("Error: -void and -voidsketch are mutually exclusive. "
                    + "Use -void for exact in-memory statistics or -voidsketch for the "
                    + "bounded-memory HyperLogLog version.");
            return 1;
        }
        if (params.version) {
            // Must be handled on the SUCCESS path: version was previously
            // printed only inside the ParameterException catch, so a plain
            // "-v" parsed fine, matched no branch, and printed nothing.
            System.out.println("beakgraph - Version : " + Params.VERSION);
            return 0;
        }
        if (params.help) {
            jc.usage();
            return 0;
        }
        if (params.jsonLdRemote) {
            // Read per parse by RdfSources through JsonLdContexts.
            System.setProperty(com.ebremer.beakgraph.utils.JsonLdContexts.REMOTE_PROPERTY, "true");
        }
        if (params.sparqlendpoint != null) {
            if (!params.sparqlendpoint.exists()) {
                System.err.println("Error: -endpoint does not exist: " + params.sparqlendpoint);
                return 1;
            }
            // BGSparqlService reads the limit per query from this property.
            System.setProperty("beakgraph.query.timeout.seconds", Long.toString(params.timeout));
            SPARQLEndPoint endpoint = SPARQLEndPoint.getSPARQLEndPoint(params);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> endpoint.shutdown()));
            System.out.println("Press Ctrl+C to stop the server...");
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return 0;
        }
        if (params.verify != null) {
            if (!params.verify.exists()) {
                System.err.println("Error: -verify path does not exist: " + params.verify);
                return 1;
            }
            return new VerifyCommand(params.verify, params.deep).run();
        }
        if (params.src != null && params.src.exists() && params.export != null) {
            // Export mode: dump BeakGraph(s) back to RDF; no -dest
            // involved (output lands beside each source .h5).
            BeakGraphCLI bg = new BeakGraphCLI(params);
            bg.export();
            return (bg.fc.getFailedConversionFileCount() > 0) ? 2 : 0;
        }
        if (params.src != null && params.src.exists()) {
            if (params.dest == null) {
                // Without this guard every FileProcessor NPEs inside a
                // discarded Future: nothing converts, nothing is logged,
                // and the run exits 0 reporting success.
                System.err.println("Error: -dest is required with -src");
                jc.usage();
                return 1;
            }
            JenaSystem.init();
            BeakGraphCLI bg = new BeakGraphCLI(params);
            if (params.merge) {
                bg.merge();
            } else {
                bg.traverse();
            }
            return (bg.fc.getFailedConversionFileCount() > 0) ? 2 : 0;
        }
        if (params.src != null) {
            System.err.println("Error: -src does not exist: " + params.src);
            return 1;
        }
        if (params.export != null || params.dest != null || params.merge) {
            System.err.println("Error: -src is required with -dest, -merge and -export");
            jc.usage();
            return 1;
        }
        System.err.println("Error: no operation given. Use -src with -dest (convert), -src with -export, "
                + "-verify, -endpoint, -help or -version.");
        jc.usage();
        return 1;
    }

    /**
     * Walks the {@code -src} tree and returns the supported, non-empty RDF
     * sources in path order, counting directories, zero-length and other
     * files on {@code fc}. A subtree that cannot be listed (an ACL-denied
     * folder, a Windows profile's "Application Data" junction, $RECYCLE.BIN,
     * another user's 0700 directory) or a file whose attributes cannot be
     * read is logged, counted as unreadable and SKIPPED: {@code Files.walk}
     * surfaced such errors as an UncheckedIOException from the middle of the
     * stream, which the callers' catch (IOException) missed, so the run died
     * with the rest of the tree unconverted, no summary and exit code 1
     * (BG-418). A directory whose real path is one of its own ancestors (a
     * junction or symlink cycle) is skipped too instead of recursing until
     * the path length fails.
     */
    static List<Path> scanSources(Path root, FileCounter fc) {
        final List<Path> accepted = new ArrayList<>();
        final Deque<Path> realAncestors = new ArrayDeque<>();
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    Path real;
                    try {
                        real = dir.toRealPath();
                    } catch (IOException e) {
                        real = dir.toAbsolutePath().normalize();
                    }
                    if (realAncestors.contains(real)) {
                        logger.warn("Skipping {}: it links back to its ancestor {} (junction/symlink cycle)", dir, real);
                        fc.incrementUnreadableCount();
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    realAncestors.push(real);
                    fc.incrementDirectoryCount();
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (attrs.size() == 0) {
                        fc.incrementZeroLengthFileCount();
                    } else if (RdfSources.isSupported(file.getFileName().toString())) {
                        fc.incrementRDFFileCount();
                        accepted.add(file);
                    } else {
                        fc.incrementOtherFileCount();
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    logger.warn("Skipping unreadable {}: {}", file, exc.toString());
                    fc.incrementUnreadableCount();
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    realAncestors.pop();
                    if (exc != null) {
                        logger.warn("Directory {} could not be fully read, its remaining entries are skipped: {}", dir, exc.toString());
                        fc.incrementUnreadableCount();
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ex) {
            // walkFileTree reports every I/O failure to the visitor; only a
            // visitor throwing lands here, and this one never does.
            logger.error("Failed to scan source tree {}", root, ex);
        }
        Collections.sort(accepted);
        return accepted;
    }

    public void traverse() {
        final List<Path> accepted = scanSources(params.src.toPath(), fc);
        try (ThreadPoolExecutor engine = new ThreadPoolExecutor(params.threads, params.threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>())) {
            engine.prestartAllCoreThreads();
            if (params.dest == null) {
                // No -dest: every accepted file is a loud failure (it used to be
                // an NPE swallowed inside the never-inspected Future).
                for (Path p : accepted) {
                    fc.incrementFailedConversionFileCount();
                    logger.error("Failed to convert {}: no -dest given", p);
                }
                accepted.clear();
            }
            for (var entry : planDestinations(accepted, params.src, params.dest).entrySet()) {
                java.util.List<Path> sources = entry.getValue();
                if (sources.size() > 1) {
                    logger.error("{} sources map to the same destination {}: converting {} only, the others fail: {}",
                            sources.size(), entry.getKey(), sources.get(0), sources.subList(1, sources.size()));
                    for (int i = 1; i < sources.size(); i++) {
                        fc.incrementFailedConversionFileCount();
                    }
                }
                if (params.status) {
                    progressBar.maxHint(fc.getRDFFileCount());
                    progressBar.stepTo(engine.getCompletedTaskCount());
                }
                engine.submit(new FileProcessor(sources.get(0), entry.getKey(), fc));
            }
            engine.shutdown();
            while (!engine.isTerminated()) {
                if (params.status) {
                    progressBar.stepTo(engine.getCompletedTaskCount());
                    progressBar.maxHint(fc.getRDFFileCount());
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    // Restore the flag and stop polling; the executor keeps draining.
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (params.status) {
            System.out.println(fc);
        }
    }

    /**
     * -merge: parse every supported RDF source under -src into ONE BeakGraph
     * HDF5 file at -dest (or {@code <dest>/merged.h5} when -dest is an existing
     * directory). Sources are taken in sorted path order so repeated merges of
     * the same tree are deterministic; blank nodes stay distinct per source
     * document. The destination is rebuilt - the write is atomic, so a failed
     * rebuild never destroys a previous good artifact.
     */
    public void merge() {
        final List<File> inputs = new ArrayList<>();
        for (Path p : scanSources(params.src.toPath(), fc)) {
            inputs.add(p.toFile());
        }
        if (inputs.isEmpty()) {
            System.err.println("No supported RDF sources found under " + params.src);
            return;
        }
        File dest = params.dest;
        // A directory by INTENT, existing or not: an existing directory, a
        // trailing separator, or a name without a store suffix. java.io.File
        // strips the separator, so "-dest D:\\out\\" for a directory that did
        // not exist yet used to write a store named "out" with no .h5, which
        // -verify, -export and the LWS servlet then all ignored (BG-422).
        boolean trailingSeparator = dest instanceof Parameters.DestinationFile d && d.trailingSeparator();
        if (dest.isDirectory() || trailingSeparator
                || !com.ebremer.beakgraph.core.BeakGraphFiles.isBeakGraphFileName(dest.getName())) {
            if (!dest.isDirectory()) {
                logger.info("-dest {} names a directory (no .h5/.hdf5 suffix): the merged store is {}/merged.h5", dest, dest);
            }
            dest.mkdirs();
            dest = new File(dest, "merged.h5");
        }
        if (dest.getParentFile() != null) {
            dest.getParentFile().mkdirs();
        }
        logger.info("Merging {} RDF sources into {} (method {})", inputs.size(), dest, effectiveMethod());
        try {
            // All sources feed the ONE store being written; blank nodes stay
            // distinct per document in every engine.
            newWriter(null, inputs, dest).write();
        } catch (Throwable ex) {
            // Throwable: an OutOfMemoryError from an in-RAM engine used to escape
            // main() with a stack trace and no summary (BG-226).
            fc.incrementFailedConversionFileCount();
            logger.error("Failed to merge {} sources into {}", inputs.size(), dest, ex);
            hintOnHeapExhaustion(ex, dest.toString());
        }
        if (params.status) {
            System.out.println(fc);
        }
    }

    /**
     * -export: dump the BeakGraph(s) at -src back to RDF. -src may be one .h5
     * file or a directory tree of them; each store exports to a sibling file
     * with the same name and the format's extension (plus .gz with -compress).
     * TTL/NT are upgraded to TRIG/NQ when a store holds named graphs beyond
     * the default graph. BeakGraph-internal metadata graphs (the VoID
     * statistics and spatial index, urn:x-beakgraph:*) are derived build
     * artifacts and are excluded - both from the dump and from the "has named
     * graphs" decision, so a plain-triples store round-trips to plain triples.
     */
    public void export() {
        final List<File> inputs = new ArrayList<>();
        if (params.src.isDirectory()) {
            try (var walk = Files.walk(params.src.toPath())) {
                walk.filter(p -> p.toFile().isFile()
                                && com.ebremer.beakgraph.core.BeakGraphFiles.isBeakGraphFileName(p.getFileName().toString())
                                && p.toFile().length() > 0)
                    .sorted()
                    .forEach(p -> inputs.add(p.toFile()));
            } catch (IOException ex) {
                logger.error("Failed to scan source tree {}", params.src, ex);
            }
        } else {
            inputs.add(params.src);
        }
        if (inputs.isEmpty()) {
            System.err.println("No BeakGraph (.h5/.hdf5) files found under " + params.src);
            return;
        }
        for (File h5 : inputs) {
            fc.incrementRDFFileCount();
            try {
                exportOne(h5);
            } catch (Exception ex) {
                fc.incrementFailedConversionFileCount();
                logger.error("Failed to export {}", h5, ex);
            }
        }
        if (params.status) {
            System.out.println(fc);
        }
    }

    private void exportOne(File h5) throws Exception {
        String fmt = ExportFormatValidator.normalize(params.export);
        try (com.ebremer.beakgraph.core.BeakGraph bg = new com.ebremer.beakgraph.core.BeakGraph(
                new com.ebremer.beakgraph.hdf5.readers.HDF5Reader(h5))) {
            org.apache.jena.sparql.core.DatasetGraph dsg = bg.getDataset().asDatasetGraph();
            boolean named = hasUserNamedGraphs(dsg);
            if (named && "NT".equals(fmt)) {
                logger.info("{} holds named graphs: exporting NQ instead of NT", h5.getName());
                fmt = "NQ";
            } else if (named && "TTL".equals(fmt)) {
                logger.info("{} holds named graphs: exporting TRIG instead of TTL", h5.getName());
                fmt = "TRIG";
            }
            String ext = switch (fmt) {
                case "NT" -> "nt";
                case "NQ" -> "nq";
                case "TTL" -> "ttl";
                case "TRIG" -> "trig";
                default -> "jsonld";
            };
            String base = com.ebremer.beakgraph.core.BeakGraphFiles.stripExtension(h5.getName());
            Path out = h5.toPath().resolveSibling(base + "." + ext + (params.compress ? ".gz" : ""));
            // A unique temp name: two exports of one store at once must not
            // truncate each other's work (BG-101).
            Path tmp = com.ebremer.beakgraph.core.AtomicPublish.tempFor(out);
            logger.info("Exporting {} -> {} ({})", h5.getName(), out.getFileName(), fmt);
            try (OutputStream os = openExportStream(tmp)) {
                writeExport(os, dsg, fmt, h5);
            } catch (Exception ex) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {}
                throw ex;
            }
            com.ebremer.beakgraph.core.AtomicPublish.publish(tmp, out);
            logger.info("Export complete: {}", out);
        }
    }

    private OutputStream openExportStream(Path tmp) throws IOException {
        OutputStream os = new java.io.BufferedOutputStream(Files.newOutputStream(tmp), 1 << 17);
        return params.compress ? new java.util.zip.GZIPOutputStream(os, 1 << 16) : os;
    }

    /** A graph the USER put in the store (not BeakGraph's own metadata graphs: VoID, Spatial, grid tiles). */
    private static boolean isUserGraph(org.apache.jena.graph.Node g) {
        return !Params.isInternalGraph(g);
    }

    private static boolean hasUserNamedGraphs(org.apache.jena.sparql.core.DatasetGraph dsg) {
        var it = dsg.listGraphNodes();
        while (it.hasNext()) {
            if (isUserGraph(it.next())) {
                return true;
            }
        }
        return false;
    }

    /**
     * NT/NQ line formats stream straight off the GSPO index with per-id text
     * memoization (see IndexExport) when the store supports it; false means
     * nothing was written and the generic writer below runs instead.
     */
    private static boolean tryIndexExport(OutputStream os, org.apache.jena.sparql.core.DatasetGraph dsg,
            boolean quads, java.util.function.UnaryOperator<org.apache.jena.graph.Node> termMap) throws IOException {
        if (dsg instanceof com.ebremer.beakgraph.core.BGDatasetGraph bgd
                && bgd.getBeakGraph().getReader() instanceof com.ebremer.beakgraph.hdf5.readers.HDF5Reader reader) {
            return com.ebremer.beakgraph.hdf5.jena.IndexExport.tryWrite(reader, os, quads, termMap);
        }
        return false;
    }

    /**
     * How exported terms are mapped. Stores keep document-relative IRIs
     * ({@code <>}, {@code <sib.png>}, {@code <../x>}) in relative form; they
     * only have an absolute identity relative to a base. With {@code -base}
     * every such term is resolved against it (all formats). Without one,
     * N-Triples / N-Quads cannot carry them at all (IRIs there MUST be
     * absolute), so the export fails on the first relative IRI with a message
     * naming the option; Turtle / TriG / JSON-LD accept relative references
     * syntactically and are written as stored, with one warning that they
     * will re-resolve against wherever the file ends up.
     */
    private java.util.function.UnaryOperator<org.apache.jena.graph.Node> exportTermMap(String fmt, File h5) {
        if (params.base != null) {
            com.ebremer.beakgraph.core.RelativeIRIResolver resolver =
                    new com.ebremer.beakgraph.core.RelativeIRIResolver(params.base);
            if (!resolver.isActive()) {
                throw new IllegalArgumentException("-base is not a usable IRI: " + params.base);
            }
            org.apache.jena.sparql.graph.NodeTransform t = resolver.storageToAbsolute();
            return t::apply;
        }
        boolean lineFormat = "NT".equals(fmt) || "NQ".equals(fmt);
        java.util.concurrent.atomic.AtomicBoolean warned = new java.util.concurrent.atomic.AtomicBoolean();
        return n -> {
            String rel = relativeReference(n);
            if (rel != null) {
                if (lineFormat) {
                    throw new IllegalStateException("Store " + h5.getName() + " holds document-relative IRIs (e.g. <"
                            + rel + ">), which " + fmt + " cannot carry: pass -base <the URL the store is served from>"
                            + " so they are resolved, or export as TTL/TRIG/JSON-LD");
                }
                if (warned.compareAndSet(false, true)) {
                    logger.warn("{} holds document-relative IRIs (e.g. <{}>); without -base they are written as stored and "
                            + "will resolve against the exported file's own location", h5.getName(), rel);
                }
            }
            return n;
        };
    }

    /** The document-relative IRI a node carries - as a term or as a literal's datatype - or null. */
    private static String relativeReference(org.apache.jena.graph.Node n) {
        if (n == null) {
            return null;
        }
        if (n.isURI() && com.ebremer.beakgraph.utils.UTIL.isRelativeIRI(n.getURI())) {
            return n.getURI();
        }
        if (n.isLiteral()) {
            String dt = n.getLiteralDatatypeURI();
            if (dt != null && com.ebremer.beakgraph.utils.UTIL.isRelativeIRI(dt)) {
                return dt;
            }
        }
        return null;
    }

    private void writeExport(OutputStream os, org.apache.jena.sparql.core.DatasetGraph dsg, String fmt, File h5)
            throws IOException {
        java.util.function.UnaryOperator<org.apache.jena.graph.Node> termMap = exportTermMap(fmt, h5);
        org.apache.jena.sparql.graph.NodeTransform transform = termMap::apply;
        switch (fmt) {
            case "NT", "TTL" -> {
                // Triple export: by this point the store has no user named
                // graphs, so the default graph IS the data.
                if ("NT".equals(fmt) && tryIndexExport(os, dsg, false, termMap)) {
                    return;
                }
                org.apache.jena.riot.system.StreamRDF stream = org.apache.jena.riot.system.StreamRDFWriter
                        .getWriterStream(os, "NT".equals(fmt)
                                ? org.apache.jena.riot.RDFFormat.NTRIPLES
                                : org.apache.jena.riot.RDFFormat.TURTLE_BLOCKS);
                stream.start();
                var it = dsg.getDefaultGraph().find();
                while (it.hasNext()) {
                    stream.triple(org.apache.jena.sparql.graph.NodeTransformLib.transform(transform, it.next()));
                }
                stream.finish();
            }
            case "NQ", "TRIG" -> {
                if ("NQ".equals(fmt) && tryIndexExport(os, dsg, true, termMap)) {
                    return;
                }
                org.apache.jena.riot.system.StreamRDF stream = org.apache.jena.riot.system.StreamRDFWriter
                        .getWriterStream(os, "NQ".equals(fmt)
                                ? org.apache.jena.riot.RDFFormat.NQUADS
                                : org.apache.jena.riot.RDFFormat.TRIG_BLOCKS);
                stream.start();
                var it = dsg.find();
                while (it.hasNext()) {
                    var q = it.next();
                    if (isUserGraph(q.getGraph())) {
                        stream.quad(org.apache.jena.sparql.graph.NodeTransformLib.transform(transform, q));
                    }
                }
                stream.finish();
            }
            default -> {
                // JSON-LD has no streaming writer: materialize the filtered
                // dataset. Fine for JSON-LD-sized data; use NQ/TRIG for bulk.
                org.apache.jena.sparql.core.DatasetGraph copy =
                        org.apache.jena.sparql.core.DatasetGraphFactory.create();
                var it = dsg.find();
                while (it.hasNext()) {
                    var q = it.next();
                    if (!isUserGraph(q.getGraph())) {
                        continue;
                    }
                    if (q.getObject().isTripleTerm()) {
                        // Same stance as the SPARQL endpoint: JSON-LD has no RDF 1.2
                        // triple-term syntax, so refuse plainly instead of emitting a
                        // corrupt document or an opaque serializer failure (BG-268).
                        throw new IllegalArgumentException("JSON-LD cannot represent RDF 1.2 triple terms ("
                                + h5.getName() + " holds " + q.getObject() + "); export as NT, NQ, TTL or TRIG instead");
                    }
                    copy.add(org.apache.jena.sparql.graph.NodeTransformLib.transform(transform, q));
                }
                org.apache.jena.riot.RDFDataMgr.write(os, copy, org.apache.jena.riot.Lang.JSONLD);
            }
        }
    }

    /** The VoID statistics mode for this run: NONE unless -void or -voidsketch was given. */
    private com.ebremer.beakgraph.core.VoidMode voidMode() {
        if (params.voidExact) {
            return com.ebremer.beakgraph.core.VoidMode.EXACT;
        }
        if (params.voidSketch) {
            return com.ebremer.beakgraph.core.VoidMode.SKETCH;
        }
        return com.ebremer.beakgraph.core.VoidMode.NONE;
    }

    /** {@code -voidbase}, or the default sd:Dataset IRI (BG-109). */
    private String voidDatasetIri() {
        return (params.voidBase == null || params.voidBase.isBlank()) ? Params.VOID_DATASET_IRI : params.voidBase;
    }

    /**
     * The conversion engine for this run: {@code -method} (0 = in-memory,
     * 1 = disk, 2 = parallel, 3 = ultra, 4 = hugeUltra, 5 = plaid), with the
     * legacy {@code -huge} flag acting as "-method 1" when no explicit
     * -method was given.
     */
    private int effectiveMethod() {
        return (params.method == 0 && params.huge) ? 1 : params.method;
    }

    /**
     * An OutOfMemoryError from an in-RAM engine is almost always "the input
     * does not fit": say so and name the way out, instead of leaving the
     * operator with a bare heap dump - a decompressed .gz/.zip source can be
     * many times its size (BG-226).
     */
    private void hintOnHeapExhaustion(Throwable ex, String what) {
        int method = effectiveMethod();
        if (ex instanceof OutOfMemoryError && (method == 0 || method == 2 || method == 3)) {
            logger.error("Out of memory converting {} with the in-memory -method {}: the (decompressed) input is "
                    + "too large for the heap; use a disk-based engine (-method 1, 4 or 5) or a larger -Xmx", what, method);
        }
    }

    /** True inside a GraalVM native image (the runtime sets this property; tests may set it too). */
    static boolean inNativeImage() {
        return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    }

    /**
     * Builds the writer selected by {@link #effectiveMethod()} for one
     * source-or-sources -> destination conversion. Shared by the per-file
     * processors and -merge so the two can never route differently.
     */
    /** Package-private so tests can substitute a writer (e.g. one that fails with an Error). */
    BeakGraphWriter newWriter(File source, List<File> sources, File dest) throws IOException {
        int method = effectiveMethod();
        if ((method == 1 || method == 4 || method == 5) && inNativeImage()) {
            // The GraalVM binary carries no JNI metadata for hdf.hdf5lib and no
            // natives: the disk engines would fail only after the parse and
            // sort stages. Say so before any work is done (BG-442).
            throw new IOException("-method " + method + " (disk-based, native HDF5) is not available in the "
                    + "native binary; use -method 0/2/3 here, or run the jar with java for the disk engines");
        }
        switch (method) {
            case 1 -> {
                // Disk-based build: same output format, but sorting/indexing
                // spill to a workspace instead of the heap. Needs the native
                // HDF5 backend on the classpath (the hdf5-backend-* profiles);
                // with -threads N, N builds run concurrently, each with its
                // own workspace.
                HugeHDF5Writer.Builder builder = HugeHDF5Writer.Builder()
                        .setDestination(dest)
                        .setVoidMode(voidMode())
                        .setVoidDatasetIri(voidDatasetIri())
                        .setSpatial(params.spatial)
                        .setFeatures(params.features);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources).setSourceRoot(params.src);
                if (params.workdir != null) {
                    params.workdir.mkdirs();
                    builder.setWorkDirectory(params.workdir.toPath());
                }
                // Spill sizing (BG-125): the builders' knobs, reachable from the command line.
                if (params.termSpillBatch != null) builder.setTermSpillBatch(params.termSpillBatch);
                if (params.idSpillBatch != null) builder.setIdSpillBatch(params.idSpillBatch);
                if (params.mergeFanIn != null) builder.setMergeFanIn(params.mergeFanIn);
                if (params.spillMB != null) builder.setTermSpillBytes(params.spillMB * (1L << 20));
                return builder.build();
            }
            case 2 -> {
                // Multi-threaded in-memory build on a pool of -cores threads.
                ParallelHDF5Writer.Builder builder = ParallelHDF5Writer.Builder()
                        .setDestination(dest)
                        .setVoidMode(voidMode())
                        .setVoidDatasetIri(voidDatasetIri())
                        .setSpatial(params.spatial)
                        .setFeatures(params.features)
                        .setCores(params.cores);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources).setSourceRoot(params.src);
                return builder.build();
            }
            case 3 -> {
                // Ultra in-memory build: parallel parse, packed-key radix-sorted
                // indexes, parallel emission - also capped at -cores threads.
                UltraHDF5Writer.Builder builder = UltraHDF5Writer.Builder()
                        .setDestination(dest)
                        .setVoidMode(voidMode())
                        .setVoidDatasetIri(voidDatasetIri())
                        .setSpatial(params.spatial)
                        .setFeatures(params.features)
                        .setCores(params.cores);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources).setSourceRoot(params.src);
                return builder.build();
            }
            case 4 -> {
                // hugeUltra: the disk-based pipeline (bounded RAM, any quad
                // count) on parallel primitive sorting machinery - the engine
                // for multi-billion-quad builds. Needs the native HDF5 backend.
                HugeUltraHDF5Writer.Builder builder = HugeUltraHDF5Writer.Builder()
                        .setDestination(dest)
                        .setVoidMode(voidMode())
                        .setVoidDatasetIri(voidDatasetIri())
                        .setSpatial(params.spatial)
                        .setFeatures(params.features)
                        .setCores(params.cores);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources).setSourceRoot(params.src);
                if (params.workdir != null) {
                    params.workdir.mkdirs();
                    builder.setWorkDirectory(params.workdir.toPath());
                }
                // Spill sizing (BG-125): the builders' knobs, reachable from the command line.
                if (params.termSpillBatch != null) builder.setTermSpillBatch(params.termSpillBatch);
                if (params.idSpillBatch != null) builder.setIdSpillBatch(params.idSpillBatch);
                if (params.mergeFanIn != null) builder.setMergeFanIn(params.mergeFanIn);
                if (params.spillMB != null) builder.setTermSpillBytes(params.spillMB * (1L << 20));
                return builder.build();
            }
            case 5 -> {
                // plaid: hugeUltra plus parallel multi-file ingest - up to
                // -cores documents parse concurrently. The engine of choice
                // for -merge over many files. Needs the native HDF5 backend.
                PlaidHDF5Writer.Builder builder = PlaidHDF5Writer.Builder()
                        .setDestination(dest)
                        .setVoidMode(voidMode())
                        .setVoidDatasetIri(voidDatasetIri())
                        .setSpatial(params.spatial)
                        .setFeatures(params.features)
                        .setCores(params.cores);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources).setSourceRoot(params.src);
                if (params.workdir != null) {
                    params.workdir.mkdirs();
                    builder.setWorkDirectory(params.workdir.toPath());
                }
                // Spill sizing (BG-125): the builders' knobs, reachable from the command line.
                if (params.termSpillBatch != null) builder.setTermSpillBatch(params.termSpillBatch);
                if (params.idSpillBatch != null) builder.setIdSpillBatch(params.idSpillBatch);
                if (params.mergeFanIn != null) builder.setMergeFanIn(params.mergeFanIn);
                if (params.spillMB != null) builder.setTermSpillBytes(params.spillMB * (1L << 20));
                return builder.build();
            }
            default -> {
                HDF5Writer.Builder builder = HDF5Writer.Builder()
                        .setDestination(dest)
                        .setVoidMode(voidMode())
                        .setVoidDatasetIri(voidDatasetIri())
                        .setSpatial(params.spatial)
                        .setFeatures(params.features);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources).setSourceRoot(params.src);
                return builder.build();
            }
        }
    }

    public static Path mapToDestinationWithNewExtension(Path srcFile, Path srcDirectory, Path destDirectory, String newExt) {
        // Absolute before normalizing: Path.of(".").normalize() is the EMPTY path,
        // and no path startsWith the empty path, so "-src ." failed every file.
        Path normalizedSrcFile = srcFile.toAbsolutePath().normalize();
        Path normalizedSrcDir = srcDirectory.toAbsolutePath().normalize();
        Path normalizedDestDir = destDirectory.toAbsolutePath().normalize();
        if (!normalizedSrcFile.startsWith(normalizedSrcDir)) {
            throw new IllegalArgumentException("Source file " + srcFile + " is not located under source directory " + srcDirectory);
        }
        Path relativePath = normalizedSrcDir.relativize(normalizedSrcFile);
        Path parentInRelative = relativePath.getParent();
        Path destParent = (parentInRelative == null) ? normalizedDestDir : normalizedDestDir.resolve(parentInRelative);
        String originalName = relativePath.getFileName().toString();
        int dotIndex = originalName.lastIndexOf('.');
        String nameWithoutExt = (dotIndex == -1) ? originalName : originalName.substring(0, dotIndex);
        String newFileName = (newExt == null || newExt.isEmpty()) ? nameWithoutExt : nameWithoutExt + "." + newExt;
        return destParent.resolve(newFileName);
    }

    /**
     * The .h5 a per-file conversion of {@code src} writes. Under a directory
     * {@code -src} the source tree is mirrored below {@code -dest} with the
     * extension replaced. For a single-file {@code -src}, {@code -dest} names
     * the output file itself, or - when it is an existing directory - the
     * directory to put {@code <name>.h5} in (the -merge convention). Mapping a
     * single file through the tree rule made {@code -dest out.h5} a DIRECTORY
     * holding a file named ".h5".
     */
    public static Path destinationFor(Path src, File srcRoot, File dest) {
        if (srcRoot.isFile()) {
            if (dest.isDirectory()) {
                String name = src.getFileName().toString();
                int dot = name.lastIndexOf('.');
                return dest.toPath().resolve((dot == -1 ? name : name.substring(0, dot)) + ".h5");
            }
            return dest.toPath().toAbsolutePath().normalize();
        }
        return mapToDestinationWithNewExtension(src, srcRoot.toPath(), dest.toPath(), "h5");
    }

    /**
     * Groups the accepted sources by destination and reports every collision
     * (a.ttl, a.nt and a.rdf all map to a.h5): the extras are counted as
     * failed conversions and only the first source in path order is built.
     * Before this, a second source for the same .h5 was silently "skipped as
     * existing" single-threaded, and with -threads &gt; 1 two writers built the
     * same file at once.
     */
    static java.util.Map<Path, java.util.List<Path>> planDestinations(java.util.List<Path> sources, File srcRoot, File dest) {
        java.util.Map<Path, java.util.List<Path>> byDest = new java.util.TreeMap<>();
        for (Path p : sources) {
            byDest.computeIfAbsent(destinationFor(p, srcRoot, dest), k -> new ArrayList<>()).add(p);
        }
        return byDest;
    }

    class FileProcessor implements Callable<Model> {
        private final Path src;
        private final Path dest;
        private final FileCounter fc;

        public FileProcessor(Path src, Path dest, FileCounter fc) {
            this.src = src;
            this.dest = dest;
            this.fc = fc;
        }

        @Override
        public Model call() {
            // The Future from engine.submit() is never inspected, so anything
            // escaping this method is swallowed silently by FutureTask and the
            // file still counts as a success. EVERYTHING must be counted and
            // logged inside this catch - Errors included: an OutOfMemoryError
            // from an in-RAM engine on a big file used to be dropped on the
            // floor, the file reported as converted and the run exiting 0.
            try {
                if (dest.toFile().exists() && dest.toFile().length() > 0) {
                    if (!params.force) {
                        fc.incrementSkippedExistingCount();
                        logger.info("Skipping {}: destination {} exists (use -force to rebuild)", src, dest);
                        return null;
                    }
                    logger.info("Rebuilding {} over the existing {} (-force)", src, dest);
                }
                dest.getParent().toFile().mkdirs();
                newWriter(src.toFile(), null, dest.toFile()).write();
            } catch (Throwable ex) {
                fc.incrementFailedConversionFileCount();
                logger.error("Failed to convert {}", src, ex);
                if (ex instanceof VirtualMachineError) {
                    logger.error("The JVM reported {} while converting {}; later results in this run may be unreliable",
                            ex.getClass().getSimpleName(), src);
                }
                hintOnHeapExhaustion(ex, src.toString());
            }
            return null;
        }
    }
}
