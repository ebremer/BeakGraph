package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.SPARQLEndPoint;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sys.JenaSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line entry point for BeakGraph ("beakgraph" command): converts RDF source trees to
 * HDF5-backed graphs, or starts the SPARQL/LWS endpoint.
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
        this.params = params;
        this.fc = new FileCounter();
        String os = System.getProperty("os.name").toLowerCase();
        ProgressBarStyle style = os.contains("win") ? ProgressBarStyle.ASCII : ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
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

    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        logger.info(String.format("%s %s", "beakgraph ", Arrays.toString(args)));
        Parameters params = new Parameters();
        JCommander jc = JCommander.newBuilder().addObject(params).build();
        jc.setProgramName("beakgraph");
        if (args.length != 0) {
            try {
                jc.parse(args);
                if (params.version) {
                    // Must be handled on the SUCCESS path: version was previously
                    // printed only inside the ParameterException catch, so a plain
                    // "-v" parsed fine, matched no branch, and printed nothing.
                    System.out.println("beakgraph - Version : " + Params.VERSION);
                    System.exit(0);
                }
                if (params.help) {
                    jc.usage();
                    System.exit(0);
                } else {
                    if (params.sparqlendpoint != null) {
                        if (!params.sparqlendpoint.exists()) {
                            System.err.println("Error: -endpoint does not exist: " + params.sparqlendpoint);
                            System.exit(1);
                        }
                        SPARQLEndPoint endpoint = SPARQLEndPoint.getSPARQLEndPoint(params);
                        Runtime.getRuntime().addShutdownHook(new Thread(() -> endpoint.shutdown()));
                        System.out.println("Press Ctrl+C to stop the server...");
                        try {
                            Thread.currentThread().join();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else if (params.src != null && params.src.exists() && params.export != null) {
                        // Export mode: dump BeakGraph(s) back to RDF; no -dest
                        // involved (output lands beside each source .h5).
                        BeakGraphCLI bg = new BeakGraphCLI(params);
                        bg.export();
                        if (bg.fc.getFailedConversionFileCount() > 0) {
                            System.exit(2);
                        }
                    } else if (params.src != null && params.src.exists()) {
                        if (params.dest == null) {
                            // Without this guard every FileProcessor NPEs inside a
                            // discarded Future: nothing converts, nothing is logged,
                            // and the run exits 0 reporting success.
                            System.err.println("Error: -dest is required with -src");
                            jc.usage();
                            System.exit(1);
                        }
                        JenaSystem.init();
                        BeakGraphCLI bg = new BeakGraphCLI(params);
                        if (params.merge) {
                            bg.merge();
                        } else {
                            bg.traverse();
                        }
                        if (bg.fc.getFailedConversionFileCount() > 0) {
                            System.exit(2);
                        }
                    } else if (params.src != null) {
                        System.err.println("Error: -src does not exist: " + params.src);
                        System.exit(1);
                    }
                }
            } catch (ParameterException ex) {
                if (params.version) {
                    System.out.println("beakgraph - Version : " + Params.VERSION);
                } else {
                    // Bad arguments are an error: say so on stderr and exit non-zero
                    // (scripts used to see a successful exit 0 for a failed run).
                    System.err.println(ex.getMessage());
                    jc.usage();
                    System.exit(1);
                }
            }
        }
    }

    public void traverse() {
        try (ThreadPoolExecutor engine = new ThreadPoolExecutor(params.threads, params.threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>())) {
            engine.prestartAllCoreThreads();
            Files.walk(params.src.toPath())
                .parallel()
                .filter(p -> {
                    if (p.toFile().isDirectory()) {
                        fc.incrementDirectoryCount();
                        return false;
                    }
                    if (p.toFile().length() == 0) {
                        fc.incrementZeroLengthFileCount();
                        return false;
                    }
                    if (RdfSources.isSupported(p.getFileName().toString())) {
                        return true;
                    }
                    fc.incrementOtherFileCount();
                    return false;
                })
                .forEach(p -> {
                    fc.incrementRDFFileCount();
                    if (params.status) {
                        progressBar.maxHint(fc.getRDFFileCount());
                        progressBar.stepTo(engine.getCompletedTaskCount());
                    }
                    engine.submit(new FileProcessor(p, fc));
                });
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
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(BeakGraphCLI.class.getName()).log(Level.SEVERE, null, ex);
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
        try (var walk = Files.walk(params.src.toPath())) {
            walk.filter(p -> {
                    File f = p.toFile();
                    if (f.isDirectory()) {
                        fc.incrementDirectoryCount();
                        return false;
                    }
                    if (f.length() == 0) {
                        fc.incrementZeroLengthFileCount();
                        return false;
                    }
                    if (RdfSources.isSupported(p.getFileName().toString())) {
                        return true;
                    }
                    fc.incrementOtherFileCount();
                    return false;
                })
                .sorted()
                .forEach(p -> {
                    fc.incrementRDFFileCount();
                    inputs.add(p.toFile());
                });
        } catch (IOException ex) {
            logger.error("Failed to scan source tree {}", params.src, ex);
        }
        if (inputs.isEmpty()) {
            System.err.println("No supported RDF sources found under " + params.src);
            return;
        }
        File dest = params.dest;
        if (dest.isDirectory()) {
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
        } catch (Exception ex) {
            fc.incrementFailedConversionFileCount();
            logger.error("Failed to merge {} sources into {}", inputs.size(), dest, ex);
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
                                && p.getFileName().toString().toLowerCase().endsWith(".h5")
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
            System.err.println("No BeakGraph (.h5) files found under " + params.src);
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
            String base = h5.getName();
            if (base.toLowerCase().endsWith(".h5")) {
                base = base.substring(0, base.length() - 3);
            }
            Path out = h5.toPath().resolveSibling(base + "." + ext + (params.compress ? ".gz" : ""));
            Path tmp = out.resolveSibling(out.getFileName() + ".tmp");
            logger.info("Exporting {} -> {} ({})", h5.getName(), out.getFileName(), fmt);
            try (OutputStream os = openExportStream(tmp)) {
                writeExport(os, dsg, fmt);
            } catch (Exception ex) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {}
                throw ex;
            }
            try {
                Files.move(tmp, out, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, out, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            logger.info("Export complete: {}", out);
        }
    }

    private OutputStream openExportStream(Path tmp) throws IOException {
        OutputStream os = new java.io.BufferedOutputStream(Files.newOutputStream(tmp), 1 << 17);
        return params.compress ? new java.util.zip.GZIPOutputStream(os, 1 << 16) : os;
    }

    /** A graph the USER put in the store (not BeakGraph's own metadata graphs). */
    private static boolean isUserGraph(org.apache.jena.graph.Node g) {
        return !Params.BGVOID.equals(g) && !Params.SPATIAL.equals(g);
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

    private static void writeExport(OutputStream os, org.apache.jena.sparql.core.DatasetGraph dsg, String fmt)
            throws IOException {
        switch (fmt) {
            case "NT", "TTL" -> {
                // Triple export: by this point the store has no user named
                // graphs, so the default graph IS the data.
                org.apache.jena.riot.system.StreamRDF stream = org.apache.jena.riot.system.StreamRDFWriter
                        .getWriterStream(os, "NT".equals(fmt)
                                ? org.apache.jena.riot.RDFFormat.NTRIPLES
                                : org.apache.jena.riot.RDFFormat.TURTLE_BLOCKS);
                stream.start();
                var it = dsg.getDefaultGraph().find();
                while (it.hasNext()) {
                    stream.triple(it.next());
                }
                stream.finish();
            }
            case "NQ", "TRIG" -> {
                org.apache.jena.riot.system.StreamRDF stream = org.apache.jena.riot.system.StreamRDFWriter
                        .getWriterStream(os, "NQ".equals(fmt)
                                ? org.apache.jena.riot.RDFFormat.NQUADS
                                : org.apache.jena.riot.RDFFormat.TRIG_BLOCKS);
                stream.start();
                var it = dsg.find();
                while (it.hasNext()) {
                    var q = it.next();
                    if (isUserGraph(q.getGraph())) {
                        stream.quad(q);
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
                    if (isUserGraph(q.getGraph())) {
                        copy.add(q);
                    }
                }
                org.apache.jena.riot.RDFDataMgr.write(os, copy, org.apache.jena.riot.Lang.JSONLD);
            }
        }
    }

    /**
     * The conversion engine for this run: {@code -method} (0 = in-memory,
     * 1 = disk, 2 = parallel, 3 = ultra), with the legacy {@code -huge} flag
     * acting as "-method 1" when no explicit -method was given.
     */
    private int effectiveMethod() {
        return (params.method == 0 && params.huge) ? 1 : params.method;
    }

    /**
     * Builds the writer selected by {@link #effectiveMethod()} for one
     * source-or-sources -> destination conversion. Shared by the per-file
     * processors and -merge so the two can never route differently.
     */
    private BeakGraphWriter newWriter(File source, List<File> sources, File dest) throws IOException {
        switch (effectiveMethod()) {
            case 1 -> {
                // Disk-based build: same output format, but sorting/indexing
                // spill to a workspace instead of the heap. Needs the native
                // HDF5 backend on the classpath (the hdf5-backend-* profiles);
                // with -threads N, N builds run concurrently, each with its
                // own workspace.
                HugeHDF5Writer.Builder builder = HugeHDF5Writer.Builder()
                        .setDestination(dest)
                        .setSpatial(params.spatial)
                        .setFeatures(params.features);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources);
                if (params.workdir != null) {
                    params.workdir.mkdirs();
                    builder.setWorkDirectory(params.workdir.toPath());
                }
                return builder.build();
            }
            case 2 -> {
                // Multi-threaded in-memory build on a pool of -cores threads.
                ParallelHDF5Writer.Builder builder = ParallelHDF5Writer.Builder()
                        .setDestination(dest)
                        .setSpatial(params.spatial)
                        .setFeatures(params.features)
                        .setCores(params.cores);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources);
                return builder.build();
            }
            case 3 -> {
                // Ultra in-memory build: parallel parse, packed-key radix-sorted
                // indexes, parallel emission - also capped at -cores threads.
                UltraHDF5Writer.Builder builder = UltraHDF5Writer.Builder()
                        .setDestination(dest)
                        .setSpatial(params.spatial)
                        .setFeatures(params.features)
                        .setCores(params.cores);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources);
                return builder.build();
            }
            case 4 -> {
                // hugeUltra: the disk-based pipeline (bounded RAM, any quad
                // count) on parallel primitive sorting machinery - the engine
                // for multi-billion-quad builds. Needs the native HDF5 backend.
                HugeUltraHDF5Writer.Builder builder = HugeUltraHDF5Writer.Builder()
                        .setDestination(dest)
                        .setSpatial(params.spatial)
                        .setFeatures(params.features)
                        .setCores(params.cores);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources);
                if (params.workdir != null) {
                    params.workdir.mkdirs();
                    builder.setWorkDirectory(params.workdir.toPath());
                }
                return builder.build();
            }
            default -> {
                HDF5Writer.Builder builder = HDF5Writer.Builder()
                        .setDestination(dest)
                        .setSpatial(params.spatial)
                        .setFeatures(params.features);
                if (source != null) builder.setSource(source);
                if (sources != null) builder.setSources(sources);
                return builder.build();
            }
        }
    }

    public static Path mapToDestinationWithNewExtension(Path srcFile, Path srcDirectory, Path destDirectory, String newExt) {
        Path normalizedSrcFile = srcFile.normalize();
        Path normalizedSrcDir = srcDirectory.normalize();
        Path normalizedDestDir = destDirectory.normalize();
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

    class FileProcessor implements Callable<Model> {
        private final Path src;
        private final FileCounter fc;

        public FileProcessor(Path src, FileCounter fc) {
            this.src = src;
            this.fc = fc;
        }

        @Override
        public Model call() {
            // The Future from engine.submit() is never inspected, so anything
            // escaping this method is swallowed silently by FutureTask and the
            // file still counts as a success. EVERYTHING - including destination
            // mapping - must be counted and logged inside this catch.
            try {
                Path dest = mapToDestinationWithNewExtension(src, params.src.toPath(), params.dest.toPath(), "h5");
                if (dest.toFile().exists() && dest.toFile().length() > 0) {
                    return null;
                }
                dest.getParent().toFile().mkdirs();
                newWriter(src.toFile(), null, dest.toFile()).write();
            } catch (Exception ex) {
                fc.incrementFailedConversionFileCount();
                logger.error("Failed to convert {}", src, ex);
            }
            return null;
        }
    }
}
