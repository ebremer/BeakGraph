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
        // -merge is one big conversion, not a stream of per-file tasks; the
        // per-file progress bar would only ever render empty.
        if (params.status && !params.merge) {
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
