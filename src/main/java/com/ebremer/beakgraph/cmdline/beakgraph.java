package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.fuseki.SPARQLEndPoint;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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
 *
 * @author Erich Bremer
 */
public class beakgraph {
    private static final Logger logger = LoggerFactory.getLogger(beakgraph.class);
    private static ProgressBar progressBar = null;
    private final FileCounter fc;
    private Parameters params;   // ← fixed: instance field

    public beakgraph(Parameters params) {
        JenaSystem.init();
        this.params = params;
        this.fc = new FileCounter();
        String os = System.getProperty("os.name").toLowerCase();
        ProgressBarStyle style = os.contains("win") ? ProgressBarStyle.ASCII : ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
        if (params.status) {
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
                        JenaSystem.init();
                        beakgraph bg = new beakgraph(params);
                        bg.Traverse();
                    } else if (params.src != null) {
                        System.out.println("Source does not exist! " + params.src);
                    }
                }
            } catch (ParameterException ex) {
                if (params.version) {
                    System.out.println("beakgraph - Version : " + Params.VERSION);
                } else {
                    System.out.println(ex.getMessage());
                }
            }
        }
    }

    public void Traverse() {
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
                        fc.getZeroFileCount();
                        return false;
                    }
                    if (p.toFile().toString().toLowerCase().endsWith(".ttl.gz") || p.toFile().toString().toLowerCase().endsWith(".ttl")) {
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
                }
            }
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(beakgraph.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (params.status) {
            System.out.println(fc);
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
            Path dest = mapToDestinationWithNewExtension(src, params.src.toPath(), params.dest.toPath(), "h5");
            if (dest.toFile().exists()) {
                dest.toFile().delete();
            }
            dest.getParent().toFile().mkdirs();
            try {
                HDF5Writer.Builder()
                    .setSource(src.toFile())
                    .setDestination(dest.toFile())
                    .setSpatial(params.spatial)
                    .build()
                    .write();
            } catch (IOException ex) {
                fc.incrementFailedConversionFileCount();
                throw new Error(ex.getMessage());
            } catch (Throwable ex) {
                IO.println("***********************************************************************************\n" + ex.getMessage() + "\n*******************************************");
                ex.printStackTrace();
                throw new Error(ex.getMessage());
            }
            return null;
        }
    }
}
