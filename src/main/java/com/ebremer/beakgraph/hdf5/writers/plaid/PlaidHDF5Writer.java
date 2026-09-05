package com.ebremer.beakgraph.hdf5.writers.plaid;

import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.UltraSorterProvider;
import com.ebremer.beakgraph.huge.HugeBuildPipeline;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The plaid writer (CLI: {@code -method 5}): everything the hugeUltra
 * disk-based writer is - bounded RAM at any quad count, radix-sorted
 * bit-packed spill runs, background spilling, concurrent pipeline stages -
 * plus PARALLEL MULTI-FILE INGEST. Where methods 1 and 4 parse sources one
 * after another on a single thread, plaid parses up to {@code -cores}
 * documents concurrently ({@link PlaidIngest}); only the row-assignment sink
 * remains serial. For merges of many files (the natural shape of
 * billion-quad inputs) this converts the parse phase - the wall-clock
 * majority of a method-4 build - from ~2 busy cores to genuinely
 * {@code -cores} busy cores.
 *
 * <p>Single-source builds work too (the one document parses on one worker,
 * i.e. method-4 behaviour). Output is identical to methods 1/4: row numbers
 * interleave differently, but rows are internal - all sorted artifacts, and
 * therefore the store, are the same. Requires the native HDF5 backend.
 *
 * @author Erich Bremer
 */
public class PlaidHDF5Writer implements BeakGraphWriter {

    private static final Logger logger = LoggerFactory.getLogger(PlaidHDF5Writer.class);

    private final Builder builder;

    private PlaidHDF5Writer(Builder builder) {
        this.builder = builder;
    }

    @Override
    public void write() throws IOException {
        logger.info("Writing BeakGraph (plaid: parallel-ingest disk-based, {} cores) to {}",
                builder.cores, builder.getDestination());
        // Fail before any parsing or sorting if the native HDF5 library is missing (BG-441).
        com.ebremer.beakgraph.huge.NativeHdf5File.requireAvailable();
        Path dest = builder.getDestination().toPath();
        Path tmp = AtomicPublish.tempFor(dest);
        Path workBase = (builder.workDir != null) ? builder.workDir
                : (dest.toAbsolutePath().getParent() != null ? dest.toAbsolutePath().getParent() : Path.of("."));
        Path workspace = Files.createTempDirectory(workBase, ".bgplaid-");
        List<File> inputs = builder.getSources().isEmpty()
                ? List.of(builder.getSource())
                : builder.getSources();
        ForkJoinPool pool = new ForkJoinPool(builder.cores);
        try {
            UltraSorterProvider provider = new UltraSorterProvider(
                    builder.termSpillBatch, builder.idSpillBatch, builder.mergeFanIn, pool);
            try (HugeBuildPipeline pipeline = new HugeBuildPipeline(
                    inputs, builder.getSpatial(), builder.getFeatures(), builder.getVoidMode(), workspace,
                    provider, pool, new PlaidIngest(builder.cores))) {
                pipeline.setSourceRoot(builder.getSourceRoot());
                pipeline.run(tmp);
            }
        } catch (IOException | RuntimeException | Error ex) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException cleanup) {
                logger.warn("Failed to remove temp output {}", tmp, cleanup);
            }
            throw ex;
        } finally {
            pool.shutdown();
            deleteRecursively(workspace);
        }
        // Publish OUTSIDE the build's try/catch: a busy destination must not
        // delete a finished build (AtomicPublish keeps it as <dest>.new).
        AtomicPublish.publish(tmp, dest);
        logger.info("Write complete: {}", builder.getDestination());
    }

    private static void deleteRecursively(Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                    Files.deleteIfExists(d);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.warn("Failed to remove workspace {}", dir, e);
        }
    }

    public static class Builder extends AbstractGraphBuilder<Builder> {

        private Path workDir;
        private int cores = Math.max(2, Runtime.getRuntime().availableProcessors());
        private int termSpillBatch = 1 << 19;
        private int idSpillBatch = 1 << 22;
        private int mergeFanIn = 128;

        /** Workspace for spill runs; needs disk on the order of a few times the source. */
        public Builder setWorkDirectory(Path dir) {
            this.workDir = dir;
            return this;
        }

        /** Parse workers AND sort/spill/merge workers (two pools of this size). */
        public Builder setCores(int cores) {
            if (cores < 1) throw new IllegalArgumentException("cores must be >= 1, got " + cores);
            this.cores = cores;
            return this;
        }

        public Builder setTermSpillBatch(int records) {
            if (records < 1) throw new IllegalArgumentException("termSpillBatch must be >= 1");
            this.termSpillBatch = records;
            return this;
        }

        public Builder setIdSpillBatch(int records) {
            if (records < 1) throw new IllegalArgumentException("idSpillBatch must be >= 1");
            this.idSpillBatch = records;
            return this;
        }

        public Builder setMergeFanIn(int fanIn) {
            if (fanIn < 2) throw new IllegalArgumentException("mergeFanIn must be >= 2");
            this.mergeFanIn = fanIn;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public String getName() {
            return Params.BG;
        }

        @Override
        public PlaidHDF5Writer build() {
            return new PlaidHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
