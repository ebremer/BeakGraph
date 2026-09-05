package com.ebremer.beakgraph.hdf5.writers.plaid;

import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.UltraSorterProvider;
import com.ebremer.beakgraph.huge.HugeBuildPipeline;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
        // Fail before any parsing or sorting if the HDF5 backend is missing (BG-441).
        com.ebremer.beakgraph.huge.StreamingHdf5.requireBackend();
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
            // Prove the installed backend (native, or a replaced provider) can
            // write a file here before any work is done (BG-135).
            com.ebremer.beakgraph.huge.StreamingHdf5.requireWritable(workspace);
            com.ebremer.beakgraph.huge.StreamingHdf5.probeFile(tmp);   // the output path itself (BG-419)
            UltraSorterProvider provider = new UltraSorterProvider(
                    builder.termSpillBatch, builder.idSpillBatch, builder.mergeFanIn, builder.termSpillBytes,
                    builder.effectiveMergeConcurrency(), pool);
            try (HugeBuildPipeline pipeline = new HugeBuildPipeline(
                    inputs, builder.getSpatial(), builder.getFeatures(), builder.getVoidMode(), workspace,
                    provider, pool, new PlaidIngest(builder.cores))) {
                pipeline.setSourceRoot(builder.getSourceRoot());
                pipeline.setVoidDatasetIri(builder.getVoidDatasetIri());
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
            // Stop and DRAIN the pool before touching the workspace: a spill
            // or merge still running would keep its run file open (undeletable
            // on Windows) and outlive write() (BG-134).
            com.ebremer.beakgraph.huge.Workspaces.drain(pool, logger);
            com.ebremer.beakgraph.huge.Workspaces.deleteTree(workspace, logger);
        }
        // Publish OUTSIDE the build's try/catch: a busy destination must not
        // delete a finished build (AtomicPublish keeps it as <dest>.new).
        AtomicPublish.publish(tmp, dest);
        logger.info("Write complete: {}", builder.getDestination());
    }

    public static class Builder extends AbstractGraphBuilder<Builder> {

        private Path workDir;
        private int cores = Math.max(2, Runtime.getRuntime().availableProcessors());
        private int termSpillBatch = 1 << 19;
        private int idSpillBatch = 1 << 22;
        private int mergeFanIn = 128;
        private long termSpillBytes = com.ebremer.beakgraph.huge.SorterProvider.defaultTermSpillBytes(2);
        private int mergeConcurrency = 0;   // 0 = UltraSorterProvider.defaultMergeConcurrency(mergeFanIn, cores)

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

        /**
         * Maximum spill runs merged in one pass. Each running merge holds
         * {@code fanIn + 1} open files; a level runs at most
         * {@link #setMergeConcurrency} merges at a time.
         */
        public Builder setMergeFanIn(int fanIn) {
            if (fanIn < 2) throw new IllegalArgumentException("mergeFanIn must be >= 2");
            this.mergeFanIn = fanIn;
            return this;
        }

        /**
         * Merge groups one sorter level runs concurrently (default: at most
         * one per core and no more than fit 1024 open files at
         * {@code mergeFanIn + 1} each). The peak is {@code concurrency x
         * (mergeFanIn + 1)} file descriptors and, for the term sorters,
         * {@code concurrency x termSpillBatch} live records (BG-133).
         */
        public Builder setMergeConcurrency(int merges) {
            if (merges < 1) throw new IllegalArgumentException("mergeConcurrency must be >= 1");
            this.mergeConcurrency = merges;
            return this;
        }

        int effectiveMergeConcurrency() {
            return (mergeConcurrency > 0) ? mergeConcurrency
                    : com.ebremer.beakgraph.hdf5.writers.hugeUltra.UltraSorterProvider.defaultMergeConcurrency(mergeFanIn, cores);
        }

        /**
         * Estimated heap of parsed terms one term sorter buffers before a run
         * spills, whatever the record count (default: max heap / 16 - three
         * term sorters, each with a batch spilling in the background). The
         * bound that keeps multi-KB WKT literals from exhausting the heap
         * (BG-125).
         */
        public Builder setTermSpillBytes(long bytes) {
            if (bytes < 1) throw new IllegalArgumentException("termSpillBytes must be >= 1");
            this.termSpillBytes = bytes;
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
            requireSourceAndDestination();
            return new PlaidHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
