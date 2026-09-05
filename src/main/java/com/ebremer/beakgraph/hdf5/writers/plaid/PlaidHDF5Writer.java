package com.ebremer.beakgraph.hdf5.writers.plaid;

import com.ebremer.beakgraph.huge.AbstractDiskWriterBuilder;
import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
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
        Path workspace = Files.createTempDirectory(builder.workspaceBase(dest), ".bgplaid-");
        List<File> inputs = builder.getSources().isEmpty()
                ? List.of(builder.getSource())
                : builder.getSources();
        ForkJoinPool pool = new ForkJoinPool(builder.cores);
        try {
            // AtomicPublish.build: a unique sibling temp file, published only
            // on success, removed on ANY failure - an OutOfMemoryError included,
            // the realistic failure of this engine - with dest untouched; the
            // one publish discipline for every engine (BG-119, BG-140, BG-314).
            AtomicPublish.build(dest, tmp -> {
                // Prove the installed backend (native, or a replaced provider) can
                // write a file here before any work is done (BG-135).
                com.ebremer.beakgraph.huge.StreamingHdf5.requireWritable(workspace);
                com.ebremer.beakgraph.huge.StreamingHdf5.probeFile(tmp);   // the output path itself (BG-419)
                UltraSorterProvider provider = new UltraSorterProvider(
                        builder.getTermSpillBatch(), builder.getIdSpillBatch(), builder.getMergeFanIn(), builder.getTermSpillBytes(),
                        builder.effectiveMergeConcurrency(), pool);
                try (HugeBuildPipeline pipeline = new HugeBuildPipeline(
                        inputs, builder.getSpatial(), builder.getFeatures(), builder.getVoidMode(), workspace,
                        provider, pool, new PlaidIngest(builder.cores))) {
                    pipeline.setSourceRoot(builder.getSourceRoot());
                    pipeline.setVoidDatasetIri(builder.getVoidDatasetIri());
                    pipeline.run(tmp);
                }
            });
        } finally {
            // Stop and DRAIN the pool before touching the workspace: a spill
            // or merge still running would keep its run file open (undeletable
            // on Windows) and outlive write() (BG-134).
            com.ebremer.beakgraph.huge.Workspaces.drain(pool, logger);
            com.ebremer.beakgraph.huge.Workspaces.deleteTree(workspace, logger);
        }
        logger.info("Write complete: {}", builder.getDestination());
    }

    /**
     * Disk-engine defaults ({@link AbstractDiskWriterBuilder}): {@code 1 << 19}
     * term and {@code 1 << 22} id records per run, fan-in 128, term byte
     * budget heap / 16 (two batches in flight per sorter). Programmatic
     * {@code cores} default: every available processor (at least 2) - the
     * disk engines overlap parsing, spilling and merging, so they use what
     * the machine has; the CLI's {@code -cores} defaults to 4 (BG-276).
     */
    public static class Builder extends AbstractDiskWriterBuilder<Builder> {

        private int cores = Math.max(2, Runtime.getRuntime().availableProcessors());
        private int mergeConcurrency = 0;   // 0 = UltraSorterProvider.defaultMergeConcurrency(mergeFanIn, cores)

        public Builder() {
            super(1 << 19, 1 << 22, 128, com.ebremer.beakgraph.huge.SorterProvider.defaultTermSpillBytes(2));
        }

        /** Parse workers AND sort/spill/merge workers (two pools of this size). */
        public Builder setCores(int cores) {
            if (cores < 1) throw new IllegalArgumentException("cores must be >= 1, got " + cores);
            this.cores = cores;
            return this;
        }

        public int getCores() {
            return cores;
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
