package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.huge.AbstractDiskWriterBuilder;
import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraphWriter;
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
 * The parallel disk-based BeakGraph writer (CLI: {@code -method 4}, threads
 * via {@code -cores}) for graphs of billions of quads: the exact
 * {@link HugeBuildPipeline} flow and output of {@code -method 1} - RAM stays
 * bounded by spill-batch sizes regardless of quad count - but every heavy
 * component is swapped for a parallel one:
 *
 * <ul>
 * <li>spill runs sort and write on a background worker while ingestion
 *     continues - one spill in flight, ingestion backpressured on the previous
 *     one; when the shared pool is saturated by stage tasks the spill runs
 *     on the ingesting thread itself (BG-138);</li>
 * <li>the three column sorts, three dictionary encodes, and three id joins
 *     each run concurrently;</li>
 * <li>encoded quads and (row, id) join records sort as bit-packed primitive
 *     keys - parallel radix sort per run, fixed-width binary spills, no
 *     objects ({@link PackedQuadSorter}, {@link PackedRowIdSorter});</li>
 * <li>term runs group each distinct term's text once per run and compare via
 *     an order-preserving 8-byte prefix key ({@link UltraSorterProvider});</li>
 * <li>GPOS sorts only the DEDUPLICATED quad set teed out of the GSPO
 *     scan.</li>
 * </ul>
 *
 * Output is isomorphic to the other writers' (same divergence -method 1 has:
 * blank nodes keep parsed labels rather than being relabelled). Requires the
 * native HDF5 backend, like -method 1. Bigger spill batches = fewer merge
 * levels = less disk churn; size them to your heap
 * ({@code setIdSpillBatch(1 << 26)} at 16 GiB+ is reasonable for
 * 10B+ quad builds).
 *
 * @author Erich Bremer
 */
public class HugeUltraHDF5Writer implements BeakGraphWriter {

    private static final Logger logger = LoggerFactory.getLogger(HugeUltraHDF5Writer.class);

    private final Builder builder;

    private HugeUltraHDF5Writer(Builder builder) {
        this.builder = builder;
    }

    @Override
    public void write() throws IOException {
        logger.info("Writing BeakGraph (hugeUltra: disk-based, {} cores) to {}",
                builder.cores, builder.getDestination());
        // Fail before any parsing or sorting if the HDF5 backend is missing (BG-441).
        com.ebremer.beakgraph.huge.StreamingHdf5.requireBackend();
        Path dest = builder.getDestination().toPath();
        Path workspace = Files.createTempDirectory(builder.workspaceBase(dest), ".bghugeultra-");
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
                        inputs, builder.getSpatial(), builder.getFeatures(), builder.getVoidMode(),
                        workspace, provider, pool)) {
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

        /** Worker threads for sorting, spilling, merging, and concurrent stages. */
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
        public HugeUltraHDF5Writer build() {
            requireSourceAndDestination();
            return new HugeUltraHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
