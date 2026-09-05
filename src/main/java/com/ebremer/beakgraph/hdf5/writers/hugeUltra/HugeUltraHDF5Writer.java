package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
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
 * <li>spill runs sort and write on background workers while ingestion
 *     continues (double-buffered);</li>
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
        Path tmp = AtomicPublish.tempFor(dest);
        Path workBase = (builder.workDir != null) ? builder.workDir
                : (dest.toAbsolutePath().getParent() != null ? dest.toAbsolutePath().getParent() : Path.of("."));
        Path workspace = Files.createTempDirectory(workBase, ".bghugeultra-");
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
                    inputs, builder.getSpatial(), builder.getFeatures(), builder.getVoidMode(),
                    workspace, provider, pool)) {
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
        private int termSpillBatch = 1 << 19;  // 524288 term records per run
        private int idSpillBatch = 1 << 22;    // 4M packed id records per run
        private int mergeFanIn = 128;
        private long termSpillBytes = com.ebremer.beakgraph.huge.SorterProvider.defaultTermSpillBytes(2);
        private int mergeConcurrency = 0;   // 0 = UltraSorterProvider.defaultMergeConcurrency(mergeFanIn, cores)

        /** Workspace for spill runs; needs disk on the order of a few times the source. */
        public Builder setWorkDirectory(Path dir) {
            this.workDir = dir;
            return this;
        }

        /** Worker threads for sorting, spilling, merging, and concurrent stages. */
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
        public HugeUltraHDF5Writer build() {
            requireSourceAndDestination();
            return new HugeUltraHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
