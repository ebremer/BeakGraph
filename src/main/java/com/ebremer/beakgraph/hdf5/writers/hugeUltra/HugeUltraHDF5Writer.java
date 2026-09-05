package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
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
            UltraSorterProvider provider = new UltraSorterProvider(
                    builder.termSpillBatch, builder.idSpillBatch, builder.mergeFanIn, pool);
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
        private int termSpillBatch = 1 << 19;  // 524288 term records per run
        private int idSpillBatch = 1 << 22;    // 4M packed id records per run
        private int mergeFanIn = 128;

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
        public HugeUltraHDF5Writer build() {
            return new HugeUltraHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
