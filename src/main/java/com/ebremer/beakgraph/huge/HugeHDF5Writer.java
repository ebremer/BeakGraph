package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disk-based BeakGraph writer for very large graphs. Produces the same HDF5
 * format as {@link com.ebremer.beakgraph.hdf5.writers.HDF5Writer} (readable by
 * the existing {@code HDF5Reader}), but sorts and indexes on disk: quads are
 * spilled to a temp workspace, dictionaries are derived by external merge sort,
 * ids are assigned by sort-merge joins, and every dataset streams into the file
 * through the {@link StreamingHdf5} backend. Peak heap stays bounded by the
 * configured spill batch sizes; the size ceiling moves from RAM to free disk in
 * the workspace (roughly a few times the source size, transiently).
 *
 * <p>Usage mirrors HDF5Writer:
 * <pre>{@code
 * HugeHDF5Writer.Builder()
 *     .setSource(new File("data.nq.gz"))
 *     .setDestination(new File("data.h5"))
 *     .setSpatial(true)
 *     .build()
 *     .write();
 * }</pre>
 *
 * <p>Output is semantically identical to the RAM writer's (same dictionaries,
 * indexes, VoID metadata, spatial augmentation); the only divergence is blank
 * node ordering (labels are kept as parsed rather than relabelled), which
 * yields an isomorphic - not byte-identical - store.
 *
 * <p>Like the other writers, {@code setSources(List)} merges several source
 * documents into the one store being written (-merge), with blank nodes kept
 * distinct per document.
 *
 * @author Erich Bremer
 */
public class HugeHDF5Writer implements BeakGraphWriter {

    private static final Logger logger = LoggerFactory.getLogger(HugeHDF5Writer.class);

    private final Builder builder;

    private HugeHDF5Writer(Builder builder) {
        this.builder = builder;
    }

    @Override
    public void write() throws IOException {
        logger.info("Writing BeakGraph (huge/disk-based) to {}", builder.getDestination());
        Path dest = builder.getDestination().toPath();
        // Same publish discipline as HDF5Writer: build into a sibling temp file,
        // swap in atomically on success, never disturb a previous good artifact.
        Path tmp = dest.resolveSibling(dest.getFileName() + ".tmp");
        Path workBase = (builder.workDir != null) ? builder.workDir
                : (dest.toAbsolutePath().getParent() != null ? dest.toAbsolutePath().getParent() : Path.of("."));
        Path workspace = Files.createTempDirectory(workBase, ".bghuge-");
        // -merge: every source in the list is parsed into this one store
        // (blank nodes stay distinct per document); otherwise the single src.
        List<File> inputs = builder.getSources().isEmpty()
                ? List.of(builder.getSource())
                : builder.getSources();
        try {
            try (HugeBuildPipeline pipeline = new HugeBuildPipeline(
                    inputs, builder.getSpatial(), builder.getFeatures(), builder.getVoidMode(),
                    workspace, builder.termSpillBatch, builder.idSpillBatch, builder.mergeFanIn)) {
                pipeline.run(tmp);
            }
            try {
                Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException | RuntimeException ex) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException cleanup) {
                logger.warn("Failed to remove temp output {}", tmp, cleanup);
            }
            throw ex;
        } finally {
            deleteRecursively(workspace);
        }
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
        private int termSpillBatch = 1 << 18;  // 262144 (term, row) records per column before a run spills
        private int idSpillBatch = 1 << 21;    // 2M numeric records per run for the id/quad sorts
        private int mergeFanIn = 64;

        /**
         * Directory for the build workspace (spill runs, staged buffers).
         * Defaults to the destination's directory - the workspace transiently
         * needs disk on the order of a few times the (uncompressed) source.
         */
        public Builder setWorkDirectory(Path dir) {
            this.workDir = dir;
            return this;
        }

        /** Records buffered in RAM per term column before spilling a sorted run. */
        public Builder setTermSpillBatch(int records) {
            if (records < 1) throw new IllegalArgumentException("termSpillBatch must be >= 1");
            this.termSpillBatch = records;
            return this;
        }

        /** Records buffered in RAM per numeric (id/quad) sorter before spilling. */
        public Builder setIdSpillBatch(int records) {
            if (records < 1) throw new IllegalArgumentException("idSpillBatch must be >= 1");
            this.idSpillBatch = records;
            return this;
        }

        /** Maximum spill runs merged in one pass. */
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
        public HugeHDF5Writer build() {
            return new HugeHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
