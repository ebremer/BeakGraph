package com.ebremer.beakgraph.hdf5.writers.parallel;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.Index;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-threaded twin of {@link com.ebremer.beakgraph.hdf5.writers.HDF5Writer}:
 * same source formats, same output format, same readers - but each store is
 * built on a dedicated {@link ForkJoinPool} of {@code cores} threads (CLI:
 * {@code -method 2} with {@code -cores}, default {@value #DEFAULT_CORES}).
 * The three sub-dictionaries build concurrently, the columnar id lists
 * populate concurrently, every quad's dictionary ids are resolved once in a
 * parallel pass, and the GSPO/GPOS indexes are then built concurrently from
 * that shared id array. Only the final jHDF write of the finished buffers is
 * sequential.
 *
 * <p>All nested parallelism (parallel sorts, parallel streams) forks into the
 * writer's own pool, so a conversion never exceeds its core budget - with
 * {@code -threads N} in the CLI, N conversions run concurrently, each capped
 * at its own {@code -cores}.
 *
 * @author Erich Bremer
 */
public class ParallelHDF5Writer implements BeakGraphWriter {
    /** Default worker-thread count when the builder (or CLI -cores) does not set one. */
    public static final int DEFAULT_CORES = 4;

    private static final Logger logger = LoggerFactory.getLogger(ParallelHDF5Writer.class);
    private final Builder builder;

    private ParallelHDF5Writer(Builder builder) {
        this.builder = builder;
    }

    @Override
    public void write() throws IOException {
        logger.info("Writing BeakGraph to {} (parallel, {} cores)", builder.getDestination(), builder.getCores());
        Path dest = builder.getDestination().toPath();
        // Build into a sibling temp file and swap it in only on success, exactly
        // like the sequential writer: a failed rebuild must never destroy a
        // previous good artifact at dest, and readers never observe a
        // half-written file at the published path.
        Path tmp = dest.resolveSibling(dest.getFileName() + ".tmp");
        ForkJoinPool pool = new ForkJoinPool(builder.getCores());
        try {
            ParallelPositionalDictionaryWriterBuilder db = new ParallelPositionalDictionaryWriterBuilder();
            try (ParallelPositionalDictionaryWriter w = db
                    .setSource(builder.getSource())
                    .setSources(builder.getSources())
                    .setDestination(builder.getDestination())
                    .setName(Params.DICTIONARY)
                    .setSpatial(builder.getSpatial())
                    .setFeatures(builder.getFeatures())
                    .setPool(pool)
                    .buildParallel()) {
                Quad[] allQuads = w.getQuads();
                // Resolve every quad's four dictionary ids once; both index
                // orderings consume the same tuples. Clone BEFORE either build
                // starts: each index sorts its array in place, and cloning
                // concurrently with a sort could capture a torn permutation.
                ParallelBGIndex.QuadIds[] ids = ParallelBGIndex.resolveIds(w, allQuads, pool);
                ParallelBGIndex.QuadIds[] idsForGpos = ids.clone();
                allQuads = null;
                w.releaseQuads(); // index builds are id-only; let the Quad wrappers go

                ForkJoinTask<ParallelBGIndex> gspoTask = pool.submit(() -> new ParallelBGIndex(w, Index.GSPO, ids));
                ForkJoinTask<ParallelBGIndex> gposTask = pool.submit(() -> new ParallelBGIndex(w, Index.GPOS, idsForGpos));
                ParallelBGIndex gspo = joinIndex(gspoTask, Index.GSPO);
                ParallelBGIndex gpos = joinIndex(gposTask, Index.GPOS);

                logger.info("Creating HDF5 file {}", builder.getDestination());
                try (WritableHdfFile hdfFile = HdfFile.write(tmp)) {
                    final WritableGroup hdt = hdfFile.putGroup(builder.getName());
                    hdt.putAttribute("numQuads", w.getNumberOfQuads());
                    hdt.putAttribute("formatVersion", Params.FORMAT_VERSION);
                    w.add(hdt);
                    gspo.add(hdt);
                    gpos.add(hdt);
                }
            }
            try {
                Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException | RuntimeException ex) {
            // Only the temp file is ever cleaned up; dest is untouched on failure.
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException cleanup) {
                logger.warn("Failed to remove temp output {}", tmp, cleanup);
            }
            throw ex;
        } finally {
            pool.shutdown();
        }
        logger.info("Write complete: {}", builder.getDestination());
    }

    private static ParallelBGIndex joinIndex(ForkJoinTask<ParallelBGIndex> task, Index which) throws IOException {
        try {
            return task.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while building index " + which, ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException("Failed to build index " + which, cause);
        }
    }

    public static class Builder extends AbstractGraphBuilder<Builder> {
        private int cores = DEFAULT_CORES;

        public Builder setCores(int cores) {
            if (cores < 1) {
                throw new IllegalArgumentException("cores must be >= 1, got " + cores);
            }
            this.cores = cores;
            return this;
        }

        public int getCores() {
            return cores;
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
        public ParallelHDF5Writer build() {
            return new ParallelHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
