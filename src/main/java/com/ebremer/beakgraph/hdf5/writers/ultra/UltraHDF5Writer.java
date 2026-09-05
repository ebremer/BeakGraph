package com.ebremer.beakgraph.hdf5.writers.ultra;

import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The most aggressively parallel in-memory BeakGraph writer (CLI:
 * {@code -method 3}, thread count via {@code -cores}). Same source formats,
 * same HDF5 output format, same readers as
 * {@link com.ebremer.beakgraph.hdf5.writers.HDF5Writer}; the entire build is
 * restructured as a dependency DAG on one dedicated {@link ForkJoinPool}:
 *
 * <pre>
 *  parse docs (parallel)  ->  dedup sets + stats (parallel)
 *      -> sort entities/predicates/literals (one shared sort each)
 *          -> storage dictionary encodes  \
 *          -> node-&gt;id hash maps           } run CONCURRENTLY
 *              -> columnar id lists        /
 *              -> pack GSPO keys -> radix sort -> dedup once
 *                  -> GSPO emission || GPOS repack/sort/emission
 *      -> single sequential jHDF write of the finished buffers
 * </pre>
 *
 * Relative to the {@code -method 2} parallel writer this removes its three
 * serial tails: per-quad binary-search id resolution (O(1) rank maps), the
 * comparator object sort (packed primitive keys, radix sorted), and the
 * single-threaded index emission scan (chunked two-pass positional writes).
 *
 * <p>Output is semantically identical to the sequential writer's and, for a
 * single source document, structurally identical (same datasets, sizes, and
 * attributes; bytes differ only through VoID's per-write random bnode
 * labels). Multi-source merges label blank nodes per document ordinal, giving
 * an isomorphic - not structurally identical - store, same as the huge
 * writer's merge.
 *
 * @author Erich Bremer
 */
public class UltraHDF5Writer implements BeakGraphWriter {

    /** Default worker-thread count when the builder (or CLI -cores) does not set one. */
    public static final int DEFAULT_CORES = 4;

    private static final Logger logger = LoggerFactory.getLogger(UltraHDF5Writer.class);
    private final Builder builder;

    private UltraHDF5Writer(Builder builder) {
        this.builder = builder;
    }

    @Override
    public void write() throws IOException {
        logger.info("Writing BeakGraph to {} (ultra, {} cores)", builder.getDestination(), builder.getCores());
        Path dest = builder.getDestination().toPath();
        // Same publish discipline as every other writer: build into a sibling
        // temp file, swap in atomically on success, never disturb a previous
        // good artifact, never expose a half-written file.
        Path tmp = AtomicPublish.tempFor(dest);
        final long total = System.nanoTime();
        ForkJoinPool pool = new ForkJoinPool(builder.getCores());
        try {
            logger.info("Stage 1/4: ingest (parallel parse + dedup + statistics)");
            UltraIngest ingest = new UltraIngest();
            ingest.setSource(builder.getSource());
            ingest.setSources(builder.getSources());
            ingest.setSourceRoot(builder.getSourceRoot());
            ingest.setSpatial(builder.getSpatial());
            ingest.setVoidMode(builder.getVoidMode());
            ingest.setDestination(builder.getDestination());
            ingest.setFeatures(builder.getFeatures());
            ingest.setName(Params.DICTIONARY);
            ingest.ingest(pool);

            // Dictionary storage encodes in the background; the index stage
            // needs only the id maps and starts immediately.
            logger.info("Stage 2/4: dictionaries (sorts, rank maps; encoding + columns in background)");
            UltraDictionary dict = new UltraDictionary(ingest, pool);
            logger.info("Stage 3/4: GSPO/GPOS indexes (packed keys, sort, dedup, parallel emission)");
            UltraBGIndex[] indexes = UltraBGIndex.buildBoth(dict, ingest, pool);
            dict.awaitStorage();
            // The rank maps and the ingest's node sets served the packing pass
            // and the column fills, which awaitStorage() has just joined; drop
            // them before the HDF5 emission allocates its buffers (BG-115).
            dict.releaseRankMaps();
            ingest.releaseNodeSets();

            logger.info("Stage 4/4: writing HDF5 file {}", builder.getDestination());
            long ioStart = System.nanoTime();
            try (WritableHdfFile hdfFile = HdfFile.write(tmp)) {
                final WritableGroup hdt = hdfFile.putGroup(builder.getName());
                hdt.putAttribute("numQuads", ingest.getNumberOfQuads());
                hdt.putAttribute("formatVersion", Params.FORMAT_VERSION);
                dict.add(hdt);
                indexes[0].add(hdt);
                indexes[1].add(hdt);
            }
            logger.info("HDF5 file written in {} ms", (System.nanoTime() - ioStart) / 1_000_000L);
        } catch (IOException | RuntimeException | Error ex) {
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
        // Publish OUTSIDE the build's try/catch: a busy destination must not
        // delete a finished build (AtomicPublish keeps it as <dest>.new).
        AtomicPublish.publish(tmp, dest);
        logger.info("Write complete in {} ms: {}", (System.nanoTime() - total) / 1_000_000L, builder.getDestination());
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
        public UltraHDF5Writer build() {
            requireSourceAndDestination();
            return new UltraHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
