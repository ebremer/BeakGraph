package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * configured spill batches - a record cap per sorter plus a byte budget per
 * term sorter ({@link Builder#setTermSpillBytes}), so multi-KB literals
 * cannot blow the bound (BG-125); the size ceiling moves from RAM to free
 * disk in the workspace (roughly a few times the source size, transiently).
 * The one exception is a JSON-LD source, which Jena parses whole in memory
 * (see {@link com.ebremer.beakgraph.utils.RdfSources#isStreaming}).
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
        // Fail before any parsing or sorting if the HDF5 backend is missing (BG-441).
        StreamingHdf5.requireBackend();
        Path dest = builder.getDestination().toPath();
        Path workspace = Files.createTempDirectory(builder.workspaceBase(dest), ".bghuge-");
        // -merge: every source in the list is parsed into this one store
        // (blank nodes stay distinct per document); otherwise the single src.
        List<File> inputs = builder.getSources().isEmpty()
                ? List.of(builder.getSource())
                : builder.getSources();
        try {
            // AtomicPublish.build: a unique sibling temp file, published only
            // on success, removed on ANY failure - an OutOfMemoryError included,
            // the realistic failure of this engine - with dest untouched; the
            // one publish discipline for every engine (BG-119, BG-140, BG-314).
            AtomicPublish.build(dest, tmp -> {
                // Prove the installed backend (native, or a replaced provider) can
                // write a file here before any work is done (BG-135).
                StreamingHdf5.requireWritable(workspace);
                StreamingHdf5.probeFile(tmp);   // the output path itself (BG-419)
                try (HugeBuildPipeline pipeline = new HugeBuildPipeline(
                        inputs, builder.getSpatial(), builder.getFeatures(), builder.getVoidMode(),
                        workspace, builder.getTermSpillBatch(), builder.getIdSpillBatch(), builder.getMergeFanIn(),
                        builder.getTermSpillBytes())) {
                    pipeline.setSourceRoot(builder.getSourceRoot());
                    pipeline.setVoidDatasetIri(builder.getVoidDatasetIri());
                    pipeline.run(tmp);
                }
            });
        } finally {
            Workspaces.deleteTree(workspace, logger);
        }
        logger.info("Write complete: {}", builder.getDestination());
    }

    /**
     * Sequential-engine defaults ({@link AbstractDiskWriterBuilder}):
     * {@code 1 << 18} term and {@code 1 << 21} id records per run, fan-in 64,
     * term byte budget heap / 8 - one batch live per sorter, merges one at a
     * time.
     */
    public static class Builder extends AbstractDiskWriterBuilder<Builder> {

        public Builder() {
            super(1 << 18, 1 << 21, 64, SorterProvider.defaultTermSpillBytes(1));
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
            requireSourceAndDestination();
            return new HugeHDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
