package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.Index;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default (method 0) writer: parse, dictionary, GSPO/GPOS indexes, one
 * jHDF file. The writer SNAPSHOTS its builder's settings at construction:
 * the builder stays mutable (it is reused freely by the CLI and by tests),
 * and a writer that read the builder on every {@code write()} call could be
 * redirected after the fact (BG-102).
 *
 * @author Erich Bremer
 */
public class HDF5Writer implements BeakGraphWriter {
    private static final Logger logger = LoggerFactory.getLogger(HDF5Writer.class);
    private final File src;
    private final List<File> sources;
    private final File sourceRoot;
    private final File dest;
    private final VoidMode voidMode;
    private final String voidDatasetIri;
    private final boolean spatial;
    private final boolean features;
    private final String name;

    private HDF5Writer(Builder builder) {
        this.src = builder.getSource();
        this.sources = List.copyOf(builder.getSources());
        this.sourceRoot = builder.getSourceRoot();
        this.dest = builder.getDestination();
        this.voidMode = builder.getVoidMode();
        this.voidDatasetIri = builder.getVoidDatasetIri();
        this.spatial = builder.getSpatial();
        this.features = builder.getFeatures();
        this.name = builder.getName();
    }

    @Override
    public void write() throws IOException {
        logger.info("Writing BeakGraph to {}", dest);
        // Build into a unique sibling temp file and swap it in only on success:
        // a failed rebuild must never destroy a previous good artifact at dest,
        // readers never observe a half-written file at the published path, and
        // every failure - Error included - removes the temp file (AtomicPublish).
        AtomicPublish.build(dest.toPath(), tmp -> {
            PositionalDictionaryWriterBuilder db = new PositionalDictionaryWriterBuilder();
            db.setSourceRoot(sourceRoot);
            try (PositionalDictionaryWriter w = db
                    .setSource(src)
                    .setSources(sources)
                    .setVoidMode(voidMode)
                    .setVoidDatasetIri(voidDatasetIri)
                    .setDestination(dest)
                    .setName(Params.DICTIONARY)
                    .setSpatial(spatial)
                    .setFeatures(features)
                    .build()) {
                Quad[] allQuads = w.getQuads();
                // Four dictionary lookups per quad, once; both indexes sort and
                // scan the resulting id tuples (BG-241). Clone before the first
                // build sorts its array in place.
                BGIndex.QuadIds[] ids = BGIndex.resolveIds(w, allQuads);
                BGIndex gspo = new BGIndex(w, Index.GSPO, ids.clone());
                BGIndex gpos = new BGIndex(w, Index.GPOS, ids);

                logger.info("Creating HDF5 file {}", dest);
                try (WritableHdfFile hdfFile = HdfFile.write(tmp)) {
                    final WritableGroup hdt = hdfFile.putGroup(name);
                    hdt.putAttribute(Params.NUM_QUADS, w.getNumberOfQuads());
                    hdt.putAttribute(Params.FORMAT_VERSION_ATTR, Params.FORMAT_VERSION);
                    w.add(hdt);
                    gspo.add(hdt);
                    gpos.add(hdt);
                }
            }
        });
        logger.info("Write complete: {}", dest);
    }

    public static class Builder extends AbstractGraphBuilder<Builder> {

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public String getName() {
            return Params.BG;
        }

        @Override
        public HDF5Writer build() {
            requireSourceAndDestination();
            return new HDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
