package com.ebremer.beakgraph.hdf5.writers;

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
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Erich Bremer
 */
public class HDF5Writer implements BeakGraphWriter {
    private static final Logger logger = LoggerFactory.getLogger(HDF5Writer.class);
    private final Builder builder;
    
    private HDF5Writer(Builder builder) {
        this.builder = builder;
    }

    @Override
    public void write() throws IOException {
        logger.info("Writing BeakGraph to {}", builder.getDestination());
        Path dest = builder.getDestination().toPath();
        // Build into a sibling temp file and swap it in only on success: a failed
        // rebuild must never destroy a previous good artifact at dest (the old
        // cleanup deleted dest even when the failure - a parse error, say -
        // happened before a single byte was written), and readers never observe
        // a half-written file at the published path.
        Path tmp = dest.resolveSibling(dest.getFileName() + ".tmp");
        try {
            PositionalDictionaryWriterBuilder db = new PositionalDictionaryWriterBuilder();
            try (PositionalDictionaryWriter w = db
                    .setSource(builder.getSource())
                    .setSources(builder.getSources())
                    .setVoidMode(builder.getVoidMode())
                    .setDestination(builder.getDestination())
                    .setName(Params.DICTIONARY)
                    .setSpatial(builder.getSpatial())
                    .setFeatures(builder.getFeatures())
                    .build()) {
                Quad[] allQuads = w.getQuads();
                BGIndex gspo = new BGIndex(builder, w, Index.GSPO, allQuads);
                BGIndex gpos = new BGIndex(builder, w, Index.GPOS, allQuads);

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
        }
        logger.info("Write complete: {}", builder.getDestination());
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
            return new HDF5Writer(this);
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
