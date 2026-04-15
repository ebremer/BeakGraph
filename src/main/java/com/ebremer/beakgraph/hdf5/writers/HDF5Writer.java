package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.Index;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
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
        IO.println("Writing...");
        PositionalDictionaryWriterBuilder db = new PositionalDictionaryWriterBuilder();
        try (PositionalDictionaryWriter w = db
                .setSource(builder.getSource())
                .setDestination(builder.getDestination())
                .setName("dictionary")
                .setSpatial(builder.getSpatial())
                .setFeatures(builder.getFeatures())
                .build()) {
            Quad[] allQuads = w.getQuads();
            BGIndex gspo = new BGIndex(builder, w, Index.GSPO, allQuads);
            BGIndex gpos = new BGIndex(builder, w, Index.GPOS, allQuads);
            //BGIndex gosp = new BGIndex(builder, w, Index.GOSP, allQuads);

            IO.print("Creating HDF5 File..." + builder.getDestination() + "...");
            try (WritableHdfFile hdfFile = HdfFile.write(builder.getDestination().toPath())) {
                final WritableGroup hdt = hdfFile.putGroup(builder.getName());
                hdt.putAttribute("numQuads", w.getNumberOfQuads());
                w.Add(hdt);
                gspo.Add(hdt);
                gpos.Add(hdt);
                //gosp.Add(hdt);
            }
        }
        IO.println("Done.");
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

        public Builder setCompressionLevel(int level) {
            return this;
        }
    }

    public static Builder Builder() {
        return new Builder();
    }
}
