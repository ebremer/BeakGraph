package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.Index;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.Quad;

/**
 *
 * @author Erich Bremer
 */
public class HDF5Writer implements BeakGraphWriter {
    private final Builder builder;
    
    private HDF5Writer(Builder builder) {
        this.builder = builder;
    }

    @Override
    public void write() throws IOException {
        FiveSectionDictionaryWriter.Builder db = new FiveSectionDictionaryWriter.Builder();
        FiveSectionDictionaryWriter w = db
            .setSource(builder.getSource())
            .setDestination(builder.getDestination())
            .setName("dictionary")
            .setSpatial(builder.getSpatial())
            .build();
        
        Quad[] allQuads = w.getQuads();
        BGIndex gspo = new BGIndex(builder, w, Index.GSPO, allQuads);
        BGIndex gpos = new BGIndex(builder, w, Index.GPOS, allQuads);
        BGIndex gosp = new BGIndex(builder, w, Index.GOSP, allQuads);
        
        IO.print("Creating HDF5 File..."+builder.getDestination()+"...");
        try (WritableHdfFile hdfFile = HdfFile.write(builder.getDestination().toPath())) {
            final WritableGroup hdt = hdfFile.putGroup(builder.getName());            
            hdt.putAttribute("numQuads", w.getNumberOfQuads() );
            w.Add(hdt);
            gspo.Add(hdt);
            gpos.Add(hdt);
            gosp.Add(hdt);            
        } catch (Exception e) {
            e.printStackTrace();
            
        } catch (Throwable ex) {
            IO.println(ex.getMessage());
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

    public static ArrayList<Quad> convertDatasetToQuadList(Dataset dataset) {
        ArrayList<Quad> quads = new ArrayList<>();
        Iterator<Quad> iter = dataset.asDatasetGraph().find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);        
        while (iter.hasNext()) {
            quads.add(iter.next());
        }
        return quads;
    }
}
