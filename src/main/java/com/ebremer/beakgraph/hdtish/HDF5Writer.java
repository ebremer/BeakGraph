package com.ebremer.beakgraph.hdtish;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;

/**
 *
 * @author Erich Bremer
 */
public class HDF5Writer {
    private File src = null;
    private File dest = null;
    private final BitPackedWriter bitmapX;
    private final BitPackedWriter bitmapY;
    private final BitPackedWriter bitmapZ;
    private final BitPackedWriter arrayX;
    private final BitPackedWriter arrayY;
    private final BitPackedWriter arrayZ;
        
    private HDF5Writer(Builder builder) throws IOException {
        src = builder.getSource();
        dest = builder.getDestination();
        FiveSectionDictionaryWriter.Builder db = new FiveSectionDictionaryWriter.Builder();
        FiveSectionDictionaryWriter w = db
            .setSource(src)
            .setDestination(dest)
            .build();
        bitmapX = BitPackedWriter.forBuffer( Path.of( "BitmapX" ), 1 );
        bitmapY = BitPackedWriter.forBuffer( Path.of( "BitmapY" ), 1 );
        bitmapZ = BitPackedWriter.forBuffer( Path.of( "BitmapZ" ), 1 );
        arrayX = BitPackedWriter.forBuffer( Path.of( "ArrayX" ), 1 );
        arrayY = BitPackedWriter.forBuffer( Path.of( "ArrayY" ), 1 );
        arrayZ = BitPackedWriter.forBuffer( Path.of( "ArrayZ" ), 1 );        
        final Current c = new Current();
        try (GZIPInputStream fis = new GZIPInputStream(new FileInputStream(src))) {
            AsyncParserBuilder xbuilder = AsyncParser.of(fis, Lang.NQUADS, null);
            xbuilder.mutateSources(rdfBuilder->
                rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven())
            );
            xbuilder
                .streamQuads()
                //.limit(10)
                .forEach(quad->{
                    //Node g = quad.getGraph();
                    Node s = quad.getSubject();
                    Node p = quad.getPredicate();
                    Node o = quad.getObject();
                    if (!s.equals(c.cs)) {
                        c.cs = s;
                        try {
                            bitmapX.writeInteger(1);
                            arrayX.writeInteger(w.locateSubject(quad.getSubject()));
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        try {
                            bitmapX.writeInteger(0);
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if (!p.equals(c.cp)) {
                        c.cp = p;
                        try {
                            bitmapY.writeInteger(1);
                            arrayY.writeInteger(w.locatePredicate(quad.getPredicate()));
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        try {
                            bitmapY.writeInteger(0);
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if (!o.equals(c.co)) {
                        c.co = o;                    
                        try {
                            bitmapZ.writeInteger(1);
                            arrayZ.writeInteger(w.locateObject(quad.getObject()));
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        try {
                            bitmapZ.writeInteger(0);
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                });
        }        
        try (WritableHdfFile hdfFile = HdfFile.write(builder.getDestination().toPath())) {
            WritableGroup ultra = hdfFile.putGroup(builder.getName());           
            WritableGroup group = null;
            Path curr = Path.of("dummy");
            for (HDF5Buffer b : w.getBuffers()) {
                if (!curr.equals(b.getName().getParent())) {
                    curr = b.getName().getParent();
                    System.out.println("Creating new HDF5 group : "+b.getName().getParent().toString());
                    group = ultra.putGroup(b.getName().getParent().toString());
                    group.putAttribute("metadata", "this is really cool --> "+b.getName().getParent().toString());
                }
                if (b.getBuffer().length>0) {
                    System.out.println("Adding : "+b.getName().toFile().getName()+" ----> "+b.getBuffer().length);                    
                    WritableDataset ds = group.putDataset(b.getName().toFile().getName(), b.getBuffer());
                    b.getProperties().forEach((k,v)->{
                        ds.putAttribute(k, v);
                    });
                }               
            }
            if (bitmapX.getBuffer().length>0) {
                ultra.putDataset(bitmapX.getName().toString(), bitmapX.getBuffer());
            }
            if (bitmapY.getBuffer().length>0) {
                ultra.putDataset(bitmapY.getName().toString(), bitmapY.getBuffer());
            }
            if (bitmapZ.getBuffer().length>0) {
                ultra.putDataset(bitmapZ.getName().toString(), bitmapZ.getBuffer());
            }
            if (arrayX.getBuffer().length>0) {
                ultra.putDataset(arrayX.getName().toString(), arrayX.getBuffer());
            }
            if (arrayY.getBuffer().length>0) {
                ultra.putDataset(arrayY.getName().toString(), arrayY.getBuffer());
            }
            if (arrayZ.getBuffer().length>0) {
                ultra.putDataset(arrayZ.getName().toString(), arrayZ.getBuffer());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        w.close();
    }
    
    public static class Builder {
        private File src;
        private File dest;
        
        public String getName() {
            return "HDT";
        }
        
        public File getSource() {
            return src;
        }
        
        public File getDestination() {
            return dest;
        }

        public Builder setSource(File file) {
            src = file;
            return this;
        }
        
        public Builder setDestination(File file) {
            dest = file;
            return this;
        }
        
        public HDF5Writer build() throws IOException {
            return new HDF5Writer(this);
        }
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        File file = new File("/data/sorted.nq.gz");
        File dest = new File("/data/data.hdf5");
        
        Builder builder = new HDF5Writer.Builder();
        builder
            .setSource(file)
            .setDestination(dest)
            .build();
    }
    
}
