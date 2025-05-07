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
import java.util.zip.GZIPInputStream;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.AsyncParser;

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
        bitmapX = BitPackedWriter.forBuffer( Path.of( builder.getName(), "BitmapX"), 1 );
        bitmapY = BitPackedWriter.forBuffer( Path.of( builder.getName(), "BitmapY"), 1 );
        bitmapZ = BitPackedWriter.forBuffer( Path.of( builder.getName(), "BitmapZ"), 1 );
        arrayX = BitPackedWriter.forBuffer( Path.of( builder.getName(), "ArrayX"), 1 );
        arrayY = BitPackedWriter.forBuffer( Path.of( builder.getName(), "ArrayY"), 1 );
        arrayZ = BitPackedWriter.forBuffer( Path.of( builder.getName(), "ArrayZ"), 1 );        
        try (GZIPInputStream fis = new GZIPInputStream(new FileInputStream(src))) {
            AsyncParser.of(fis, Lang.NQUADS, null)
                .streamQuads()
                .limit(10)
                .forEach(quad->{
                    arrayX.writeInteger();
                });
        }        
        try (WritableHdfFile hdfFile = HdfFile.write(builder.getDestination().toPath())) {
            WritableGroup group = null;
            Path curr = Path.of("dummy");
            for (HDF5Buffer b : w.getBuffers()) {
                if (!curr.equals(b.getName().getParent())) {
                    curr = b.getName().getParent();
                    System.out.println("Creating new HDF5 group : "+b.getName().getParent().toString());
                    group = hdfFile.putGroup(b.getName().getParent().toString());
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
                hdfFile.putDataset(bitmapX.getName().toString(), bitmapX.getBuffer());
            }
            if (bitmapY.getBuffer().length>0) {
                hdfFile.putDataset(bitmapY.getName().toString(), bitmapY.getBuffer());
            }
            if (bitmapZ.getBuffer().length>0) {
                hdfFile.putDataset(bitmapZ.getName().toString(), bitmapZ.getBuffer());
            }
            if (arrayX.getBuffer().length>0) {
                hdfFile.putDataset(arrayX.getName().toString(), arrayX.getBuffer());
            }
            if (arrayY.getBuffer().length>0) {
                hdfFile.putDataset(arrayY.getName().toString(), arrayY.getBuffer());
            }
            if (arrayZ.getBuffer().length>0) {
                hdfFile.putDataset(arrayZ.getName().toString(), arrayZ.getBuffer());
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
