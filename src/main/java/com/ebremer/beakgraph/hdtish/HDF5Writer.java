package com.ebremer.beakgraph.hdtish;

import static com.ebremer.beakgraph.hdtish.UTIL.MinBits;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final BitPackedWriter Bs;
    private final BitPackedWriter Bp;
    private final BitPackedWriter Bo;
    private final BitPackedWriter Ss;
    private final BitPackedWriter Sp;
    private final BitPackedWriter So;
    private final BitPackedWriter SBs;
    private final BitPackedWriter SBp;
    private final BitPackedWriter SBo;
    private final BitPackedWriter BBs;
    private final BitPackedWriter BBp;
    private final BitPackedWriter BBo;
    public static final int BLOCKSIZE = 64;
    public static final int SUPERBLOCKSIZE = 512;
        
    private HDF5Writer(Builder builder) throws IOException {
        src = builder.getSource();
        dest = builder.getDestination();
        FiveSectionDictionaryWriter.Builder db = new FiveSectionDictionaryWriter.Builder();
        FiveSectionDictionaryWriter w = db
            .setSource(src)
            .setDestination(dest)
            .build();
        System.out.println("# of Quads      -> "+w.getNumberOfQuads());
        System.out.println("# of Graphs     -> "+w.getNumberOfGraphs());
        System.out.println("# of Subjects   -> "+w.getNumberOfSubjects());
        System.out.println("# of Predicates -> "+w.getNumberOfPredicates());
        System.out.println("# of Objects    -> "+w.getNumberOfObjects());
     
        Bs = BitPackedWriter.forBuffer( Path.of( "Bs" ), 1 );
        Bp = BitPackedWriter.forBuffer( Path.of( "Bp" ), 1 );
        Bo = BitPackedWriter.forBuffer( Path.of( "Bo" ), 1 );
        Ss = BitPackedWriter.forBuffer( Path.of( "Ss" ), MinBits(w.getNumberOfSubjects()) );
        Sp = BitPackedWriter.forBuffer( Path.of( "Sp" ), MinBits(w.getNumberOfPredicates()) );
        So = BitPackedWriter.forBuffer( Path.of( "So" ), MinBits(w.getNumberOfObjects()) );        
        SBs = BitPackedWriter.forBuffer( Path.of( "SBs" ), MinBits(w.getNumberOfSubjects()) );
        SBp = BitPackedWriter.forBuffer( Path.of( "SBp" ), MinBits(w.getNumberOfPredicates()) );
        SBo = BitPackedWriter.forBuffer( Path.of( "SBo" ), MinBits(w.getNumberOfObjects()) );
        BBs = BitPackedWriter.forBuffer( Path.of( "BBs" ), MinBits(w.getNumberOfSubjects()) );
        BBp = BitPackedWriter.forBuffer( Path.of( "BBp" ), MinBits(w.getNumberOfPredicates()) );
        BBo = BitPackedWriter.forBuffer( Path.of( "BBo" ), MinBits(w.getNumberOfObjects()) );
        final Current c = new Current();
        final AtomicInteger totaltriples = new AtomicInteger();
        final AtomicInteger totalsubjects = new AtomicInteger();
        final AtomicInteger totalpredicates = new AtomicInteger();
        final AtomicInteger totalobjects = new AtomicInteger();
        final AtomicInteger deltatriples = new AtomicInteger();
        final AtomicInteger deltasubjects = new AtomicInteger();
        final AtomicInteger deltapredicates = new AtomicInteger();
        final AtomicInteger deltaobjects = new AtomicInteger();
        try (GZIPInputStream fis = new GZIPInputStream(new FileInputStream(src))) {
            AsyncParserBuilder xbuilder = AsyncParser.of(fis, Lang.NQUADS, null);
            xbuilder.mutateSources(rdfBuilder->
                rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven())
            );
            xbuilder
                .streamQuads()
                //.limit(10)
                .forEach(quad->{
                    totaltriples.incrementAndGet();
                    //Node g = quad.getGraph();
                    Node s = quad.getSubject();
                    Node p = quad.getPredicate();
                    Node o = quad.getObject();
                    if (!s.equals(c.cs)) {
                        c.cs = s;
                        try {
                            totalsubjects.incrementAndGet();
                            Bs.writeInteger(1);
                            Ss.writeInteger(w.locateSubject(quad.getSubject()));
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        try {
                            Bs.writeInteger(0);
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if (!p.equals(c.cp)) {
                        c.cp = p;
                        try {
                            totalpredicates.incrementAndGet();
                            Bp.writeInteger(1);
                            Sp.writeInteger(w.locatePredicate(quad.getPredicate()));
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        try {
                            Bp.writeInteger(0);
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if (!o.equals(c.co)) {
                        c.co = o;                    
                        try {
                            totalobjects.incrementAndGet();
                            Bo.writeInteger(1);
                            So.writeInteger(w.locateObject(quad.getObject()));
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        try {
                            Bo.writeInteger(0);
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if ((totaltriples.get() % BLOCKSIZE)==0) {
                        try {
                            BBs.writeInteger(deltasubjects.get());
                            BBp.writeInteger(deltapredicates.get());
                            BBo.writeInteger(deltaobjects.get());
                        } catch (IOException ex) {
                            Logger.getLogger(HDF5Writer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if ((totaltriples.get() % SUPERBLOCKSIZE)==0) {
                        try {
                            SBs.writeInteger(totalsubjects.get());
                            SBp.writeInteger(totalpredicates.get());
                            SBo.writeInteger(totalobjects.get());
                            deltatriples.set(0);
                            deltasubjects.set(0);
                            deltapredicates.set(0);
                            deltaobjects.set(0);
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
            if (Bs.getBuffer().length>0) {
                ultra.putDataset(Bs.getName().toString(), Bs.getBuffer());
            }
            if (Bp.getBuffer().length>0) {
                ultra.putDataset(Bp.getName().toString(), Bp.getBuffer());
            }
            if (Bo.getBuffer().length>0) {
                ultra.putDataset(Bo.getName().toString(), Bo.getBuffer());
            }
            if (Ss.getBuffer().length>0) {
                ultra.putDataset(Ss.getName().toString(), Ss.getBuffer());
            }
            if (Sp.getBuffer().length>0) {
                ultra.putDataset(Sp.getName().toString(), Sp.getBuffer());
            }
            if (So.getBuffer().length>0) {
                ultra.putDataset(So.getName().toString(), So.getBuffer());
            }
            if (SBs.getBuffer().length>0) {
                ultra.putDataset(SBs.getName().toString(), SBs.getBuffer());
            }            
            if (SBp.getBuffer().length>0) {
                ultra.putDataset(SBp.getName().toString(), SBp.getBuffer());
            } 
            if (SBo.getBuffer().length>0) {
                ultra.putDataset(SBo.getName().toString(), SBo.getBuffer());
            }
            if (BBs.getBuffer().length>0) {
                ultra.putDataset(BBs.getName().toString(), BBs.getBuffer());
            }            
            if (BBp.getBuffer().length>0) {
                ultra.putDataset(BBp.getName().toString(), BBp.getBuffer());
            } 
            if (BBo.getBuffer().length>0) {
                ultra.putDataset(BBo.getName().toString(), BBo.getBuffer());
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
