package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.AsyncParser;

/**
 *
 * @author Erich Bremer
 */
public class FiveSectionDictionaryWriter implements AutoCloseable {

    public FiveSectionDictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        DictionaryWriter.Builder shared = new DictionaryWriter.Builder();        
        DictionaryWriter.Builder subjects = new DictionaryWriter.Builder();
        DictionaryWriter.Builder predicates = new DictionaryWriter.Builder();
        DictionaryWriter.Builder objects = new DictionaryWriter.Builder();
        DictionaryWriter.Builder graphs = new DictionaryWriter.Builder();        
        DictionaryWriter shareddict = shared  
            .setName("shared")
            .setNodes(builder.getShared())            
            .build();
        DictionaryWriter subjectsdict = subjects
            .setName("subjects")
            .setNodes(builder.getSubjects())
            .build();
        DictionaryWriter predicatesdict = predicates
            .setName("predicates")
            .setNodes(builder.getPredicates())
            .build();
        DictionaryWriter objectsdict = objects
            .setName("objects")
            .setNodes(builder.getObjects())
            .build();
        DictionaryWriter graphsdict = graphs
            .setName("graphs")
            .setNodes(builder.getGraphs())
            .build();
        
    }

    //    IO.println("Dictionary : "+dict);
      //  IO.println("quads : "+cc.get());
     //   IO.println("diff nodes : "+dict.getNodes().size());
       // ArrayList<Node> list = NodeSorter.sortNodes(dict.getNodes());
        //list.forEach(n->System.out.println(n));

    @Override
    public void close() {
        
    }

    public static class Builder {
        class Currents {
            Node cg = Node.ANY;
            Node cs = Node.ANY;
            Node cp = Node.ANY;
            Node co = Node.ANY;   
        }
        
        private File src;
        private File dest;
        private HashSet<Node> shared = new HashSet<>();
        private LinkedHashSet<Node> graphs = new LinkedHashSet<>();
        private LinkedHashSet<Node> subjects = new LinkedHashSet<>();
        private HashSet<Node> predicates = new HashSet<>();
        private LinkedHashSet<Node> objects = new LinkedHashSet<>();
        
        public File getDestination() {
            return dest;
        }
        
        public Set<Node> getShared() {
            return shared;
        }
       
        public Set<Node> getSubjects() {
            return subjects;
        }
        
        public Set<Node> getPredicates() {
            return predicates;
        }
        
        public Set<Node> getObjects() {
            return objects;
        }
        
        public Set<Node> getGraphs() {
            return graphs;
        }
        
        public Builder setSource(File src) {
            this.src = src;
            return this;
        }

        public Builder setDestination(File dest) {
            this.dest = dest;
            return this;
        }        
        
        public FiveSectionDictionaryWriter build() throws IOException {
            final Currents c = new Currents();
            final AtomicLong cc = new AtomicLong();        
            System.out.print("Create sections...");
            try (GZIPInputStream fis = new GZIPInputStream(new FileInputStream(src))) {
                AsyncParser.of(fis, Lang.NQUADS, null)
                    .streamQuads()
                   // .limit(100)
                    .forEach(quad->{
                        cc.incrementAndGet();
                        Node g = quad.getGraph();
                        Node s = quad.getSubject();
                        Node p = quad.getPredicate();
                        Node o = quad.getObject();
                        if (!g.equals(c.cg)) {
                            c.cg = g;
                            if (graphs.contains(g)) {
                                graphs.remove(g);
                                shared.add(g);
                            } else {
                                graphs.add(g);
                            }
                        }
                        if (!s.equals(c.cs)) {
                            c.cs = s;
                            if (subjects.contains(s)) {
                                subjects.remove(s);
                                shared.add(s);
                            } else {
                                subjects.add(s);
                            }
                        }
                        if (!p.equals(c.cp)) {
                            c.cp = p;
                            predicates.add(p);
                        }
                        if (!o.equals(c.co)) {
                            c.co = o;
                            if (objects.contains(o)) {
                                objects.remove(o);
                                shared.add(o);
                            } else {
                                objects.add(o);
                            }
                        }                 
                    });
            } catch (FileNotFoundException ex) {
                Logger.getLogger(FiveSectionDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(FiveSectionDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Done.");
            return new FiveSectionDictionaryWriter(this);
        }
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        File file = new File("/data/sorted.nq.gz");
        File dest = new File("/data/HDT");
        Builder builder = new FiveSectionDictionaryWriter.Builder();
        FiveSectionDictionaryWriter w = builder
            .setSource(file)
            .setDestination(dest)
            .build();
        w.close();
    }
}
