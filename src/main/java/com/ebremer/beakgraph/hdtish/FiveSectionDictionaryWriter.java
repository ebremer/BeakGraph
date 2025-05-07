package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
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
public class FiveSectionDictionaryWriter implements Dictionary, AutoCloseable {
    private DictionaryWriter shareddict;
    private DictionaryWriter subjectsdict;
    private DictionaryWriter predicatesdict;
    private DictionaryWriter objectsdict;
    private DictionaryWriter graphsdict;          

    public FiveSectionDictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        DictionaryWriter.Builder shared = new DictionaryWriter.Builder();        
        DictionaryWriter.Builder subjects = new DictionaryWriter.Builder();
        DictionaryWriter.Builder predicates = new DictionaryWriter.Builder();
        DictionaryWriter.Builder objects = new DictionaryWriter.Builder();
        DictionaryWriter.Builder graphs = new DictionaryWriter.Builder();
        shareddict = shared  
            .setName("shared")
            .setNodes(builder.getShared())            
            .build();
        subjectsdict = subjects
            .setName("subjects")
            .setNodes(builder.getSubjects())
            .build();
        predicatesdict = predicates
            .setName("predicates")
            .setNodes(builder.getPredicates())
            .build();
        objectsdict = objects
            .setName("objects")
            .setNodes(builder.getObjects())
            .build();
        graphsdict = graphs
            .setName("graphs")
            .setNodes(builder.getGraphs())
            .build();

        if (!builder.getDestination().getParentFile().exists()) {
            builder.getDestination().getParentFile().mkdirs();
        }
    }
    
    @Override
    public int locateGraph(Node element) {
        int c = shareddict.locateGraph(element);
        if (c > 0) {
            return c;
        } else {
            c = graphsdict.locateGraph(element);
            if (c > 0) {
                return c;
            }
        }
        throw new Error("Cannot resolve Graph : "+element);
    }

    @Override
    public int locateSubject(Node element) {
        int c = shareddict.locateGraph(element);
        if (c > 0) {
            return c;
        } else {
            c = subjectsdict.locateGraph(element);
            if (c > 0) {
                return c;
            }
        }
        throw new Error("Cannot resolve Subject : "+element);
    }
    
    @Override
    public int locatePredicate(Node element) {
        int c = predicatesdict.locateGraph(element);
        if (c > 0) {
            return c;
        }
        throw new Error("Cannot resolve Predicate : "+element);
    }

    @Override
    public int locateObject(Node element) {
        int c;
        if (element.isLiteral()) {
            c = objectsdict.locateGraph(element);
            if (c > 0) {
                return c;
            }       
        }
        c = shareddict.locateGraph(element);
        if (c > 0) {
            return c;
        } else {
            c = objectsdict.locateGraph(element);
            if (c > 0) {
                return c;
            }
        }
        throw new Error("Cannot resolve : "+element);
    }

    @Override
    public Object extractGraph(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractSubject(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractPredicate(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
    @Override
    public Object extractObject(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public List<HDF5Buffer> getBuffers() {
        List<HDF5Buffer> list = new ArrayList<>();
        list.addAll(shareddict.getBuffers());
        list.addAll(subjectsdict.getBuffers());
        list.addAll(predicatesdict.getBuffers());
        list.addAll(objectsdict.getBuffers());
        list.addAll(graphsdict.getBuffers());
        return list;
    }

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
                            if (subjects.contains(g)||objects.contains(g)) {
                                graphs.remove(g);
                                subjects.remove(g);
                                objects.remove(g);
                                shared.add(g);
                            } else {
                                graphs.add(g);
                            }
                        }
                        if (!s.equals(c.cs)) {
                            c.cs = s;
                            if (graphs.contains(s)||objects.contains(s)) {
                                graphs.remove(s);
                                subjects.remove(s);
                                objects.remove(s);
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
                            if (graphs.contains(o)||subjects.contains(o)) {
                                graphs.remove(o);
                                subjects.remove(o);
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
}
