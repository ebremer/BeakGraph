package com.ebremer.beakgraph.hdtish;

import com.ebremer.beakgraph.hdtish.DictionaryWriter.Builder;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IO;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
public class PreSort {
    
    private File file;
    private DictionaryWriter dict;
    private HashSet<Node> shared = new HashSet<>();
    private LinkedHashSet<Node> graphs = new LinkedHashSet<>();
    private LinkedHashSet<Node> subjects = new LinkedHashSet<>();
    private HashSet<Node> predicates = new HashSet<>();
    private LinkedHashSet<Node> objects = new LinkedHashSet<>();
    
    class Currents {
        Node cg = Node.ANY;
        Node cs = Node.ANY;
        Node cp = Node.ANY;
        Node co = Node.ANY;   
    }
    
    public PreSort(File file) throws FileNotFoundException, IOException {
        this.dict = null;
        this.file = file;
    }
    
    public void build() {
        final Currents c = new Currents();
        final AtomicLong cc = new AtomicLong();        
        try (GZIPInputStream fis = new GZIPInputStream(new FileInputStream(file))) {
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
            Logger.getLogger(PreSort.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PreSort.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        IO.println("Dictionary : "+dict);
        IO.println("quads : "+cc.get());
     //   IO.println("diff nodes : "+dict.getNodes().size());
       // ArrayList<Node> list = NodeSorter.sortNodes(dict.getNodes());
        //list.forEach(n->System.out.println(n));
    }
    
    public static void main(String[] args) throws FileNotFoundException {
        File file = new File("/data/sorted.nq.gz");
        Builder builder = new DictionaryWriter.Builder();
        try (
            GZIPInputStream fis = new GZIPInputStream( new BufferedInputStream( new FileInputStream(file),  32768) );
        ) {
            DictionaryWriter w = builder.Add(AsyncParser.of(fis, Lang.NQUADS, null).streamQuads()).build();
            w.close();
        } catch (IOException ex) {
            Logger.getLogger(PreSort.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(PreSort.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
}
