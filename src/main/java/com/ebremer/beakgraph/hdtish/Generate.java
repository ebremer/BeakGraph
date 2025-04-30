package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IO;
import java.io.IOException;
import java.util.ArrayList;
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
public class Generate {
    
    private File file;
    private PlainDictionary dict;
    
    class Currents {
        Node cg = Node.ANY;
        Node cs = Node.ANY;
        Node cp = Node.ANY;
        Node co = Node.ANY;   
    }
    
    public Generate(File file) throws FileNotFoundException {
        this.dict = new PlainDictionary();
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
                        try {
                            dict.Add(g);
                        } catch (IOException ex) {
                            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if (!s.equals(c.cs)) {
                        c.cs = s;
                        try {
                            dict.Add(s);
                        } catch (IOException ex) {
                            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if (!p.equals(c.cp)) {
                        c.cp = p;
                        try {
                            dict.Add(p);
                        } catch (IOException ex) {
                            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    if (!o.equals(c.co)) {
                        c.co = o;
                        try {
                            dict.Add(o);
                        } catch (IOException ex) {
                            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }                 
                });
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        IO.println("Dictionary : "+dict);
        IO.println("quads : "+cc.get());
        IO.println("diff nodes : "+dict.getNodes().size());
        ArrayList<Node> list = NodeSorter.sortNodes(dict.getNodes());
        list.forEach(n->System.out.println(n));
    }
    
    public static void main(String[] args) throws FileNotFoundException {
        File file = new File("/data/sorted.nq.gz");
        Generate gen = new Generate(file);
        gen.build();
    }
    
}
