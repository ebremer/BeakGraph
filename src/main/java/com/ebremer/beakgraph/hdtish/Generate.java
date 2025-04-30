package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IO;
import java.io.IOException;
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
    private PlainDictionary dict = new PlainDictionary();
    
    class Currents {
        Node cg = Node.ANY;
        Node cs = Node.ANY;
        Node cp = Node.ANY;
        Node co = Node.ANY;   
    }
    
    public Generate(File file) {
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
                        dict.Add(g);
                    }
                    if (!s.equals(c.cs)) {
                        c.cs = s;
                        dict.Add(s);
                    }
                    if (!p.equals(c.cp)) {
                        c.cp = p;
                        dict.Add(p);
                    }
                    if (!o.equals(c.co)) {
                        c.co = o;
                        dict.Add(o);
                    }                 
                });
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        IO.println("Dictionary : "+dict);
        IO.println("quads : "+cc.get());
    }
    
    public static void main(String[] args) {
        File file = new File("/data/sorted.nq.gz");
        Generate gen = new Generate(file);
        gen.build();
    }
    
}
