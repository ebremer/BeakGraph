package com.ebremer.beakgraph.hdf5.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;

/**
 *
 * @author Erich Bremer
 */
public class SorterA {
    private ArrayList<Quad> quads;
    
    public SorterA(File file) {
        quads = new ArrayList<>();
        final AtomicInteger c = new AtomicInteger();
        try (GZIPInputStream fis = new GZIPInputStream(new FileInputStream(file))) {
            AsyncParserBuilder xbuilder = AsyncParser.of(fis, Lang.NQUADS, null);
            xbuilder.mutateSources(rdfBuilder->
                rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven())
            );
            xbuilder
                .streamQuads()
                .forEach(q->{
                    c.incrementAndGet();
                    quads.add(q);
                });
        } catch (FileNotFoundException ex) {
            System.getLogger(SorterA.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } catch (IOException ex) {
            System.getLogger(SorterA.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
        IO.println("triples : "+c.get());
        Quad[] quadArray = quads.toArray(new Quad[0]);
        Arrays.parallelSort(quadArray, new QuadComparator());
    }
    
    public static void main(String[] args) {
       
       File file = new File("/data/sorted.nq.gz");
       SorterA s = new SorterA(file); 
    }
    
}
