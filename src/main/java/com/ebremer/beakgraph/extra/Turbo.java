package com.ebremer.beakgraph.extra;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.stream.Stream;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.AsyncParser;

/**
 *
 * @author erich
 */
public class Turbo {
    
    public static void main(String[] args) throws FileNotFoundException {
        String f = "/data2/halcyon/seg/TCGA-3C-AALI-01Z-00-DX1.F6E9A5DF-D8FB-45CF-B4BD-C6B76294C291.ttl.gz";
        File raw = new File(f);
       // System.out.println(raw.toString());
        //Model m = ModelFactory.createDefaultModel();
       // Model m = RDFDataMgr.loadModel("/turbo/TCGA-AR-A1AI-01Z-00-DX1.5EF2A589-4284-45CF-BF0C-169E3A85530C.ttl.gz", Lang.TURTLE);
       // System.out.println("Triples : "+m.size());
        
        //IteratorCloseable<Triple> iter = AsyncParser.asyncParseTriples("/turbo/TCGA-AR-A1AI-01Z-00-DX1.5EF2A589-4284-45CF-BF0C-169E3A85530C.ttl.gz");
        
        //iter.forEachRemaining(triple->{
          //  System.out.println(triple);
        //});
        LinkedList<Triple> list = new LinkedList<>();
        /*
        try (Stream<Triple> stream = AsyncParser.of(f)
            .setQueueSize(2)
            .setChunkSize(100)
            .streamTriples()) {
                stream.limit(10).forEach(triple->{
                    //list.add(triple);
                    System.out.println(triple);
                });       
            }
*/
        //System.out.println(list.size());
    }
    
}
