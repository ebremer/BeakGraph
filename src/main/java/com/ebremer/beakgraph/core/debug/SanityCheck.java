package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriter;
import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder;
import java.io.File;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class SanityCheck {
    public static void main(String[] args) {
        File file = new File("/data/sorted.nq.gz");
        File dest = new File("/data/dX_sanity.h5"); 

        System.out.println("--- Running Advanced Sanity Check ---");
        
        try {
            PositionalDictionaryWriterBuilder db = new PositionalDictionaryWriterBuilder();
            PositionalDictionaryWriter w = db
                .setSource(file)
                .setDestination(dest)
                .setName("dictionary")
                .build();
                
            Quad[] allQuads = w.getQuads();
            System.out.println("Total Quads: " + allQuads.length);
            
            int errors = 0;
            for (int i = 0; i < allQuads.length; i++) {
                Quad q = allQuads[i];
                Node p = q.getPredicate();
                Node o = q.getObject();
                
                if (p.isURI() && p.getURI().endsWith("type")) {
                    if (o.isBlank()) {
                        System.out.println("ERROR (BNode): " + q);
                        errors++;
                    }
                    if (o.isLiteral()) {
                        System.out.println("ERROR (Literal): " + q);
                        errors++;
                    }
                }
                
                if (errors >= 10) break;
            }
            
            if (errors == 0) {
                System.out.println("PASS: In-memory Quads are semantically correct.");
            } else {
                System.out.println("FAIL: Corruption confirmed in memory.");
            }
            
            w.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}