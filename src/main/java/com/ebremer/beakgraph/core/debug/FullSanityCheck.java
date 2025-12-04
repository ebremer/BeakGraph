package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.writers.FiveSectionDictionaryWriter;
import com.ebremer.beakgraph.hdf5.writers.FiveSectionDictionaryWriterBuilder;
import java.io.File;
import java.util.Arrays;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class FullSanityCheck {
    public static void main(String[] args) {
        File file = new File("/data/sorted.nq.gz");
        File dest = new File("/data/dX_sanity.h5");

        try {
            System.out.println("1. Loading Quads...");
            FiveSectionDictionaryWriterBuilder db = new FiveSectionDictionaryWriterBuilder();
            FiveSectionDictionaryWriter w = db
                .setSource(file)
                .setDestination(dest)
                .setName("dictionary")
                .build();
                
            Quad[] allQuads = w.getQuads();
            System.out.println("Loaded " + allQuads.length + " quads.");
            
            // 2. Sort for GPOS
            System.out.println("2. Sorting for GPOS (G->P->O->S)...");
            Arrays.parallelSort(allQuads, Index.GPOS.getComparator());
            
            System.out.println("3. Scanning for Predicate Transition...");
            
            boolean foundClass = false;
            for (int i = 0; i < allQuads.length - 1; i++) {
                Quad prev = allQuads[i];
                Quad curr = allQuads[i+1];
                
                Node pPrev = prev.getPredicate();
                Node pCurr = curr.getPredicate();
                
                // Look for the transition from 'classification' to anything else
                if (pPrev.isURI() && pPrev.getURI().endsWith("classification")) {
                    if (!pCurr.equals(pPrev)) {
                        System.out.println("--- Transition Detected at Index " + i + " ---");
                        System.out.println("Prev: " + prev);
                        System.out.println("Curr: " + curr);
                        
                        System.out.println("P_Prev: " + pPrev);
                        System.out.println("P_Curr: " + pCurr);
                        
                        boolean eq = pPrev.equals(pCurr);
                        System.out.println("Predicate Equals: " + eq);
                        System.out.println("BGIndex Logic Check: (changeL1 should be true)");
                        
                        // Check if it went to hasProbability
                        if (pCurr.isURI() && pCurr.getURI().endsWith("hasProbability")) {
                            System.out.println("Transition is to 'hasProbability'. This matches the corruption signature.");
                        } else {
                            System.out.println("Transition is to something else: " + pCurr);
                        }
                        foundClass = true;
                        break; // Found the critical spot, stop.
                    }
                }
            }
            
            if (!foundClass) {
                System.out.println("WARNING: Did not find 'classification' predicate transition in sorted data.");
            }
            
            w.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}