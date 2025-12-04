package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.writers.FiveSectionDictionaryWriter;
import com.ebremer.beakgraph.hdf5.writers.FiveSectionDictionaryWriterBuilder;
import java.io.File;
import java.util.Arrays;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

public class ScanSortedQuads {
    public static void main(String[] args) {
        File file = new File("/data/sorted.nq.gz");
        // Temp dest just for loading, not used
        File dest = new File("/data/dX_temp.h5");

        try {
            System.out.println("Loading Quads...");
            FiveSectionDictionaryWriterBuilder db = new FiveSectionDictionaryWriterBuilder();
            FiveSectionDictionaryWriter w = db
                .setSource(file)
                .setDestination(dest)
                .setName("dictionary")
                .build();
                
            Quad[] allQuads = w.getQuads();
            System.out.println("Loaded " + allQuads.length + " quads.");
            
            System.out.println("Sorting by GPOS...");
            Arrays.parallelSort(allQuads, Index.GPOS.getComparator());
            
            System.out.println("--- PREDICATE TRANSITION SCAN ---");
            Node prevP = null;
            int count = 0;
            
            for (int i = 0; i < allQuads.length; i++) {
                Node p = allQuads[i].getPredicate();
                
                if (prevP == null || !p.equals(prevP)) {
                    if (prevP != null) {
                        System.out.println("  -> Count: " + count);
                        long id = w.locatePredicate(prevP);
                        System.out.println("  (Dictionary ID: " + id + ")");
                    }
                    System.out.println("Transition at Quad " + i + ": " + p);
                    prevP = p;
                    count = 0;
                }
                count++;
            }
            // Last one
            if (prevP != null) {
                System.out.println("  -> Count: " + count);
                long id = w.locatePredicate(prevP);
                System.out.println("  (Dictionary ID: " + id + ")");
            }
            
            w.close();
            if (dest.exists()) dest.delete();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}