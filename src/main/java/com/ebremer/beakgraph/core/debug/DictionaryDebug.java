package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.hdf5.writers.FiveSectionDictionaryWriter;
import com.ebremer.beakgraph.hdf5.writers.FiveSectionDictionaryWriterBuilder;
import java.io.File;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public class DictionaryDebug {
    public static void main(String[] args) {
        File file = new File("/data/sorted.nq.gz");
        File dest = new File("/data/dX_debug.h5");

        try {
            System.out.println("Building Dictionary in Memory...");
            FiveSectionDictionaryWriterBuilder db = new FiveSectionDictionaryWriterBuilder();
            FiveSectionDictionaryWriter w = db
                .setSource(file)
                .setDestination(dest)
                .setName("dictionary")
                .build();
            
            System.out.println("--- Dictionary Integrity Check ---");
            
            // 1. Check ID of 'snomed'
            Node snomed = NodeFactory.createURI("http://snomed.info/id/48512009");
            long idSnomed = w.locateObject(snomed);
            System.out.println("ID for " + snomed + " = " + idSnomed);
            
            // 2. Check ID of '1.0'
            Node one = NodeFactory.createLiteral("1.0", org.apache.jena.datatypes.xsd.XSDDatatype.XSDfloat);
            long idOne = w.locateObject(one);
            System.out.println("ID for " + one + " = " + idOne);
            
            // 3. What is at ID 9 (relative to objects)?
            // Note: locateObject returns absolute ID (shared + objects).
            // We need to adjust if shared > 0.
            long numShared = w.getNumberOfShared();
            System.out.println("Num Shared: " + numShared);
            
            // The logs showed IndexReader reading 9.
            // If shared is 0, then it's just index 9 (rank 8) in objects.
            // We can't extract easily from Writer without reflection or modifying it, 
            // but we can iterate 'w.objectsdict.getNodes()' if we could access it.
            // Since we can't, we rely on the locate tests above.
            
            w.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}