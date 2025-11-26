package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.FiveSectionDictionaryReader;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;
import org.apache.jena.graph.Node;

public class DebugStructure {
    public static void main(String[] args) {
        File file = new File("/data/dX.h5");
        if (!file.exists()) {
            System.err.println("File not found");
            return;
        }

        try (HdfFile hdf = new HdfFile(file.toPath())) {
            Group hdt = (Group) hdf.getChild(Params.BG);
            Group dictGroup = (Group) hdt.getChild(Params.DICTIONARY);
            FiveSectionDictionaryReader dict = new FiveSectionDictionaryReader(dictGroup);
            
            System.out.println("--- GPOS DEEP SCAN ---");
            
            Group gpos = (Group) hdt.getChild("GPOS");
            BitPackedUnSignedLongBuffer Sp = loadBuffer(gpos, "Sp");
            BitPackedUnSignedLongBuffer Bo = loadBuffer(gpos, "Bo"); 
            BitPackedUnSignedLongBuffer So = loadBuffer(gpos, "So");

            long numPredicates = Sp.getNumEntries();
            long numObjects = So.getNumEntries();
            
            System.out.println("Scanning So (Object IDs) for ID 3 (rdf:type)...");
            
            boolean found = false;
            long firstIndex = -1;
            long lastIndex = -1;
            
            for (long i = 0; i < numObjects; i++) {
                long val = So.get(i);
                if (val == 3) { // ID 3 is rdf:type
                     if (!found) {
                         System.out.println("FOUND ID 3 at Index: " + i);
                         firstIndex = i;
                         found = true;
                     }
                     lastIndex = i;
                } else if (found) {
                     // Found it previously, now it stopped
                     System.out.println("ID 3 STOPPED at Index: " + (i-1));
                     found = false; // Reset to find disjoint blocks
                }
            }
            if (found) System.out.println("ID 3 STOPPED at End of Buffer: " + lastIndex);
            
            // Standard Range check again for reference
            System.out.println("\n--- BITMAP RANGES ---");
            for (long i = 0; i < numPredicates; i++) {
                long oStartIndex = (i == 0) ? 0 : Bo.select1(i) + 1;
                long oEndIndex = Bo.select1(i + 1);
                System.out.printf("Pred[%d]: Range [%d, %d]\n", i, oStartIndex, oEndIndex);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static BitPackedUnSignedLongBuffer loadBuffer(Group g, String name) {
        if (g.getChild(name) == null) return null;
        ContiguousDataset ds = (ContiguousDataset) g.getDatasetByPath(name);
        long num = (Long) ds.getAttribute("numEntries").getData();
        int width = (Integer) ds.getAttribute("width").getData();
        return new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), num, width);
    }
}