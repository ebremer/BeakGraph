package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;
import org.apache.jena.graph.Node;

public class GPOSDumper {
    public static void main(String[] args) {
        File file = new File("/data/dX.h5");
        if (!file.exists()) {
            System.err.println("File not found");
            return;
        }

        try (HdfFile hdf = new HdfFile(file.toPath())) {
            Group hdt = (Group) hdf.getChild(Params.BG);
            Group dictGroup = (Group) hdt.getChild(Params.DICTIONARY);
            PositionalDictionaryReader dict = new PositionalDictionaryReader(dictGroup);
            
            System.out.println("--- GPOS DUMP WITH INDICES ---");
            
            Group gpos = (Group) hdt.getChild("GPOS");
            BitPackedUnSignedLongBuffer Bp = loadBuffer(gpos, "Bp");
            BitPackedUnSignedLongBuffer Sp = loadBuffer(gpos, "Sp");
            BitPackedUnSignedLongBuffer Bo = loadBuffer(gpos, "Bo");
            BitPackedUnSignedLongBuffer So = loadBuffer(gpos, "So");

            long numGraphs = dict.getGraphs().getNumberOfNodes();
            for (long gIndex = 1; gIndex <= numGraphs; gIndex++) {
                long pStart = (gIndex == 1) ? 0 : Bp.select1(gIndex - 1) + 1;
                long pEnd = Bp.select1(gIndex);
                
                for (long pIndex = pStart; pIndex <= pEnd; pIndex++) {
                    long pId = Sp.get(pIndex);
                    Node pNode = dict.getPredicates().extract(pId);
                    
                    long currentPredicateRank = pIndex + 1;
                    long oStart = (currentPredicateRank == 1) ? 0 : Bo.select1(currentPredicateRank - 1) + 1;
                    long oEnd = Bo.select1(currentPredicateRank);
                    
                    System.out.printf("P[%d] (ID %d: %s) -> O-Range [%d, %d]\n", 
                        pIndex, pId, pNode, oStart, oEnd);
                        
                    for (long k = oStart; k <= Math.min(oEnd, oStart + 2); k++) {
                        long oId = So.get(k);
                        Node oNode = dict.getObjects().extract(oId);
                        System.out.printf("    O[%d] -> ID %d (%s)\n", k, oId, oNode);
                    }
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static BitPackedUnSignedLongBuffer loadBuffer(Group g, String name) {
        ContiguousDataset ds = (ContiguousDataset) g.getDatasetByPath(name);
        long num = (Long) ds.getAttribute("numEntries").getData();
        int width = (Integer) ds.getAttribute("width").getData();
        return new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), num, width);
    }
}