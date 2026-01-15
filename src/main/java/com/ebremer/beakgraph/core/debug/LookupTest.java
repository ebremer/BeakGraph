package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import java.io.File;
import org.apache.jena.graph.Node;

public class LookupTest {
    public static void main(String[] args) {
        File file = new File("/data/dX.h5");
        try (HdfFile hdf = new HdfFile(file.toPath())) {
            Group hdt = (Group) hdf.getChild(Params.BG);
            Group dictGroup = (Group) hdt.getChild(Params.DICTIONARY);
            PositionalDictionaryReader dict = new PositionalDictionaryReader(dictGroup);
            
            System.out.println("--- Dictionary Lookup Test ---");
            
            // Test Object IDs seen in BGIteratorPOS
            long[] testIds = {9, 10, 11, 12, 13, 14, 15};
            
            for (long id : testIds) {
                try {
                    Node n = dict.getObjects().extract(id);
                    System.out.printf("Object ID %d -> %s (IsURI: %b, IsBlank: %b, IsLiteral: %b)\n", 
                        id, n, n.isURI(), n.isBlank(), n.isLiteral());
                } catch (Exception e) {
                    System.out.printf("Object ID %d -> ERROR: %s\n", id, e.getMessage());
                }
            }
            
            // Also check what ID 'Feature' has
            org.apache.jena.graph.Node feature = org.apache.jena.graph.NodeFactory.createURI("http://www.opengis.net/ont/geosparql#Feature");
            long featureId = dict.getObjects().locate(feature);
            System.out.println("Locate 'geo:Feature' -> ID " + featureId);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}