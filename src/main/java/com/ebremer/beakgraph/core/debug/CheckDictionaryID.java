package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.readers.FiveSectionDictionaryReader;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import java.io.File;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public class CheckDictionaryID {
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
            
            System.out.println("--- DICTIONARY ID CHECK ---");
            
            // Check ID 9
            long checkId = 9;
            Node n = dict.getObjects().extract(checkId);
            System.out.println("ID " + checkId + " maps to: " + n);
            
            // Check geo:Feature
            Node feature = NodeFactory.createURI("http://www.opengis.net/ont/geosparql#Feature");
            long featureId = dict.getObjects().locate(feature);
            System.out.println("Node " + feature + " maps to ID: " + featureId);
            
            if (featureId == checkId) {
                System.out.println("CONFIRMED: ID 9 is indeed geo:Feature.");
            } else {
                System.out.println("MISMATCH: ID 9 is NOT geo:Feature.");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}