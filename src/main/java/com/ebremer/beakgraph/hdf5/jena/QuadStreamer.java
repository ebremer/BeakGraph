package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.FiveSectionDictionaryReader;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Utility class for streaming/dumping quads directly from HDF5 bitmaps.
 * Extracted from HDF5Reader to separate debugging/dumping logic from query logic.
 * * @author Erich Bremer
 */
public class QuadStreamer {
    
    private final Group hdt;
    private final FiveSectionDictionaryReader dict;

    public QuadStreamer(Group hdt, FiveSectionDictionaryReader dict) {
        this.hdt = hdt;
        this.dict = dict;
    }

    public void streamQuadsGSPO() {
        IO.println("StreamQuads (Direct GSPO Scan)");
        
        Group gspo = (Group) hdt.getChild("GSPO");
        if (gspo == null) {
            System.err.println("GSPO Group not found!");
            return;
        }

        // Load Buffers for GSPO (G -> S -> P -> O)
        // Level 1: Subject (Bs)
        BitPackedUnSignedLongBuffer Bs = loadBuffer(gspo, "Bs");
        BitPackedUnSignedLongBuffer Ss = loadBuffer(gspo, "Ss");
        
        // Level 2: Predicate (Bp)
        BitPackedUnSignedLongBuffer Bp = loadBuffer(gspo, "Bp");
        BitPackedUnSignedLongBuffer Sp = loadBuffer(gspo, "Sp");
        
        // Level 3: Object (Bo)
        BitPackedUnSignedLongBuffer Bo = loadBuffer(gspo, "Bo");
        BitPackedUnSignedLongBuffer So = loadBuffer(gspo, "So");

        if (Bs == null || Bp == null || Bo == null) return;

        long numGraphs = dict.getGraphs().getNumberOfNodes();
        
        for (long gIndex = 1; gIndex <= numGraphs; gIndex++) {
            Node gNode = dict.getGraphs().extract(gIndex);
            
            // Level 1: Subject Range
            long sStart = (gIndex == 1) ? 0 : Bs.select1(gIndex - 1) + 1;
            long sEnd = Bs.select1(gIndex);
            if (sEnd == -1) break;

            for (long sIndex = sStart; sIndex <= sEnd; sIndex++) {
                long sId = Ss.get(sIndex);
                Node sNode = dict.getSubjects().extract(sId);
                
                // Level 2: Predicate Range
                long currentSubjectRank = sIndex + 1;
                long pStart = (currentSubjectRank == 1) ? 0 : Bp.select1(currentSubjectRank - 1) + 1;
                long pEnd = Bp.select1(currentSubjectRank);
                if (pEnd == -1) continue;

                for (long pIndex = pStart; pIndex <= pEnd; pIndex++) {
                    long pId = Sp.get(pIndex);
                    Node pNode = dict.getPredicates().extract(pId);
                    
                    // Level 3: Object Range
                    long currentPredicateRank = pIndex + 1;
                    long oStart = (currentPredicateRank == 1) ? 0 : Bo.select1(currentPredicateRank - 1) + 1;
                    long oEnd = Bo.select1(currentPredicateRank);
                    if (oEnd == -1) continue;

                    for (long oIndex = oStart; oIndex <= oEnd; oIndex++) {
                        long oId = So.get(oIndex);
                        Node oNode = dict.getObjects().extract(oId);
                        Quad q = new Quad(gNode, sNode, pNode, oNode);
                        IO.println(q);
                    }
                }
            }
        }
    }

    public void streamQuadsGPOS() {
        IO.println("StreamQuads (Direct GPOS Scan)");
        
        Group gpos = (Group) hdt.getChild("GPOS");
        if (gpos == null) {
            System.err.println("GPOS index not found.");
            return;
        }

        // Load Buffers for GPOS (G -> P -> O -> S)
        // Level 1: Predicate
        BitPackedUnSignedLongBuffer Bp = loadBuffer(gpos, "Bp");
        BitPackedUnSignedLongBuffer Sp = loadBuffer(gpos, "Sp");

        // Level 2: Object
        BitPackedUnSignedLongBuffer Bo = loadBuffer(gpos, "Bo");
        BitPackedUnSignedLongBuffer So = loadBuffer(gpos, "So");

        // Level 3: Subject
        BitPackedUnSignedLongBuffer Bs = loadBuffer(gpos, "Bs");
        BitPackedUnSignedLongBuffer Ss = loadBuffer(gpos, "Ss");

        if (Bp == null || Bo == null || Bs == null) return;

        long numGraphs = dict.getGraphs().getNumberOfNodes();
        
        for (long gIndex = 1; gIndex <= numGraphs; gIndex++) {
            Node gNode = dict.getGraphs().extract(gIndex);

            // Level 1: Predicates
            long pStart = (gIndex == 1) ? 0 : Bp.select1(gIndex - 1) + 1;
            long pEnd = Bp.select1(gIndex);
            if (pEnd == -1) break;

            for (long pIndex = pStart; pIndex <= pEnd; pIndex++) {
                long pId = Sp.get(pIndex);
                Node pNode = dict.getPredicates().extract(pId);

                // Level 2: Objects
                long currentPredicateRank = pIndex + 1;
                long oStart = (currentPredicateRank == 1) ? 0 : Bo.select1(currentPredicateRank - 1) + 1;
                long oEnd = Bo.select1(currentPredicateRank);
                if (oEnd == -1) continue;

                for (long oIndex = oStart; oIndex <= oEnd; oIndex++) {
                    long oId = So.get(oIndex);
                    Node oNode = dict.getObjects().extract(oId);

                    // Level 3: Subjects
                    long currentObjectRank = oIndex + 1;
                    long sStart = (currentObjectRank == 1) ? 0 : Bs.select1(currentObjectRank - 1) + 1;
                    long sEnd = Bs.select1(currentObjectRank);
                    if (sEnd == -1) continue;

                    for (long sIndex = sStart; sIndex <= sEnd; sIndex++) {
                        long sId = Ss.get(sIndex);
                        Node sNode = dict.getSubjects().extract(sId);
                        Quad q = new Quad(gNode, sNode, pNode, oNode);
                        IO.println(q);
                    }
                }
            }
        }
    }

    private BitPackedUnSignedLongBuffer loadBuffer(Group g, String name) {
        if (g.getChild(name) == null) {
            System.err.println("Dataset " + name + " missing in " + g.getName() + "!");
            return null;
        }
        ContiguousDataset ds = (ContiguousDataset) g.getDatasetByPath(name);
        long num = (Long) ds.getAttribute("numEntries").getData();
        int width = (Integer) ds.getAttribute("width").getData();
        return new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), num, width);
    }

    public static void main(String[] args) {
        File src = new File("/data/dX.h5");
        if (!src.exists()) {
            System.err.println("File not found: " + src.getAbsolutePath());
            return;
        }

        try (HdfFile hdf = new HdfFile(src.toPath())) {
            Group hdt = (Group) hdf.getChild(Params.BG);
            if (hdt == null) {
                System.err.println("HDT Group not found");
                return;
            }
            
            Group dictGroup = (Group) hdt.getChild(Params.DICTIONARY);
            if (dictGroup == null) {
                System.err.println("Dictionary Group not found");
                return;
            }
            
            FiveSectionDictionaryReader dict = new FiveSectionDictionaryReader(dictGroup);
            QuadStreamer streamer = new QuadStreamer(hdt, dict);
            
            System.out.println("=== Testing GSPO Stream ===");
            streamer.streamQuadsGSPO();
            
            System.out.println("\n=== Testing GPOS Stream ===");
            streamer.streamQuadsGPOS();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}