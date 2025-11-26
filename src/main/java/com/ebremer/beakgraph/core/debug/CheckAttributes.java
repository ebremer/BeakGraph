package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;

public class CheckAttributes {
    public static void main(String[] args) {
        File file = new File("/data/dX.h5");
        if (!file.exists()) {
            System.err.println("File not found.");
            return;
        }

        try (HdfFile hdf = new HdfFile(file.toPath())) {
            System.out.println("--- HDF5 ATTRIBUTE CHECK ---");
            Group hdtGroup = (Group) hdf.getChild(Params.BG);
            Group gpos = (Group) hdtGroup.getChild("GPOS");
            
            ContiguousDataset So = (ContiguousDataset) gpos.getDatasetByPath("So");
            ContiguousDataset Bo = (ContiguousDataset) gpos.getDatasetByPath("Bo");
            
            long countSo = (Long) So.getAttribute("numEntries").getData();
            long widthSo = (Integer) (So.getAttribute("width").getData());
            
            long countBo = (Long) Bo.getAttribute("numEntries").getData();
            long widthBo = (Integer) (Bo.getAttribute("width").getData());
            
            System.out.println("Dataset So (Object IDs):");
            System.out.println("  numEntries: " + countSo);
            System.out.println("  width:      " + widthSo);
            
            System.out.println("Dataset Bo (Bitmap):");
            System.out.println("  numEntries: " + countBo);
            System.out.println("  width:      " + widthBo);
            
            if (countSo != countBo) {
                System.out.println("\nCRITICAL ERROR: So and Bo counts DO NOT MATCH!");
                long diff = countBo - countSo;
                System.out.println("Difference: " + diff);
            } else {
                System.out.println("\nPASS: Counts match.");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}