package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;
import java.nio.ByteBuffer;

public class HexDumperBo {
    public static void main(String[] args) {
        File file = new File("/data/dX.h5");
        if (!file.exists()) {
            System.err.println("File not found.");
            return;
        }

        try (HdfFile hdf = new HdfFile(file.toPath())) {
            System.out.println("--- HEX DUMP GPOS/Bo (Bitmap) ---");
            Group hdtGroup = (Group) hdf.getChild(Params.BG);
            Group gpos = (Group) hdtGroup.getChild("GPOS");
            ContiguousDataset ds = (ContiguousDataset) gpos.getDatasetByPath("Bo");
            
            long width = (long) (int) ds.getAttribute("width").getData();
            System.out.println("Bit Width: " + width);
            
            ByteBuffer buffer = ds.getBuffer();
            
            // We expect the first Predicate (hasArea) to end around 142 items.
            // So the 142nd bit (index 141) should be '1'.
            // 141 / 8 = 17.625 -> Byte 17.
            
            int startByte = 16;
            int numBytes = 4;
            
            System.out.println("Dumping bytes around index 17...");
            for (int i = 0; i < numBytes; i++) {
                int idx = startByte + i;
                if (idx < buffer.limit()) {
                    byte b = buffer.get(idx);
                    System.out.printf("Byte %d: %02X  (Binary: %8s)\n", 
                        idx, b, String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}