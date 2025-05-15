package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;
import java.nio.ByteBuffer;

public class HexDumper {
    public static void main(String[] args) {
        File file = new File("/data/dX.h5");
        if (!file.exists()) {
            System.err.println("File not found.");
            return;
        }

        try (HdfFile hdf = new HdfFile(file.toPath())) {
            // Navigate to GPOS/So (Subject in GPOS, which is level 3? No, So is Level 2 Objects)
            // GPOS: G(implicit)->P->O(So)->S
            // Wait, my previous code named them S1, S2.
            // Position 2 is 'O' (Object). In file it's named "So".
            
            Group hdtGroup = (Group) hdf.getChild(Params.BG);
            Group gpos = (Group) hdtGroup.getChild("GPOS");
            // "So" corresponds to the Object IDs buffer in GPOS
            ContiguousDataset ds = (ContiguousDataset) gpos.getDatasetByPath("So");
            
            long width = (long) (int) ds.getAttribute("width").getData(); // Cast Integer to int then long
            System.out.println("--- HEX DUMP GPOS/So ---");
            System.out.println("Bit Width: " + width);
            
            ByteBuffer buffer = ds.getBuffer();
            
            // Target Index 289
            long targetIndex = 289;
            long bitOffset = targetIndex * width;
            int byteOffset = (int) (bitOffset / 8);
            
            System.out.println("Target Index: " + targetIndex);
            System.out.println("Byte Offset: " + byteOffset);
            
            // Print 4 bytes around the target
            System.out.print("Raw Bytes at offset " + byteOffset + ": ");
            for (int i = 0; i < 4; i++) {
                if (byteOffset + i < buffer.limit()) {
                    byte b = buffer.get(byteOffset + i);
                    System.out.printf("%02X ", b);
                }
            }
            System.out.println();
            
            if (width == 16) {
                 short val = buffer.getShort(byteOffset);
                 System.out.println("As Short (Big Endian): " + val);
                 System.out.println("As Short (Hex): " + String.format("%04X", val));
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}