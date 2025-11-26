package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.Params;
import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import java.io.File;
import java.nio.ByteBuffer;

public class BitmapDumper {
    public static void main(String[] args) {
        File file = new File("/data/dX.h5");
        if (!file.exists()) {
            System.err.println("File not found");
            return;
        }

        try (HdfFile hdf = new HdfFile(file.toPath())) {
            Group bg = (Group) hdf.getChild(Params.BG);
            Group gspo = (Group) bg.getChild("GSPO");
            if (gspo == null) {
                System.err.println("GSPO group not found");
                return;
            }

            // --- CRITICAL CHANGE: Look at Bo (Object Bitmap) ---
            // In GSPO: G->S->P->O. 
            // Bo marks the end of the Object list for each Predicate.
            Dataset bo = gspo.getDatasetByPath("Bo"); 
            if (bo == null) {
                System.err.println("DATASET 'Bo' NOT FOUND. Writer failed to create it.");
                return;
            }

            System.out.println("--- Inspecting GSPO Object Bitmap (Bo) ---");
            Object data = bo.getData();
            byte[] bytes;
            
            if (data instanceof byte[]) {
                bytes = (byte[]) data;
            } else if (data instanceof ByteBuffer) {
                ByteBuffer bb = (ByteBuffer) data;
                bytes = new byte[bb.remaining()];
                bb.get(bytes);
            } else {
                System.err.println("Unknown data type: " + data.getClass().getName());
                return;
            }

            System.out.println("Total Bytes: " + bytes.length);
            if (bytes.length == 0) {
                System.err.println("BITMAP IS EMPTY.");
                return;
            }

            System.out.print("First 10 Bytes (Bin): ");
            for (int i = 0; i < Math.min(10, bytes.length); i++) {
                String s = String.format("%8s", Integer.toBinaryString(bytes[i] & 0xFF)).replace(' ', '0');
                System.out.print(s + " ");
            }
            System.out.println();
            
            // Logic Check:
            // The first Quad is [S0, P0, O0]. 
            // The second Quad is [S0, P1, O1].
            // Since P changed (P0 -> P1), the Object list for P0 ended at O0.
            // Therefore, the first bit (corresponding to O0) MUST be 1.
            
            int firstByte = bytes[0] & 0xFF;
            int firstBit = (firstByte >> 7) & 1;
            System.out.println("First Bit (MSB): " + firstBit);
            
            if (firstBit == 1) {
                System.out.println("VERDICT: Writer is CORRECT. Bit is 1. Reader is failing select1(1).");
            } else {
                System.out.println("VERDICT: Writer is WRONG. Bit is 0. Logic error in BGIndex.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}