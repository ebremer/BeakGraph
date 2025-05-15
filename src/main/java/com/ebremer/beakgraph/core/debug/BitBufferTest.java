package com.ebremer.beakgraph.core.debug;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import java.nio.ByteBuffer;

public class BitBufferTest {
    public static void main(String[] args) {
        System.out.println("--- BitPackedUnSignedLongBuffer Verification ---");
        
        // Test case: Write 144 with 16-bit width
        BitPackedUnSignedLongBuffer buffer = new BitPackedUnSignedLongBuffer(null, null, 0, 16);
        
        // Pad with 289 zeros to match the scenario
        for (int i = 0; i < 289; i++) {
            buffer.writeLong(0);
        }
        
        System.out.println("Writing 144 at Index 289...");
        buffer.writeLong(144); // 0x0090
        buffer.complete();
        buffer.prepareForReading();
        
        // Check Read Back
        long val = buffer.get(289);
        System.out.println("Read Back Value: " + val);
        if (val == 144) {
            System.out.println("PASS: Buffer Logic is correct in memory.");
        } else {
            System.out.println("FAIL: Buffer Logic is broken. Expected 144, got " + val);
        }
    }
}