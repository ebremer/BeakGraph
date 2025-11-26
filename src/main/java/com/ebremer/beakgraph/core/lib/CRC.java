package com.ebremer.beakgraph.core.lib;

import com.ebremer.beakgraph.hdf5.BitPackedSignedLongBuffer;
import java.nio.ByteBuffer;

public class CRC {
    private final int width;        // Bit width of the CRC (8, 16, or 32)
    private final int poly;         // Polynomial in normal form
    private final int init;         // Initial CRC value
    private final boolean refin;    // Reflect input bytes
    private final boolean refout;   // Reflect output CRC
    private final int xorout;       // Final XOR value
    private final int[] table;      // Precomputed lookup table
    private final int mask;         // Mask to keep lower 'width' bits

    public CRC(int width, int poly, int init, boolean refin, boolean refout, int xorout) {
        this.width = width;
        this.poly = poly;
        this.init = init;
        this.refin = refin;
        this.refout = refout;
        this.xorout = xorout;
        this.mask = (1 << width) - 1;
        this.table = computeTable();
    }

    private int[] computeTable() {
        int[] table = new int[256];
        int topBit = 1 << (width - 1);
        // Use reflected polynomial if input is reflected
        int effectivePoly = refin ? reflect(poly, width) : poly;
        for (int i = 0; i < 256; i++) {
            int crc;
            if (refin) {
                crc = i;
                for (int j = 0; j < 8; j++) {
                    if ((crc & 1) != 0) {
                        crc = (crc >>> 1) ^ effectivePoly;
                    } else {
                        crc = crc >>> 1;
                    }
                }
            } else {
                crc = i << (width - 8);
                for (int j = 0; j < 8; j++) {
                    if ((crc & topBit) != 0) {
                        crc = (crc << 1) ^ effectivePoly;
                    } else {
                        crc = crc << 1;
                    }
                    crc &= mask;
                }
            }
            table[i] = crc;
        }
        return table;
    }

    public int compute(byte[] data) {
        int crc = init;
        for (byte b : data) {
            int byteVal = b & 0xFF;
            if (refin) {
                int index = (crc ^ byteVal) & 0xFF;
                crc = (crc >>> 8) ^ table[index];
            } else {
                int shift = width - 8;
                int index = ((crc >>> shift) ^ byteVal) & 0xFF;
                crc = (crc << 8) ^ table[index];
            }
            crc &= mask;
        }
        if (refout) {
            crc = reflect(crc, width);
        }
        return crc ^ xorout;
    }

    private static int reflect(int value, int width) {
        return Integer.reverse(value) >>> (32 - width);
    }

    public static final CRC CRC8_CCITT = new CRC(
        8,          // width
        0x07,       // x^8 + x^2 + x + 1
        0x00,       // initial value
        false,      // input not reflected
        false,      // output not reflected
        0x00        // final XOR
    );

    public static final CRC CRC16_ANSI = new CRC(
        16,         // width
        0x8005,     // x^16 + x^15 + x^2 + 1
        0x0000,     // initial value
        true,       // input reflected
        true,       // output reflected
        0x0000      // final XOR
    );

    public static final CRC CRC32C = new CRC(
        32,         // width
        0x1EDC6F41, // x^32 + x^28 + x^27 + x^26 + x^25 + x^23 + x^22 + x^20 + x^19 + x^18 + x^14 + x^13 + x^11 + x^10 + x^9 + x^8 + x^6 + 1
        0xFFFFFFFF, // initial value
        true,       // input reflected
        true,       // output reflected
        0xFFFFFFFF  // final XOR
    );

    public static void main2(String[] args) {
        /*
        byte[] data = "Hello, CRC!".getBytes();
        int crc8 = CRC8_CCITT.compute(data);
        int crc16 = CRC16_ANSI.compute(data);
        int crc32c = CRC32C.compute(data);
        System.out.printf("CRC8-CCITT: 0x%02X%n", crc8 & 0xFF);
        System.out.printf("CRC16-ANSI: 0x%04X%n", crc16 & 0xFFFF);
        System.out.printf("CRC32C: 0x%08X%n", (crc32c & 0xFFFFFFFFL));
        */        
    }
    
    
     public static void main(String[] args) {
        // --- Test Case 1: Positive and Negative Integers (bitWidth = 5) ---
        System.out.println("--- Test Case 1: Integers (bitWidth=5) ---");
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(100);
        int bitWidth5 = 5; // Range for 5 bits signed: -16 to 15
        BitPackedSignedLongBuffer packedBuffer1 = new BitPackedSignedLongBuffer(null, null, bitWidth5);

        int[] valuesToWrite1 = {0, 1, 15, -1, -16, 7, -8}; // 15 is max pos, -16 is min neg for 5 bits
        System.out.println("Writing values:");
        for (int val : valuesToWrite1) {
            System.out.print(val + " ");
            packedBuffer1.writeInteger(val);
        }
        System.out.println();

        packedBuffer1.prepareForReading();
        System.out.println("Reading values:");
        while (packedBuffer1.getNumEntries() > 0) {
            System.out.print(packedBuffer1.get() + " ");
        }
        IO.println("HAKA : "+packedBuffer1.get(3));
        System.out.println("\nBuffer position: " + byteBuffer1.position() + ", limit: " + byteBuffer1.limit());
        System.out.println();

        // --- Test Case 2: Positive and Negative Longs (bitWidth = 10) ---
        System.out.println("--- Test Case 2: Longs (bitWidth=10) ---");
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(100);
        int bitWidth10 = 10; // Range for 10 bits signed: -512 to 511
        BitPackedSignedLongBuffer packedBuffer2 = new BitPackedSignedLongBuffer(null, byteBuffer2, bitWidth10);

        long[] valuesToWrite2 = {0L, 1L, 511L, -1L, -512L, 123L, -256L};
        System.out.println("Writing long values:");
        for (long val : valuesToWrite2) {
            System.out.print(val + " ");
            packedBuffer2.writeLong(val);
        }
        System.out.println();

        packedBuffer2.prepareForReading();
        System.out.println("Reading long values:");
        while (packedBuffer2.getNumEntries() > 0) {
            System.out.print(packedBuffer2.getLong() + " ");
        }
        System.out.println("\nBuffer position: " + byteBuffer2.position() + ", limit: " + byteBuffer2.limit());
        System.out.println();

        // --- Test Case 3: Integers (bitWidth = 32) ---
        System.out.println("--- Test Case 3: Integers (bitWidth=32) ---");
        ByteBuffer byteBuffer3 = ByteBuffer.allocate(100);
        BitPackedSignedLongBuffer packedBuffer3 = new BitPackedSignedLongBuffer(null, byteBuffer3, 32);

        int[] valuesToWrite3 = {Integer.MAX_VALUE, Integer.MIN_VALUE, 0, -1, 123456789};
        System.out.println("Writing values:");
        for (int val : valuesToWrite3) {
            System.out.print(val + " ");
            packedBuffer3.writeInteger(val);
        }
        System.out.println();
        
        packedBuffer3.prepareForReading();
        System.out.println("Reading values:");
        while (packedBuffer3.getNumEntries() > 0) {
            System.out.print(packedBuffer3.get() + " ");
        }
        System.out.println("\nBuffer position: " + byteBuffer3.position() + ", limit: " + byteBuffer3.limit());
        System.out.println();

        // --- Test Case 4: Longs (bitWidth = 32, effectively storing int range) ---
        System.out.println("--- Test Case 4: Longs (bitWidth=32) ---");
        ByteBuffer byteBuffer4 = ByteBuffer.allocate(100);
        BitPackedSignedLongBuffer packedBuffer4 = new BitPackedSignedLongBuffer(null, byteBuffer4, 32);

        long[] valuesToWrite4 = {(long)Integer.MAX_VALUE, (long)Integer.MIN_VALUE, 0L, -1L, 123456789L, -2L};
        System.out.println("Writing long values (as 32-bit patterns):");
        for (long val : valuesToWrite4) {
            System.out.print(val + " ");
            packedBuffer4.writeLong(val);
        }
        System.out.println();
        
        packedBuffer4.prepareForReading();
        System.out.println("Reading long values:");
        while (packedBuffer4.getNumEntries() > 0) {
            System.out.print(packedBuffer4.getLong() + " ");
        }
        System.out.println("\nBuffer position: " + byteBuffer4.position() + ", limit: " + byteBuffer4.limit());


        System.out.println("--- Test Case 5: Longs (bitWidth=32) ---");
        ByteBuffer byteBuffer5 = ByteBuffer.allocate(100);
        BitPackedSignedLongBuffer packedBuffer5 = new BitPackedSignedLongBuffer(null, byteBuffer5, 17);

        System.out.println();
        long[] valuesToWrite5 = {0, 1, 10373, 112231, 1297, 14029, 1754, 2, 20746, 2594, 28058, 3, 325, 3508, 4, 41492, 439, 5, 512, 5187, 56116, 6, 649, 7, 7015, 8, 82984, 877};
        System.out.println("Writing long values (as 64-bit patterns):");
        for (long val : valuesToWrite5) {
            System.out.print(val + " ");
            packedBuffer5.writeLong(val);
        }
        System.out.println();
        
        packedBuffer5.prepareForReading();
        System.out.println("Reading long values:");
        while (packedBuffer5.getNumEntries() > 0) {
            System.out.print(packedBuffer5.getLong() + " ");
        }
        System.out.println("\nBuffer position: " + byteBuffer5.position() + ", limit: " + byteBuffer5.limit());        
        
     }
    
}