package com.ebremer.beakgraph.hdtish;

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

    public static void main(String[] args) {
        byte[] data = "Hello, CRC!".getBytes();
        int crc8 = CRC8_CCITT.compute(data);
        int crc16 = CRC16_ANSI.compute(data);
        int crc32c = CRC32C.compute(data);
        System.out.printf("CRC8-CCITT: 0x%02X%n", crc8 & 0xFF);
        System.out.printf("CRC16-ANSI: 0x%04X%n", crc16 & 0xFFFF);
        System.out.printf("CRC32C: 0x%08X%n", (crc32c & 0xFFFFFFFFL));
    }
}