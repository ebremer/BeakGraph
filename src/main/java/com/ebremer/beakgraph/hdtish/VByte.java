package com.ebremer.beakgraph.hdtish;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class VByte {
    private final byte[] encoded;

    private static byte[] encode(long value) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // write 7-bit chunks with MSB=1 except last
        while ((value & ~0x7FL) != 0) {
            baos.write((int)((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        baos.write((int)(value & 0x7F));
        return baos.toByteArray();
    }

    public VByte(long value) {
        this.encoded = encode(value);
    }

    public VByte(int value) {
        this((long) value);
    }

    public VByte(short value) {
        this((long) value);
    }

    public ByteBuffer get() {
        return ByteBuffer.wrap(encoded);
    }

    public byte[] getBytes() {
        return encoded.clone();
    }
}
