package com.ebremer.beakgraph.core.lib;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Variable-byte (VByte) encoding, trimmed to the two operations the
 * front-coded dictionaries actually use: streaming encode at write time and
 * position-independent decode at read time. The signed (zig-zag), byte-array
 * and position-mutating ByteBuffer variants that accumulated here had no
 * callers and were removed in the dead-code sweep.
 */
public class VByte {

    /**
     * Encode an unsigned long to an OutputStream using VByte.
     * @param out
     * @param value non-negative
     * @return number of bytes written
     * @throws IOException
     */
    public static int encode(OutputStream out, long value) throws IOException {
        int c = 0;
        if (value < 0)
            throw new IllegalArgumentException("Value must be non-negative: " + value);
        while (value > 0x7F) {
            out.write((int)(value & 0x7F));
            c++;
            value >>>= 7;
        }
        out.write((int)(value | 0x80));
        c++;
        return c;
    }

    /**
     * Decode an unsigned long from a ByteBuffer at an absolute offset, WITHOUT moving the
     * buffer's position. This lets a single buffer be read by concurrent threads safely.
     * @param buffer the buffer to read from
     * @param offset the absolute byte offset to start decoding at
     * @return value and nextOffset (the absolute position just past the encoded value)
     */
    public static DecodeResult decodeAt(ByteBuffer buffer, int offset) {
        long result = 0;
        int shift = 0;
        int pos = offset;
        byte b;
        do {
            if (shift >= 64) throw new IllegalArgumentException("VByte sequence too long");
            b = buffer.get(pos++);
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        return new DecodeResult(result, pos);
    }

    /**
     * Holder for decoded value and next offset.
     */
    public static class DecodeResult {
        public final long value;
        public final int nextOffset;

        public DecodeResult(long value, int nextOffset) {
            this.value = value;
            this.nextOffset = nextOffset;
        }
    }
}
