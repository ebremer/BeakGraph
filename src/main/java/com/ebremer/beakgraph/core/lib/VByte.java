package com.ebremer.beakgraph.core.lib;

import com.ebremer.beakgraph.io.RandomAccessBytes;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Variable-byte (VByte) encoding, trimmed to the two operations the
 * front-coded dictionaries actually use: streaming encode at write time and
 * position-independent decode at read time. The signed (zig-zag), byte-array
 * and position-mutating ByteBuffer variants that accumulated here had no
 * callers and were removed in the dead-code sweep. The decode side reads
 * through {@link RandomAccessBytes} with long offsets (its only caller is
 * FCDReader), so string buffers past 2 GiB decode without int truncation.
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
        if (value < 0)
            throw new IllegalArgumentException("Value must be non-negative: " + value);
        // One write per value, not per byte: the dictionary writers' streams
        // (ByteArrayOutputStream, BufferedOutputStream) lock per call (BG-250).
        byte[] buf = new byte[10];
        int c = 0;
        while (value > 0x7F) {
            buf[c++] = (byte) (value & 0x7F);
            value >>>= 7;
        }
        buf[c++] = (byte) (value | 0x80);
        out.write(buf, 0, c);
        return c;
    }

    /**
     * Decode an unsigned long at an absolute offset. Absolute reads only, so a
     * single backing region can be read by concurrent threads safely.
     * @param bytes the region to read from
     * @param offset the absolute byte offset to start decoding at
     * @return value and nextOffset (the absolute position just past the encoded value)
     */
    public static DecodeResult decodeAt(RandomAccessBytes bytes, long offset) {
        long result = 0;
        int shift = 0;
        long pos = offset;
        byte b;
        do {
            if (shift >= 64) throw new IllegalArgumentException("VByte sequence too long");
            b = bytes.get(pos++);
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
        public final long nextOffset;

        public DecodeResult(long value, long nextOffset) {
            this.value = value;
            this.nextOffset = nextOffset;
        }
    }
}
