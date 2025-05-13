package com.ebremer.beakgraph.HDTish;

import java.io.IO;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;


/**
 * Variable-byte (VByte) encoding with signed and unsigned support.
 */
public class VByte {

    /**
     * Encode an unsigned long to an OutputStream using VByte.
     * @param out
     * @param value
     * @throws java.io.IOException
     */
    public static void encode(OutputStream out, long value) throws IOException {
        if (value < 0) throw new IllegalArgumentException("Value must be non-negative: " + value);
        while (value > 0x7F) {
            out.write((int)(value & 0x7F));
            value >>>= 7;
        }
        out.write((int)(value | 0x80));
    }

    /**
     * Decode an unsigned long from an InputStream using VByte.
     */
    public static long decode(InputStream in) throws IOException {
        long result = 0;
        int shift = 0;
        int b;
        do {
            b = in.read();
            if (b < 0) throw new IOException("Unexpected end of stream");
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        return result;
    }

    /**
     * Encode a signed long to an OutputStream with zig-zag transform.
     */
    public static void encodeSigned(OutputStream out, long value) throws IOException {
        long zig = (value < 0) ? ~(value << 1) : (value << 1);
        encode(out, zig);
    }

    /**
     * Decode a signed long from an InputStream with zig-zag transform.
     */
    public static long decodeSigned(InputStream in) throws IOException {
        long zig = decode(in);
        return ((zig & 1) == 0) ? (zig >>> 1) : ~(zig >>> 1);
    }

    /**
     * Encode an unsigned long into a byte array at offset.Returns new offset.
     * @param array
     * @param offset
     * @param value
     * @return 
     */
    public static int encode(byte[] array, int offset, long value) {
        if (value < 0) throw new IllegalArgumentException("Value must be non-negative: " + value);
        while (value > 0x7F) {
            array[offset++] = (byte)(value & 0x7F);
            value >>>= 7;
        }
        array[offset++] = (byte)(value | 0x80);
        return offset;
    }

    /**
     * Decode an unsigned long from a byte array starting at offset.
     * Returns a DecodeResult with the value and the next offset.
     */
    public static DecodeResult decode(byte[] array, int offset) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = array[offset++];
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        return new DecodeResult(result, offset);
    }

    /**
     * Encode an unsigned long into a ByteBuffer. Returns number of bytes written.
     */
    public static int encode(ByteBuffer buffer, long value) {
        int start = buffer.position();
        if (value < 0) throw new IllegalArgumentException("Value must be non-negative: " + value);
        while (value > 0x7F) {
            buffer.put((byte)(value & 0x7F));
            value >>>= 7;
        }
        buffer.put((byte)(value | 0x80));
        return buffer.position() - start;
    }

    /**
     * Decode an unsigned long from a ByteBuffer. Returns DecodeResult.
     */
    public static DecodeResult decode(ByteBuffer buffer) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = buffer.get();
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        return new DecodeResult(result, buffer.position());
    }

    /**
     * Holder for decoded value and next offset/position.
     */
    public static class DecodeResult {
        public final long value;
        public final int nextOffset;

        public DecodeResult(long value, int nextOffset) {
            this.value = value;
            this.nextOffset = nextOffset;
        }
    }
    
    
    public static void main(String[] args) {
        byte[] buffer = new byte[50];
        VByte.encode(buffer, 0, 131);
        IO.println(UTIL.byteArrayToBinaryString(buffer));
        
    }
}
