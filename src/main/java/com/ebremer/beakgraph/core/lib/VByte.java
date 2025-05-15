package com.ebremer.beakgraph.core.lib;

import static com.ebremer.beakgraph.utils.UTIL.byteArrayToBinaryString;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Variable-byte (VByte) encoding with signed and unsigned support.
 */
public class VByte {

    /**
     * Encode an unsigned long to an OutputStream using VByte.
     * @param out
     * @param value non-negative
     * @return 
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
     * Decode an unsigned long from an InputStream using VByte.
     * @param in
     * @return decoded value
     * @throws IOException
     */
    public static long decode(InputStream in) throws IOException {
        long result = 0;
        int shift = 0, b;
        do {
            if (shift >= 64)
                throw new IOException("VByte sequence too long");
            b = in.read();
            if (b < 0)
                throw new IOException("Unexpected end of stream");
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        return result;
    }

    /**
     * Encode a signed long with zig-zag transform.
     * @param out
     * @param value
     * @throws java.io.IOException
     */
    public static void encodeSigned(OutputStream out, long value) throws IOException {
        // canonical zig-zag: interleave sign bit
        long zig = (value << 1) ^ (value >> 63);
        encode(out, zig);
    }

    /**
     * Decode a signed long with zig-zag transform.
     * @param in
     * @return 
     * @throws java.io.IOException
     */
    public static long decodeSigned(InputStream in) throws IOException {
        long zig = decode(in);
        // reverse zig-zag
        return (zig >>> 1) ^ -(zig & 1);
    }

    /**
     * Encode an unsigned long into a byte array at offset.
     * @param array
     * @param offset
     * @param value
     * @return new offset
     */
    public static int encode(byte[] array, int offset, long value) {
        if (value < 0)
            throw new IllegalArgumentException("Value must be non-negative: " + value);
        while (value > 0x7F) {
            array[offset++] = (byte)(value & 0x7F);
            value >>>= 7;
        }
        array[offset++] = (byte)(value | 0x80);
        return offset;
    }

    /**
     * Decode an unsigned long from a byte array starting at offset.
     * @param array
     * @param offset
     * @return DecodeResult.value and nextOffset in the array
     */
    public static DecodeResult decode(byte[] array, int offset) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            if (shift >= 64)
                throw new IllegalArgumentException("VByte sequence too long");
            b = array[offset++];
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        return new DecodeResult(result, offset);
    }

    /**
     * Encode an unsigned long into a ByteBuffer.
     * @param buffer
     * @param value
     * @return number of bytes written
     */
    public static int encode(ByteBuffer buffer, long value) {
        int start = buffer.position();
        if (value < 0)
            throw new IllegalArgumentException("Value must be non-negative: " + value);
        while (value > 0x7F) {
            buffer.put((byte)(value & 0x7F));
            value >>>= 7;
        }
        buffer.put((byte)(value | 0x80));
        return buffer.position() - start;
    }

    /**
     * Decode an unsigned long from a ByteBuffer.
     * @param buffer
     * @return DecodeResult.value and bytesConsumed
     */
    public static DecodeResult decode(ByteBuffer buffer) {
        int start = buffer.position();
        long result = 0;
        int shift = 0;
        byte b;
        do {
            if (shift >= 64)
                throw new IllegalArgumentException("VByte sequence too long");
            b = buffer.get();
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        int consumed = buffer.position() - start;
        return new DecodeResult(result, consumed);
    }    
    
    /**
     * Decode an unsigned long from a ByteBuffer.
     * @param buffer
     * @return DecodeResult.value and bytesConsumed
     */
    public static long decodeSingle(ByteBuffer buffer) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            if (shift >= 64) throw new IllegalArgumentException("VByte sequence too long "+shift);
            b = buffer.get();
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) == 0);
        return result;
    }

    /**
     * Holder for decoded value and next offset/bytesConsumed.
     */
    public static class DecodeResult {
        public final long value;
        public final int nextOffset;

        public DecodeResult(long value, int nextOffset) {
            this.value = value;
            this.nextOffset = nextOffset;
        }
    }

    /** Simple demo of unsigned VByte encoding
     * @param args
     * @throws java.io.IOException */
    public static void main(String[] args) throws IOException {
        byte[] buffer = new byte[50];
        int len = VByte.encode(buffer, 0, 131);
        IO.println("Bytes written: " + len);
        IO.println(byteArrayToBinaryString(buffer, len));
    }
}
