package com.ebremer.beakgraph.utils;

import io.jhdf.api.WritableDataset;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 *
 * @author Erich Bremer
 */

public class UTIL {

    public static String byteArrayToBinaryString(byte[] array, int length) {
        StringBuilder sb = new StringBuilder(length * 8 + (length - 1));
        for (int i = 0; i < length; i++) {
            sb.append(String.format("%8s", Integer.toBinaryString(array[i] & 0xFF))
                        .replace(' ', '0'));
            if (i < length - 1) sb.append(' ');
        }
        return sb.toString();
    }

    public static String toBinaryString(ByteBuffer buffer, int offset) {
        // Use duplicate so we don't modify the original buffer's position
        ByteBuffer dup = buffer.duplicate();
        StringBuilder sb = new StringBuilder((dup.limit() - dup.position()) * 9);
        while (dup.hasRemaining()) {
            int b = dup.get() & 0xFF;
            // format to 8-bit binary, pad with leading zeros
            byte[] ha = new byte[1];
            ha[0] = (byte) b;
            String s = new String(ha, StandardCharsets.UTF_8);
            int ye = offset+dup.position();
            sb.append(String.format("%d : %8s -- %s ==> [%s]", ye, Integer.toBinaryString(b), Integer.toHexString(b), s));
            if (dup.hasRemaining()) sb.append('\n');
        }
        return sb.toString();
    }

    public static byte[] getBytes(ByteBuffer buffer) {
        String cn = buffer.getClass().getName();
        if (cn.equals("java.nio.HeapByteBuffer2")) {
            buffer.position(0);
            int r = buffer.capacity();
            byte[] b = new byte[r];
            buffer.get(b);
            return b;
        }
        return buffer.array();
    }

    public static ByteBuffer subBuffer(ByteBuffer src, int offset, int length) {
        ByteBuffer dup = src.duplicate();
        dup.position(offset);
        dup.limit(offset + length);
        return dup.slice();
    }

    public static void skipNullTerminatedStrings(ByteBuffer buffer, int skip) {
        if (skip > 0) {
            int currentPosition = buffer.position();
            int limit = buffer.limit();
            for (int i = 0; i < skip; i++) {
                while (currentPosition < limit) {
                    if (buffer.get(currentPosition) == 0) {
                        currentPosition++;
                        break;
                    }
                    currentPosition++;
                }
            }
            buffer.position(currentPosition);
        }
    }

    public static String readNullTerminatedString(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return "";
        }
        int startPosition = buffer.position();
        int limit = buffer.limit();
        int endPosition = startPosition;
        // Find the null terminator
        while (endPosition < limit && buffer.get(endPosition) != 0) {
            endPosition++;
        }
        int length = endPosition - startPosition;
        byte[] stringBytes = new byte[length];
        // Use bulk get for better performance
        buffer.get(stringBytes);
        // If we stopped at a null terminator, skip over it
        if (buffer.hasRemaining() && buffer.get() != 0) {
            // This handles the edge case where the loop stopped at limit
            // but we still need to advance position correctly.
            // Usually, buffer.get() above moves position to endPosition.
            // If the byte at endPosition was 0, buffer.get() consumes it.
        }
        // Correctly advance position to after the null terminator if it exists
        if (endPosition < limit) {
            buffer.position(endPosition + 1);
        } else {
            buffer.position(limit);
        }
        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    public static WritableDataset putAttributes( WritableDataset ds, Map<String, Object> attributes ) {
        attributes.forEach((k,v)->{
            ds.putAttribute(k, v);
        });
        return ds;
    }

    public static int MinBits(long x) {
        if (x == 0) return 1;
        return Long.SIZE - Long.numberOfLeadingZeros(x);
    }

    /**
     * Broadword selection: the 0-based index (from the MSB) of the k-th set bit
     * (k &gt;= 1) of {@code word}, in O(1) via six popcount narrowing steps. The
     * SINGLE implementation shared by the bit-packed buffer's linear select1 and
     * the rank/select directory's accelerated select1 - the two must agree
     * bit-for-bit or the directory fast path and the fallback diverge.
     */
    public static int selectInWord(long word, long k) {
        int result = 0;
        int cnt;
        cnt = Long.bitCount(word >>> 32); if (k > cnt) { word <<= 32; result += 32; k -= cnt; }
        cnt = Long.bitCount(word >>> 48); if (k > cnt) { word <<= 16; result += 16; k -= cnt; }
        cnt = Long.bitCount(word >>> 56); if (k > cnt) { word <<= 8;  result += 8;  k -= cnt; }
        cnt = Long.bitCount(word >>> 60); if (k > cnt) { word <<= 4;  result += 4;  k -= cnt; }
        cnt = Long.bitCount(word >>> 62); if (k > cnt) { word <<= 2;  result += 2;  k -= cnt; }
        cnt = Long.bitCount(word >>> 63); if (k > cnt) { result += 1; }
        return result;
    }

    /**
     * True when {@code iri} is a relative reference (RFC 3986) - i.e. it has no
     * scheme. The empty string (a same-document reference such as {@code <>}) is
     * relative. An IRI is absolute when it begins with a valid scheme followed
     * by ':' (ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ) ":"), e.g. "http:",
     * "urn:", "file:". This is intentionally scheme-based rather than relying on
     * {@code IRIx}, whose lenient parser misclassifies the empty string.
     */
    public static boolean isRelativeIRI(String iri) {
        if (iri == null) return false;
        int n = iri.length();
        if (n == 0) return true;
        char c0 = iri.charAt(0);
        if (!((c0 >= 'A' && c0 <= 'Z') || (c0 >= 'a' && c0 <= 'z'))) return true;
        for (int i = 1; i < n; i++) {
            char c = iri.charAt(i);
            if (c == ':') return false;
            boolean schemeChar = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.';
            if (!schemeChar) return true;
        }
        return true;
    }

    public static String byteArrayToBinaryString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(bytes.length * 8);
        for (byte b : bytes) {
            for (int i = 7; i >= 0; i--) {
                int bit = (b >> i) & 1;
                sb.append(bit);
            }
        }
        return sb.toString();
    }
}
