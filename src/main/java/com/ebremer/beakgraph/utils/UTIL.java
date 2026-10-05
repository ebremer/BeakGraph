package com.ebremer.beakgraph.utils;


/**
 * Bit-width and IRI helpers shared by the writers and readers. Debug-only
 * dumps (binary strings, attribute maps) that nothing called were removed
 * (BG-284); add such helpers next to the test that needs them.
 *
 * @author Erich Bremer
 */
public class UTIL {

    public static int MinBits(long x) {
        if (x == 0) return 1;
        return Long.SIZE - Long.numberOfLeadingZeros(x);
    }

    /**
     * Byte-rounded bit width for values up to {@code maxValue}: {@link #MinBits}
     * rounded up to a multiple of 8, floor 8 (SPECIFICATIONS.md "Byte-rounded
     * width"). The ONE implementation behind every writer's columnar id lists
     * and index S/SB/BB buffers - the same float expression used to be inlined
     * 22 times across eight writer files (BG-324).
     */
    public static int byteRoundedWidth(long maxValue) {
        int w = MinBits(maxValue);
        return (w <= 0) ? 8 : ((w + 7) / 8) * 8;
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
}
