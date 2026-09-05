package com.ebremer.beakgraph.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.airlift.compress.v3.zstdFFM.ZstdJavaDecompressor;
import io.airlift.compress.v3.zstdFFM.ZstdNativeDecompressor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * BG-167: the zstd codec behind {@link StringUtils} is platform-dependent
 * (native libzstd where bundled, the pure-Java port elsewhere). Whichever one
 * this JVM picked, a fragment it wrote must decode through BOTH decoders to
 * the same text (the bytes may differ between encoders - that is documented,
 * not asserted away), and a corrupt frame must surface as the ONE exception
 * the header checks already throw, not as the codec's own type.
 */
class StringUtilsCodecContractTest {

    private static final String TEXT = "<https://example.org/some/rather/long/iri/that/compresses/well/"
            + "0123456789/0123456789/0123456789/0123456789/0123456789/0123456789/0123456789/0123456789>";

    @Test
    void aFragmentDecodesIdenticallyThroughEitherDecoder() {
        StringUtils su = new StringUtils();
        byte[] packed = su.compress(TEXT);
        byte[] expected = TEXT.getBytes(StandardCharsets.UTF_8);
        assertEquals(expected.length, ByteBuffer.wrap(packed).getInt(), "length header");
        byte[] frame = Arrays.copyOfRange(packed, 4, packed.length);

        byte[] viaJava = new byte[expected.length];
        int n = new ZstdJavaDecompressor().decompress(frame, 0, frame.length, viaJava, 0, viaJava.length);
        assertArrayEquals(expected, Arrays.copyOf(viaJava, n), "the Java decoder reads this JVM's encoder output");
        if (ZstdNativeDecompressor.isEnabled()) {
            byte[] viaNative = new byte[expected.length + 1];
            int m = new ZstdNativeDecompressor().decompress(frame, 0, frame.length, viaNative, 0, viaNative.length);
            assertArrayEquals(expected, Arrays.copyOf(viaNative, m), "the native decoder reads it too");
        }
        assertEquals(TEXT, su.decompress(packed));
        assertEquals(TEXT, su.decompress(ByteBuffer.wrap(packed)));
    }

    /** BG-105: the FCD writers hand over UTF-8 bytes directly; the frame must be what the String path wrote. */
    @Test
    void bytesAndStringCompressIdentically() {
        StringUtils su = new StringUtils();
        for (String s : new String[]{TEXT, "ünïcödé \uD83D\uDE00 text " + TEXT, "x".repeat(70), "a"}) {
            byte[] viaString = su.compress(s);
            byte[] viaBytes = su.compress(s.getBytes(StandardCharsets.UTF_8));
            assertArrayEquals(viaString, viaBytes, "same header and frame for: " + s.length() + " chars");
            assertEquals(s, su.decompress(viaBytes));
        }
        assertEquals(0, su.compress(new byte[0]).length);
        assertEquals(0, su.compress((byte[]) null).length);
    }

    @Test
    void aCorruptFrameIsOneExceptionWhateverTheCodec() {
        StringUtils su = new StringUtils();
        byte[] packed = su.compress(TEXT);
        Random r = new Random(5);
        int rejected = 0;
        for (int round = 0; round < 200; round++) {
            byte[] bad = packed.clone();
            // Keep the 4-byte length header and the frame magic intact so the
            // header checks pass and the CODEC is what rejects the bytes.
            for (int f = 0, flips = 1 + r.nextInt(3); f < flips; f++) {
                int at = 8 + r.nextInt(bad.length - 8);
                bad[at] ^= (byte) (1 << r.nextInt(8));
            }
            try {
                String s = su.decompress(bad);
                assertTrue(s.length() <= TEXT.length() * 2, "a decodable corruption still yields a bounded string");
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().startsWith("Corrupt compressed fragment"), expected.getMessage());
                rejected++;
                // The ByteBuffer path folds the same failure the same way.
                IllegalArgumentException viaBuffer = assertThrows(IllegalArgumentException.class,
                        () -> su.decompress(ByteBuffer.wrap(bad)));
                assertTrue(viaBuffer.getMessage().startsWith("Corrupt compressed fragment"), viaBuffer.getMessage());
            }
        }
        assertTrue(rejected > 100, "most corruptions must be rejected, got " + rejected);

        byte[] truncated = Arrays.copyOf(packed, packed.length - 3);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> su.decompress(truncated));
        assertTrue(ex.getMessage().startsWith("Corrupt compressed fragment"), ex.getMessage());
    }
}
