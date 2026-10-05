package com.ebremer.beakgraph.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.zstdFFM.ZstdJavaCompressor;
import io.airlift.compress.v3.zstdFFM.ZstdJavaDecompressor;
import io.airlift.compress.v3.zstdFFM.ZstdNativeCompressor;
import io.airlift.compress.v3.zstdFFM.ZstdNativeDecompressor;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Direct tests for the vendored zstd codec (BG-168), which had none: the
 * pure-Java encoder/decoder pair across sizes and entropies, the
 * native-vs-Java pairing production actually hits (a store built with the
 * native codec on Linux/macOS is read by the Java decoder on Windows, and
 * vice versa), corrupted and truncated frames, and the frame-scoped Huffman
 * table (BG-166) and corrected diagnostics (BG-174).
 */
@Timeout(120)
class ZstdCodecInteropTest {

    private static byte[] compressJava(byte[] in) {
        ZstdJavaCompressor c = new ZstdJavaCompressor();
        byte[] out = new byte[c.maxCompressedLength(in.length)];
        int n = c.compress(in, 0, in.length, out, 0, out.length);
        return Arrays.copyOf(out, n);
    }

    private static byte[] decompressJava(byte[] frame, int expected) {
        ZstdJavaDecompressor d = new ZstdJavaDecompressor();
        byte[] out = new byte[Math.max(expected, 1)];
        int n = d.decompress(frame, 0, frame.length, out, 0, out.length);
        return Arrays.copyOf(out, n);
    }

    private static byte[] random(int n, long seed, boolean lowEntropy) {
        Random r = new Random(seed);
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = lowEntropy ? (byte) "abcabcabdd  the the ".charAt(r.nextInt(4) == 0 ? r.nextInt(20) : i % 20) : (byte) r.nextInt(256);
        }
        return b;
    }

    @Test
    void javaRoundTripsAcrossSizesAndEntropies() {
        for (int size : new int[]{0, 1, 63, 64, 65, 255, 256, 4095, 65536, 1 << 20}) {
            for (boolean low : new boolean[]{true, false}) {
                byte[] in = random(size, size * 31L + (low ? 1 : 0), low);
                byte[] frame = compressJava(in);
                ZstdJavaDecompressor d = new ZstdJavaDecompressor();
                assertEquals(size, d.getDecompressedSize(frame, 0, frame.length), "frame declares its content size");
                assertArrayEquals(in, decompressJava(frame, size), "size " + size + (low ? " low-entropy" : " random"));
            }
        }
    }

    @Test
    void aReusedCompressorMatchesFreshFramesByteForByte() {
        // ZstdJavaCompressor reuses one compression context per window size
        // across frames (BG-171). Every frame it emits must equal the frame a
        // brand-new compressor emits for the same input, whatever came before:
        // stale hash-table entries, repeat offsets or a previous frame's Huffman
        // table would all change the bytes (or, for the Huffman table, produce a
        // frame the decoder rejects).
        ZstdJavaCompressor shared = new ZstdJavaCompressor();
        ZstdJavaDecompressor decoder = new ZstdJavaDecompressor();
        Random r = new Random(171);
        int[] sizes = {64, 70, 200_000, 64, 4095, 1 << 17, 65, 300, 131_073, 1000, 1 << 20, 80, 0, 1, 2048, 64};
        for (int round = 0; round < 6; round++) {
            for (int size : sizes) {
                boolean low = r.nextBoolean();
                byte[] in = random(size, r.nextLong(), low);
                byte[] fresh = compressJava(in);
                byte[] out = new byte[shared.maxCompressedLength(in.length)];
                int n = shared.compress(in, 0, in.length, out, 0, out.length);
                byte[] reused = Arrays.copyOf(out, n);
                assertArrayEquals(fresh, reused, "round " + round + " size " + size + (low ? " low-entropy" : " random"));
                byte[] back = new byte[Math.max(size, 1)];
                int m = decoder.decompress(reused, 0, reused.length, back, 0, back.length);
                assertArrayEquals(in, Arrays.copyOf(back, m), "round-trip of the reused frame");
            }
        }
        // The MemorySegment overload shares the same cache.
        byte[] in = random(50_000, 5, true);
        java.lang.foreign.MemorySegment out = java.lang.foreign.MemorySegment.ofArray(new byte[shared.maxCompressedLength(in.length)]);
        int n = shared.compress(java.lang.foreign.MemorySegment.ofArray(in), out);
        assertArrayEquals(compressJava(in), Arrays.copyOf(out.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE), n), "MemorySegment overload");
    }

    @Test
    void nativeAndJavaCodecsInteroperate() {
        assumeTrue(ZstdNativeCompressor.isEnabled() && ZstdNativeDecompressor.isEnabled(), "native zstd not available here");
        for (int size : new int[]{1, 64, 4095, 65536, 1 << 18}) {
            byte[] in = random(size, size, true);
            // native -> Java
            ZstdNativeCompressor nc = new ZstdNativeCompressor();
            byte[] nout = new byte[nc.maxCompressedLength(in.length)];
            int nn = nc.compress(in, 0, in.length, nout, 0, nout.length);
            assertArrayEquals(in, decompressJava(Arrays.copyOf(nout, nn), size), "native frame read by the Java decoder");
            // Java -> native
            byte[] jframe = compressJava(in);
            ZstdNativeDecompressor nd = new ZstdNativeDecompressor();
            byte[] out = new byte[size + 1];
            int n = nd.decompress(jframe, 0, jframe.length, out, 0, out.length);
            assertArrayEquals(in, Arrays.copyOf(out, n), "Java frame read by the native decoder");
        }
    }

    @Test
    void corruptedAndTruncatedFramesAreReportedAsMalformed() {
        byte[] in = random(20_000, 7, true);
        byte[] frame = compressJava(in);
        Random r = new Random(11);
        int malformed = 0;
        for (int round = 0; round < 300; round++) {
            byte[] bad = frame.clone();
            int flips = 1 + r.nextInt(4);
            for (int f = 0; f < flips; f++) {
                int at = 4 + r.nextInt(bad.length - 4);   // keep the magic number
                bad[at] ^= (byte) (1 << r.nextInt(8));
            }
            try {
                byte[] out = decompressJava(bad, in.length + 64);
                // A flip that happens to leave a valid frame must still give the
                // right bytes (the checksum guards the content).
                assertArrayEquals(in, out, "a decodable corrupted frame must decode correctly");
            } catch (MalformedInputException expected) {
                malformed++;
            } catch (IllegalArgumentException expected) {
                malformed++;   // "Output buffer too small" family
            } catch (RuntimeException other) {
                throw new AssertionError("round " + round + ": corruption must surface as MalformedInputException, not "
                        + other.getClass().getName() + ": " + other.getMessage(), other);
            }
        }
        assertTrue(malformed > 200, "most random corruptions must be detected: " + malformed);
        for (int cut = 1; cut < frame.length; cut += Math.max(1, frame.length / 40)) {
            byte[] truncated = Arrays.copyOf(frame, cut);
            assertThrows(MalformedInputException.class, () -> decompressJava(truncated, in.length), "truncated at " + cut);
        }
    }

    /** First byte of the first block's literals section (single-segment frames from the Java encoder). */
    private static int literalsHeaderOffset(int inputSize) {
        int fcsBytes = inputSize < 256 ? 1 : (inputSize < 65536 + 256 ? 2 : 4);
        return 4 + 1 + fcsBytes + 3;
    }

    @Test
    void huffmanTableDoesNotLeakIntoTheNextFrame() {
        byte[] text = ("the quick brown fox jumps over the lazy dog; ".repeat(20) + "zstd literals with a skewed byte distribution ".repeat(30))
                .getBytes(StandardCharsets.UTF_8);
        byte[] frame = compressJava(text);
        int lit = literalsHeaderOffset(text.length);
        assumeTrue((frame[lit] & 0b11) == 0b10, "fixture must start with a Compressed_Literals_Block (Huffman table present)");
        ZstdJavaDecompressor d = new ZstdJavaDecompressor();
        byte[] out = new byte[text.length];
        assertEquals(text.length, d.decompress(frame, 0, frame.length, out, 0, out.length));
        assertArrayEquals(text, out);
        // A second frame whose first literals block claims "treeless" (type 3):
        // it may only reuse a table from ITS OWN frame, so it must be rejected
        // even though this decoder just loaded a table from the previous frame.
        byte[] treeless = frame.clone();
        treeless[lit] = (byte) ((treeless[lit] & ~0b11) | 0b11);
        MalformedInputException ex = assertThrows(MalformedInputException.class,
                () -> d.decompress(treeless, 0, treeless.length, out, 0, out.length));
        // The failure must be the treeless-block check itself, at the literals
        // header; decoding garbage against the stale table fails somewhere later
        // with a different message.
        assertTrue(ex.getMessage().startsWith("Dictionary is corrupted"),
                "must fail at the treeless block, not decode against the stale table: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("offset=" + lit), "failure offset must be the literals header: " + ex.getMessage());
        // And a fresh decoder rejects it the same way.
        assertThrows(MalformedInputException.class,
                () -> new ZstdJavaDecompressor().decompress(treeless, 0, treeless.length, out, 0, out.length));
    }

    @Test
    void oversizedBlockHeaderIsReportedAsSuch() {
        byte[] in = random(3000, 3, true);
        byte[] frame = compressJava(in);
        int blockHeader = literalsHeaderOffset(in.length) - 3;
        // Block header (3 bytes, little-endian): bit 0 last, bits 1-2 type, bits 3-23 size.
        // Claim a compressed block of 0x1FFFFF bytes (> 128 KiB maximum).
        // Padded past the claimed size, so the decoder reaches the block-size
        // check itself rather than failing on the input length first.
        byte[] bad = Arrays.copyOf(frame, frame.length + 0x1FFFFF + 16);
        int type = (bad[blockHeader] >> 1) & 0b11;
        assumeTrue(type == 2, "fixture's first block must be a compressed block");
        int header = (0x1FFFFF << 3) | (2 << 1) | 1;
        bad[blockHeader] = (byte) header;
        bad[blockHeader + 1] = (byte) (header >>> 8);
        bad[blockHeader + 2] = (byte) (header >>> 16);
        MalformedInputException ex = assertThrows(MalformedInputException.class, () -> decompressJava(bad, in.length));
        assertTrue(ex.getMessage().contains("block size"), "message must name the failing check: " + ex.getMessage());
    }
}
