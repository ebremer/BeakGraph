package com.ebremer.beakgraph;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import static com.ebremer.beakgraph.Params.BLOCKSIZE;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;
import java.nio.file.Path;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The accelerated select1 (interpolated probe + gallop + closing binary search)
 * must agree with the buffer's linear select1 for EVERY rank, on distributions
 * chosen to break interpolation: uniform, sparse, all-ones, long zero runs with
 * ones clustered at one end, and padding-like alternating dense/sparse runs
 * (an index bitmap's real shape: single-entry padding blocks next to huge data
 * blocks). Directory arrays are built here exactly to the layout the reader
 * assumes - SB[k] = ones before superblock k, BB[j] = ones before block j
 * within its superblock.
 */
class HDTBitmapDirectorySelectTest {

    private static HDTBitmapDirectory directory(boolean[] bits) {
        BitPackedUnSignedLongBuffer bitmap = new BitPackedUnSignedLongBuffer(Path.of("b"), null, 0, 1);
        for (boolean b : bits) bitmap.writeLong(b ? 1 : 0);
        bitmap.prepareForReading();

        int superblocks = (bits.length + SUPERBLOCKSIZE - 1) / SUPERBLOCKSIZE;
        int blocks = (bits.length + BLOCKSIZE - 1) / BLOCKSIZE;

        BitPackedUnSignedLongBuffer sb = new BitPackedUnSignedLongBuffer(Path.of("sb"), null, 0, 32);
        long ones = 0;
        for (int k = 0; k < superblocks; k++) {
            sb.writeLong(ones); // ones before superblock k
            for (int i = k * SUPERBLOCKSIZE; i < Math.min((k + 1) * SUPERBLOCKSIZE, bits.length); i++) {
                if (bits[i]) ones++;
            }
        }
        sb.prepareForReading();

        BitPackedUnSignedLongBuffer bb = new BitPackedUnSignedLongBuffer(Path.of("bb"), null, 0, 16);
        for (int j = 0; j < blocks; j++) {
            int sbStart = (j * BLOCKSIZE / SUPERBLOCKSIZE) * SUPERBLOCKSIZE;
            long inSuper = 0;
            for (int i = sbStart; i < j * BLOCKSIZE; i++) {
                if (bits[i]) inSuper++;
            }
            bb.writeLong(inSuper); // ones before block j, within its superblock
        }
        bb.prepareForReading();

        // The ids buffer is not consulted by select1; any 1-bit buffer of equal length works.
        return new HDTBitmapDirectory(sb, bb, bitmap, bitmap);
    }

    private static void assertAgreesForEveryRank(boolean[] bits, String label) {
        BitPackedUnSignedLongBuffer bitmap = new BitPackedUnSignedLongBuffer(Path.of("lin"), null, 0, 1);
        long total = 0;
        for (boolean b : bits) {
            bitmap.writeLong(b ? 1 : 0);
            if (b) total++;
        }
        bitmap.prepareForReading();
        HDTBitmapDirectory dir = directory(bits);
        for (long rank = 1; rank <= total + 2; rank++) {
            assertEquals(bitmap.select1(rank), dir.select1(rank),
                    label + ": select1 mismatch at rank " + rank + " of " + total);
        }
        assertEquals(-1, dir.select1(0), label + ": rank 0 must be invalid");
    }

    /** ~23 superblocks so the interpolation path (not just the binary fallback) runs. */
    private static final int N = 12_000;

    @Test
    void uniformDensity() {
        SplittableRandom rnd = new SplittableRandom(7);
        boolean[] bits = new boolean[N];
        for (int i = 0; i < N; i++) bits[i] = rnd.nextInt(2) == 0;
        assertAgreesForEveryRank(bits, "uniform");
    }

    @Test
    void sparse() {
        SplittableRandom rnd = new SplittableRandom(11);
        boolean[] bits = new boolean[N];
        for (int i = 0; i < N; i++) bits[i] = rnd.nextInt(50) == 0;
        assertAgreesForEveryRank(bits, "sparse");
    }

    @Test
    void allOnes() {
        boolean[] bits = new boolean[N];
        java.util.Arrays.fill(bits, true);
        assertAgreesForEveryRank(bits, "allOnes");
    }

    @Test
    void onesClusteredAtEnd() {
        boolean[] bits = new boolean[N];
        for (int i = (int) (N * 0.9); i < N; i++) bits[i] = true;
        assertAgreesForEveryRank(bits, "clusteredEnd");
    }

    @Test
    void onesClusteredAtStart() {
        boolean[] bits = new boolean[N];
        for (int i = 0; i < N / 10; i++) bits[i] = true;
        assertAgreesForEveryRank(bits, "clusteredStart");
    }

    @Test
    void paddingLikeAlternatingRuns() {
        // Alternating all-ones runs (padding slots: every entry starts a block) and
        // long sparse runs (one giant data block) - the worst shape for interpolation.
        boolean[] bits = new boolean[N];
        for (int i = 0; i < N; i++) {
            int phase = (i / 700) % 2;
            bits[i] = (phase == 0) || (i % 613 == 0);
        }
        assertAgreesForEveryRank(bits, "paddingLike");
    }

    @Test
    void smallDirectoryUsesBinaryPath() {
        SplittableRandom rnd = new SplittableRandom(3);
        boolean[] bits = new boolean[700]; // 2 superblocks: below the interpolation threshold
        for (int i = 0; i < bits.length; i++) bits[i] = rnd.nextInt(3) == 0;
        assertAgreesForEveryRank(bits, "small");
    }

    @Test
    void exactSuperblockBoundaries() {
        for (int n : new int[]{SUPERBLOCKSIZE * 12, SUPERBLOCKSIZE * 12 + 1, SUPERBLOCKSIZE * 12 - 1}) {
            SplittableRandom rnd = new SplittableRandom(n);
            boolean[] bits = new boolean[n];
            for (int i = 0; i < n; i++) bits[i] = rnd.nextInt(4) == 0;
            assertAgreesForEveryRank(bits, "boundary n=" + n);
        }
    }
}
