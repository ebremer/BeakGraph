package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.huge.HugeIO;
import com.ebremer.beakgraph.io.ByteBufferBytes;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The bit-packed buffer's last word (BG-83) and its staged byte output
 * (BG-250) against per-bit and per-byte oracles, plus the varint writers
 * that now emit one write per value.
 */
class BitPackedTailAndStagingTest {

    @TempDir
    Path dir;

    private static BitPackedUnSignedLongBuffer bitmap(boolean[] bits) {
        BitPackedUnSignedLongBuffer b = new BitPackedUnSignedLongBuffer(Path.of("bits"), 1);
        for (boolean bit : bits) b.writeLong(bit ? 1 : 0);
        b.prepareForReading();
        return b;
    }

    private static long oracleWord(boolean[] bits, long from) {
        long w = 0;
        for (int i = 0; i < 64; i++) {
            w <<= 1;
            long idx = from + i;
            if (idx < bits.length && bits[(int) idx]) w |= 1;
        }
        return w;
    }

    @Test
    void tailWordsNextSetBitAndSelectMatchAPerBitOracle() {
        Random rnd = new Random(83);
        for (int n : new int[]{1, 7, 63, 64, 65, 100, 127, 128, 130, 191, 200, 1000, 1027}) {
            for (double density : new double[]{0.05, 0.5, 1.0}) {
                boolean[] bits = new boolean[n];
                int ones = 0;
                for (int i = 0; i < n; i++) {
                    bits[i] = rnd.nextDouble() < density;
                    if (bits[i]) ones++;
                }
                BitPackedUnSignedLongBuffer b = bitmap(bits);
                BitPackedUnSignedLongBuffer.BitReader reader = b.bitReader();
                for (int i = 0; i < n; i++) {
                    assertEquals(oracleWord(bits, i), b.getWord64(i), "n=" + n + " word at " + i);
                    assertEquals(bits[i], reader.bit(i), "n=" + n + " bit " + i);
                    long expectNext = -1;
                    for (int k = i; k < n; k++) { if (bits[k]) { expectNext = k; break; } }
                    assertEquals(expectNext, b.nextSetBit(i, Long.MAX_VALUE), "n=" + n + " nextSetBit from " + i);
                }
                int rank = 0;
                for (int i = 0; i < n; i++) {
                    if (bits[i]) {
                        rank++;
                        assertEquals(i, b.select1(rank), "n=" + n + " select1(" + rank + ")");
                    }
                }
                assertEquals(-1, b.select1(ones + 1), "n=" + n + ": one rank past the last set bit");
            }
        }
    }

    private static byte[] oraclePack(long[] values, int width) {
        long totalBits = (long) values.length * width;
        byte[] out = new byte[(int) ((totalBits + 7) / 8)];
        long bit = 0;
        for (long v : values) {
            for (int k = width - 1; k >= 0; k--, bit++) {
                if (((v >>> k) & 1L) != 0) {
                    out[(int) (bit >>> 3)] |= (byte) (0x80 >>> (bit & 7));
                }
            }
        }
        return out;
    }

    @Test
    void stagedWritesPackExactlyLikeTheByteAtATimeWriter() throws Exception {
        Random rnd = new Random(250);
        for (int width : new int[]{1, 3, 7, 13, 32, 57, 64}) {
            int n = (width < 8) ? 700_000 : 150_000; // several 64 KiB stages at every width
            long[] values = new long[n];
            BitPackedUnSignedLongBuffer b = new BitPackedUnSignedLongBuffer(Path.of("v" + width), width);
            for (int i = 0; i < n; i++) {
                values[i] = (width == 64) ? rnd.nextLong() : (rnd.nextLong() & ((1L << width) - 1));
                b.writeLong(values[i]);
            }
            b.prepareForReading();
            assertEquals(n, b.getNumEntries());
            for (int i = 0; i < n; i += 977) {
                assertEquals(values[i], b.get(i), "width " + width + " index " + i);
            }
            assertEquals(values[n - 1], b.get(n - 1));
            Path f = dir.resolve("staged-" + width + ".h5");
            try (WritableHdfFile out = HdfFile.write(f)) {
                b.add(out.putGroup("g"));
            }
            try (HdfFile in = new HdfFile(f)) {
                Dataset ds = (Dataset) ((Group) in.getChild("g")).getChild("v" + width);
                assertArrayEquals(oraclePack(values, width), (byte[]) ds.getData(), "width " + width + ": packed bytes");
                assertEquals((long) n, ((Number) ds.getAttribute(Params.NUM_ENTRIES).getData()).longValue());
            }
        }
    }

    @Test
    void varintsWrittenInOneCallReadBackAndMatchVByte() throws Exception {
        long[] values = {0, 1, 127, 128, 255, 16383, 16384, 1L << 21, (1L << 28) - 1, 1L << 35, 1L << 49, Long.MAX_VALUE, 42, 300, 70000};
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            for (long v : values) HugeIO.writeVarLong(out, v);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            for (long v : values) assertEquals(v, HugeIO.readVarLong(in));
            assertEquals(0, in.available(), "no trailing bytes");
        }
        ByteArrayOutputStream vb = new ByteArrayOutputStream();
        int[] lengths = new int[values.length];
        for (int i = 0; i < values.length; i++) lengths[i] = VByte.encode(vb, values[i]);
        ByteBufferBytes region = new ByteBufferBytes(ByteBuffer.wrap(vb.toByteArray()));
        long pos = 0;
        for (int i = 0; i < values.length; i++) {
            VByte.DecodeResult r = VByte.decodeAt(region, pos);
            assertEquals(values[i], r.value);
            assertEquals(pos + lengths[i], r.nextOffset, "encode reports the bytes it wrote");
            pos = r.nextOffset;
        }
        assertEquals(vb.size(), pos);
    }
}
