package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.io.ByteBufferBytes;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The spill bit packer must produce bytes the PRODUCTION reader
 * ({@link BitPackedUnSignedLongBuffer} wrapping the dataset buffer, exactly as
 * the HDF5 readers construct it) decodes back to the original values - for
 * every supported width, including the signed 32/64 escape hatches.
 */
class SpillBitPackedBufferTest {

    @TempDir
    Path dir;

    @Test
    void roundTripsThroughProductionReaderAtEveryWidth() throws Exception {
        Random rnd = new Random(42);
        int[] widths = {1, 2, 5, 8, 13, 24, 32, 40, 57, 64};
        for (int w : widths) {
            int n = 1000;
            long[] values = new long[n];
            Path f = dir.resolve("w" + w);
            try (SpillBitPackedBuffer spill = new SpillBitPackedBuffer(f, w)) {
                for (int i = 0; i < n; i++) {
                    long v;
                    if (w == 64) {
                        v = rnd.nextLong(); // full pattern incl. negatives
                    } else if (w == 32) {
                        v = rnd.nextInt();  // negative ints allowed at width 32
                        spill.writeInteger((int) v);
                        values[i] = ((int) v) & 0xFFFFFFFFL; // reader returns the unsigned pattern
                        continue;
                    } else {
                        v = rnd.nextLong() & ((1L << w) - 1);
                    }
                    values[i] = v;
                    spill.writeLong(v);
                }
                spill.complete();
                assertEquals(n, spill.getNumEntries());
                byte[] bytes = Files.readAllBytes(f);
                assertEquals((n * (long) w + 7) / 8, bytes.length, "width " + w + ": packed length");
                BitPackedUnSignedLongBuffer reader =
                        BitPackedUnSignedLongBuffer.readView(new ByteBufferBytes(ByteBuffer.wrap(bytes)), n, w);
                for (int i = 0; i < n; i++) {
                    assertEquals(values[i], reader.get(i), "width " + w + " index " + i);
                }
            }
        }
    }

    @Test
    void rejectsOutOfRangeValuesAndBadWidths() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> new SpillBitPackedBuffer(dir.resolve("bad"), 58));
        try (SpillBitPackedBuffer b = new SpillBitPackedBuffer(dir.resolve("w3"), 3)) {
            b.writeLong(7);
            assertThrows(IllegalArgumentException.class, () -> b.writeLong(8));
            assertThrows(IllegalArgumentException.class, () -> b.writeLong(-1));
        }
    }
}
