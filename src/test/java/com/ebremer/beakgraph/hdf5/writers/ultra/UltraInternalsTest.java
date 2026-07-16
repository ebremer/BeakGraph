package com.ebremer.beakgraph.hdf5.writers.ultra;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.ByteBufferBytes;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import org.apache.jena.graph.Node;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Direct verification of the ultra building blocks the end-to-end parity
 * tests exercise only implicitly: the radix sort against the JDK sort
 * (including the 2-word key path that small test stores never trigger), the
 * 128-bit pack/extract round trip across the lo/hi straddle, byte-level
 * compatibility of the positional buffers with the sequential bit-packed
 * reader, and the id maps against the storage dictionaries' binary-search
 * {@code locate()}.
 */
class UltraInternalsTest {

    @TempDir
    Path dir;

    @Test
    void radixSortSingleWordMatchesJdkSort() {
        Random rnd = new Random(42);
        int n = 200_000; // large enough to engage multi-chunk histograms
        long[] keys = new long[n];
        for (int i = 0; i < n; i++) {
            // Mixed magnitudes plus deliberate duplicates
            keys[i] = switch (i % 4) {
                case 0 -> rnd.nextLong() & 0x7FFFFFFFFFFFFFFFL;
                case 1 -> rnd.nextInt(1000);
                case 2 -> keys[Math.max(0, i - 2)];
                default -> rnd.nextLong() & 0xFFFFFFFFL;
            };
        }
        long[] expected = keys.clone();
        Arrays.sort(expected);
        ForkJoinPool pool = new ForkJoinPool(3);
        try {
            long[][] r = ParallelRadixSort.sort(keys.clone(), null, 63, pool);
            assertArrayEquals(expected, r[0]);
        } finally {
            pool.shutdown();
        }
    }

    @Test
    void radixSortTwoWordKeysMatchesReferenceSort() {
        Random rnd = new Random(7);
        int n = 100_000;
        long[] lo = new long[n];
        long[] hi = new long[n];
        for (int i = 0; i < n; i++) {
            lo[i] = rnd.nextLong();
            hi[i] = rnd.nextLong() & ((1L << 26) - 1); // 90-bit keys
            if (i % 5 == 0 && i > 0) { // duplicates and hi-only ties
                hi[i] = hi[i - 1];
                if (i % 10 == 0) lo[i] = lo[i - 1];
            }
        }
        long[][] rows = new long[n][2];
        for (int i = 0; i < n; i++) {
            rows[i][0] = hi[i];
            rows[i][1] = lo[i];
        }
        Arrays.sort(rows, Comparator.<long[]>comparingLong(r -> r[0]).thenComparing(r -> r[1], Long::compareUnsigned));
        ForkJoinPool pool = new ForkJoinPool(3);
        try {
            long[][] r = ParallelRadixSort.sort(lo.clone(), hi.clone(), 90, pool);
            for (int i = 0; i < n; i++) {
                assertEquals(rows[i][0], r[1][i], "hi at " + i);
                assertEquals(rows[i][1], r[0][i], "lo at " + i);
            }
        } finally {
            pool.shutdown();
        }
    }

    @Test
    void packExtractRoundTripsAcrossTheWordBoundary() {
        // 30+30+20+30 = 110 bits: k1 and k2 straddle or sit fully above the
        // lo/hi boundary, covering every branch of extract().
        UltraBGIndex.Layout lay = new UltraBGIndex.Layout(30, 30, 20, 30);
        Random rnd = new Random(11);
        long[] lo = new long[1];
        long[] hi = new long[1];
        for (int t = 0; t < 10_000; t++) {
            long v0 = rnd.nextLong() & ((1L << 30) - 1);
            long v1 = rnd.nextLong() & ((1L << 30) - 1);
            long v2 = rnd.nextLong() & ((1L << 20) - 1);
            long v3 = rnd.nextLong() & ((1L << 30) - 1);
            UltraBGIndex.pack(lo, hi, 0, v0, v1, v2, v3, lay);
            assertEquals(v0, UltraBGIndex.extract(lo[0], hi[0], lay.off0(), lay.b0()), "k0");
            assertEquals(v1, UltraBGIndex.extract(lo[0], hi[0], lay.off1(), lay.b1()), "k1");
            assertEquals(v2, UltraBGIndex.extract(lo[0], hi[0], lay.off2(), lay.b2()), "k2");
            assertEquals(v3, UltraBGIndex.extract(lo[0], hi[0], lay.off3(), lay.b3()), "k3");
        }
        // Single-word layouts must round-trip with hi == null.
        UltraBGIndex.Layout small = new UltraBGIndex.Layout(10, 10, 5, 11);
        UltraBGIndex.pack(lo, null, 0, 1023, 512, 31, 2047, small);
        assertEquals(1023, UltraBGIndex.extract(lo[0], 0, small.off0(), small.b0()));
        assertEquals(512, UltraBGIndex.extract(lo[0], 0, small.off1(), small.b1()));
        assertEquals(31, UltraBGIndex.extract(lo[0], 0, small.off2(), small.b2()));
        assertEquals(2047, UltraBGIndex.extract(lo[0], 0, small.off3(), small.b3()));
    }

    /**
     * The positional buffer's bytes must read back through the SEQUENTIAL
     * bit-packed reader: that is exactly what the HDF5 readers do with these
     * datasets, and it pins the big-endian byte-aligned layout.
     */
    @Test
    void packedBufferBytesReadBackThroughTheSequentialReader() {
        Random rnd = new Random(3);
        for (int width : new int[]{8, 16, 24, 40, 64}) {
            int n = 257;
            long[] values = new long[n];
            UltraPackedBuffer ultra = new UltraPackedBuffer("t", n, width);
            for (int i = 0; i < n; i++) {
                values[i] = (width == 64) ? rnd.nextLong() : (rnd.nextLong() & ((1L << width) - 1));
                ultra.set(i, values[i]);
            }
            BitPackedUnSignedLongBuffer reader = BitPackedUnSignedLongBuffer.readView(
                    new ByteBufferBytes(ByteBuffer.wrap(ultra.bytes())), n, width);
            for (int i = 0; i < n; i++) {
                assertEquals(values[i], reader.get(i), "width " + width + " entry " + i);
            }
        }
    }

    @Test
    void bitmapBytesReadBackThroughTheSequentialReader() {
        Random rnd = new Random(9);
        int n = 1003; // deliberately not a multiple of 8 or 64
        boolean[] bits = new boolean[n];
        UltraBitmap bitmap = new UltraBitmap("b", n);
        for (int i = 0; i < n; i++) {
            bits[i] = rnd.nextBoolean();
            if (bits[i]) {
                bitmap.set(i);
            }
        }
        assertEquals((n + 7) / 8, bitmap.toBytes().length);
        BitPackedUnSignedLongBuffer reader = BitPackedUnSignedLongBuffer.readView(
                new ByteBufferBytes(ByteBuffer.wrap(bitmap.toBytes())), n, 1);
        for (int i = 0; i < n; i++) {
            assertEquals(bits[i] ? 1 : 0, reader.get(i), "bit " + i);
        }
    }

    /**
     * The rank maps ARE the dictionary: for every node, the O(1) map id must
     * equal the storage dictionary's binary-search locate(). This is the
     * invariant that lets the ultra writer never call locate() at all.
     */
    @Test
    void rankMapIdsMatchDictionaryLocate() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            ex:s ex:v "1"^^xsd:int .
            ex:s ex:v "1"^^xsd:long .
            ex:s ex:v "1.0"^^xsd:double .
            ex:s ex:v "01"^^xsd:integer .
            ex:s ex:w "a" .
            ex:s ex:w "a"@en .
            ex:s ex:when "2020-01-02T00:00:00"^^xsd:dateTime .
            _:x ex:p _:y .
            _:y ex:p ex:s .
            <> ex:self <kin.png> .
            _:g { _:x ex:q "in bnode graph" . }
            ex:g { ex:s ex:q _:y . }
            """;
        File src = dir.resolve("ids.trig").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        ForkJoinPool pool = new ForkJoinPool(2);
        try {
            UltraIngest ingest = new UltraIngest();
            ingest.setSource(src);
            ingest.setDestination(dir.resolve("ids.unused.h5").toFile());
            ingest.setName(Params.DICTIONARY);
            ingest.ingest(pool);
            UltraDictionary dict = new UltraDictionary(ingest, pool);
            Dictionary entities = (Dictionary) dict.entitiesDictionary();
            Dictionary predicates = (Dictionary) dict.predicatesDictionary();
            Dictionary literals = (Dictionary) dict.literalsDictionary();
            for (Node n : ingest.getEntities()) {
                assertEquals(entities.locate(n), dict.locateGraph(n), "entity id of " + n);
                assertEquals(entities.locate(n), dict.locateSubject(n), "entity id of " + n);
                assertEquals(entities.locate(n), dict.locateObject(n), "object id of entity " + n);
            }
            for (Node n : ingest.getPredicates()) {
                assertEquals(predicates.locate(n), dict.locatePredicate(n), "predicate id of " + n);
            }
            long maxEntityId = dict.getNumberOfGraphs();
            for (Node n : ingest.getLiterals()) {
                assertEquals(literals.locate(n) + maxEntityId, dict.locateObject(n), "object id of literal " + n);
            }
        } finally {
            pool.shutdown();
        }
    }
}
