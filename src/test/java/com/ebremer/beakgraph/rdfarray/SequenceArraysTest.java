package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import com.ebremer.beakgraph.store.OpenStore;
import com.ebremer.beakgraph.store.StoreGroup;
import com.ebremer.beakgraph.store.TypedArray;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The native arrays of {@code /.BG/arrays} (format v7, RDFARRY.md §3.5) on
 * method 0, in both containers: tensor and ragged classes, masks for slots,
 * the {@code seq} column back to the object ids, {@code sequenceArrays}, and
 * what does not pack. Without {@code -arrays} nothing changes.
 */
class SequenceArraysTest {

    @TempDir
    Path dir;

    static final String DATA = """
        @prefix : <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        :w1 :hours ( 8 8 7 0 9 0 0 ) .
        :w2 :hours ( 6 undef 7 7 8 0 0 ) .
        :w3 :hours ( 9 9 9 9 9 0 0 ) .
        :m1 :matrix ( ( 1 2 3 ) ( 4 5 6 ) ) .
        :m2 :matrix ( ( 7 8 9 ) ) .
        :p1 :points ( ( 0.0E0 0.0E0 ) ( 3.0E0 0.0E0 ) ( 3.0E0 4.0E0 ) ) .
        :p2 :points ( ( -0.0E0 1.5E0 ) ) .
        :b1 :flags ( true false true ) .
        :x  :mixed ( 1 "two" 3 ) .
        :x  :decimals ( 0.5 1.5 ) .
        :x  :lexical ( "05"^^xsd:integer 6 ) .
        :x  :ragged ( ( 1 2 ) ( 3 ) ) .
        :x  :one ( 5 ) .
        :x  :slotrow ( ( 1 2 ) undef ) .
        :x  :empty ( ) .
        :x  :outer ( :iri ( 1 2 ) ) .
        """;

    private File build(String name, SequenceArrays.Options options) throws Exception {
        Path src = dir.resolve("arrays.ttl");
        Files.writeString(src, DATA);
        File out = dir.resolve(name).toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).setArrays(options).build().write();
        return out;
    }

    private static Node integer(long n) {
        return NodeFactory.createLiteralDT(Long.toString(n), XSDDatatype.XSDinteger);
    }

    @ParameterizedTest
    @ValueSource(strings = {"arrays.h5", "arrays.zarr", "compressed.h5", "compressed.zarr"})
    void classesHoldTheNumericSequences(String name) throws Exception {
        File store = build(name, new SequenceArrays.Options(3, 2, name.startsWith("compressed")));
        long[] hoursSeq;
        try (OpenStore open = OpenStore.open(store.toPath())) {
            StoreGroup arrays = open.root().group(".BG").group("arrays");
            // i64_7: the three rosters (min rows 3); i64_r_3 the two matrices; f64_r_2 the polygons;
            // bool_r the flags. No other vector packs: ( 1 "two" 3 ) mixes types, ( 0.5 1.5 ) is
            // decimal, "05" is not canonical, ( ( 1 2 ) ( 3 ) ) is ragged below the top, ( 5 ) has one
            // leaf, ( ( 1 2 ) undef ) has a slot where a row belongs, ( ) none, and the ( 1 2 ) inside
            // :outer is no object of its own.
            assertEquals(List.of("bool_r", "f64_r_2", "i64_7", "i64_r_3"), arrays.groupNames());

            StoreGroup hours = arrays.group("i64_7");
            assertEquals("tensor", hours.stringAttribute("kind"));
            assertEquals("http://www.w3.org/2001/XMLSchema#integer", hours.stringAttribute("datatype"));
            TypedArray values = hours.typedArray("values");
            assertArrayEquals(new long[]{3, 7}, values.shape());
            // Rows in object-id order, which for sequences is element order: ( 6 undef ... ) first.
            assertArrayEquals(new long[]{6, 0, 7, 7, 8, 0, 0, 8, 8, 7, 0, 9, 0, 0, 9, 9, 9, 9, 9, 0, 0},
                    (long[]) values.values());
            boolean[] mask = (boolean[]) hours.typedArray("mask").values();
            assertTrue(mask[1] && !mask[0] && !mask[8], ":w2's second day is a slot, and only it");
            hoursSeq = (long[]) hours.typedArray("seq").values();
            assertEquals(3, hoursSeq.length);
            assertTrue(hoursSeq[0] < hoursSeq[1] && hoursSeq[1] < hoursSeq[2], "rows in id order");
            if (name.endsWith(".zarr")) {
                assertEquals(List.of("seq", "d1"), values.dimensionNames());
            }

            StoreGroup matrices = arrays.group("i64_r_3");
            assertEquals("ragged", matrices.stringAttribute("kind"));
            assertArrayEquals(new long[]{3, 3}, matrices.typedArray("values").shape());
            assertArrayEquals(new long[]{0, 2, 3}, (long[]) matrices.typedArray("starts").values(),
                    ":m1's two rows, then :m2's one, in id order");
            assertNull(matrices.typedArray("mask"), "no slot, no mask");

            double[] points = (double[]) arrays.group("f64_r_2").typedArray("values").values();
            assertEquals(8, points.length);
            assertTrue(java.util.Arrays.stream(points).anyMatch(d -> Double.doubleToRawLongBits(d) == Long.MIN_VALUE),
                    "-0.0 keeps its sign bit");

            assertArrayEquals(new boolean[]{true, false, true}, (boolean[]) arrays.group("bool_r").typedArray("values").values());
        }
        // Each packed row's seq id is the object id of that very sequence.
        try (BeakGraph bg = BG.getBeakGraph(store)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) bg.getReader().getDictionary();
            Node w2 = dict.getObjects().extract(hoursSeq[0]);
            assertEquals(java.util.Arrays.asList(integer(6), com.ebremer.beakgraph.core.lib.SeqTerms.UNDEF, integer(7),
                    integer(7), integer(8), integer(0), integer(0)), w2.getSeq());
        }
    }

    @Test
    void theInMemoryEnginesWriteTheSameBytes() throws Exception {
        Path src = dir.resolve("engines.ttl");
        Files.writeString(src, DATA);
        SequenceArrays.Options o = new SequenceArrays.Options(3, 2, false);
        File m0 = dir.resolve("m0.h5").toFile();
        File m2 = dir.resolve("m2.h5").toFile();
        File m3 = dir.resolve("m3.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(m0).setArrays(o).build().write();
        com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer.Builder().setSource(src.toFile())
                .setDestination(m2).setArrays(o).setCores(3).build().write();
        com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer.Builder().setSource(src.toFile())
                .setDestination(m3).setArrays(o).setCores(3).build().write();
        byte[] expected = Files.readAllBytes(m0.toPath());
        assertArrayEquals(expected, Files.readAllBytes(m2.toPath()), "method 2");
        assertArrayEquals(expected, Files.readAllBytes(m3.toPath()), "method 3");
    }

    @ParameterizedTest
    @ValueSource(strings = {"verify.h5", "verify.zarr"})
    void verifyDeepChecksEveryPackedRow(String name) throws Exception {
        File store = build(name, new SequenceArrays.Options(3, 2, false));
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int exit = new com.ebremer.beakgraph.cmdline.VerifyCommand(store, true, new java.io.PrintStream(out, true)).run();
        assertEquals(0, exit, out.toString());
    }

    @Test
    void verifyDeepNamesATamperedValue() throws Exception {
        File store = build("tampered.zarr", new SequenceArrays.Options(3, 2, false));
        // The roster tensor's one raw chunk: its first int64, little-endian, 6 becomes 7.
        Path chunk = store.toPath().resolve(".BG/arrays/i64_7/values/c/0/0");
        byte[] bytes = Files.readAllBytes(chunk);
        assertEquals(6, bytes[0]);
        bytes[0] = 7;
        Files.write(chunk, bytes);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int exit = new com.ebremer.beakgraph.cmdline.VerifyCommand(store, true, new java.io.PrintStream(out, true)).run();
        assertEquals(2, exit, out.toString());
        assertTrue(out.toString().contains("arrays/i64_7: row 0") && out.toString().contains("differs at value 0"), out.toString());
    }

    /**
     * Opt-in: the arrays as array tools read them (RDFARRY.md step 6). Runs
     * where a Python with numpy, h5py and zarr is on the PATH; skipped
     * elsewhere, with the reason.
     */
    @Test
    void pythonReadsTheArrays() throws Exception {
        String python = System.getProperty("beakgraph.python", "python");
        org.junit.jupiter.api.Assumptions.assumeTrue(runs(python, "-c", "import numpy, h5py, zarr"),
                "no Python with numpy, h5py and zarr (set -Dbeakgraph.python)");
        File h5 = build("py.h5", new SequenceArrays.Options(3, 2, false));
        File zarr = build("py.zarr", new SequenceArrays.Options(3, 2, true));
        // Format v8 (-arrays-only, RDFARRY.md step 9): the arrays are the packed sequences' only copy.
        File h5v8 = build("py8.h5", new SequenceArrays.Options(3, 2, false).withOnly(true));
        File zarrv8 = build("py8.zarr", new SequenceArrays.Options(3, 2, true).withOnly(true));
        Path script = dir.resolve("read.py");
        Files.writeString(script, """
            import sys, numpy as np, h5py, zarr
            with h5py.File(sys.argv[1]) as f:
                c = f["/.BG/arrays/i64_7"]
                h = np.ma.masked_array(c["values"][()], mask=c["mask"][()])
                assert h.shape == (3, 7) and h.dtype == np.int64, h
                assert h.mask[0, 1] and int(h[0, 0]) == 6
                assert f["/.BG/arrays/bool_r/values"].dtype == np.bool_
            g = zarr.open_group(sys.argv[2], mode="r")[".BG/arrays/i64_7"]
            z = np.ma.masked_array(g["values"][:], mask=g["mask"][:])
            assert (z == h).all() and (z.mask == h.mask).all()
            with h5py.File(sys.argv[3]) as f:
                assert int(f["/.BG"].attrs["formatVersion"]) == 8
                c = f["/.BG/arrays/i64_7"]
                v8 = np.ma.masked_array(c["values"][()], mask=c["mask"][()])
                assert (v8 == h).all() and (v8.mask == h.mask).all()
                assert int(c.attrs["shared"]) in (0, 1)
            g = zarr.open_group(sys.argv[4], mode="r")[".BG/arrays/i64_7"]
            z8 = np.ma.masked_array(g["values"][:], mask=g["mask"][:])
            assert (z8 == h).all() and (z8.mask == h.mask).all()
            print("ok")
            """);
        assertTrue(runs(python, script.toString(), h5.toString(), zarr.toString(), h5v8.toString(), zarrv8.toString()),
                "the Python reads failed");
    }

    private static boolean runs(String... command) throws Exception {
        try {
            Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes());
            boolean ok = p.waitFor() == 0;
            if (!ok) {
                System.err.println(String.join(" ", command) + ":\n" + out);
            }
            return ok;
        } catch (java.io.IOException noPython) {
            return false;
        }
    }

    @Test
    void withoutArraysTheStoreIsUnchanged() throws Exception {
        File plain = build("plain.h5", null);
        File again = build("again.h5", null);
        assertArrayEquals(Files.readAllBytes(plain.toPath()), Files.readAllBytes(again.toPath()));
        try (OpenStore open = OpenStore.open(plain.toPath())) {
            assertNull(open.root().group(".BG").group("arrays"));
            assertNull(open.root().group(".BG").group("dictionary").group("literals").array("sequenceArrays"));
        }
        File with = build("with.h5", SequenceArrays.Options.DEFAULT);
        try (OpenStore open = OpenStore.open(with.toPath())) {
            // The default -arrays-min-rows (16) makes no tensor here, only ragged classes.
            StoreGroup arrays = open.root().group(".BG").group("arrays");
            assertFalse(arrays.groupNames().contains("i64_7"));
            assertTrue(arrays.groupNames().contains("i64_r"));
            assertTrue(open.root().group(".BG").group("dictionary").group("literals").array("sequenceArrays") != null);
        }
    }
}
