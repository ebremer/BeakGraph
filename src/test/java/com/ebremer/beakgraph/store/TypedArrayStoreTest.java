package com.ebremer.beakgraph.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.WritableGroup;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The typed arrays of {@code /.BG/arrays} (format v7, RDFARRY.md §3.5) through
 * the store abstraction, in both containers, raw and compressed: every element
 * type, an N-D shape, string and integer attributes, Zarr's dimension names,
 * and values that only exact storage keeps (-0.0, NaN bits, the extremes).
 */
class TypedArrayStoreTest {

    @TempDir
    Path dir;

    private static final double[] F64 = {0.0, -0.0, Double.NaN, Double.longBitsToDouble(0x7ff8000000000123L),
            1.5, -2.25e8, Double.MAX_VALUE, Double.MIN_VALUE};
    private static final float[] F32 = {0f, -0f, Float.NaN, 1.5f, -3.25f, Float.MAX_VALUE};
    private static final long[] I64 = {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE, 42};
    private static final int[] I32 = {Integer.MIN_VALUE, -7, 0, 7, Integer.MAX_VALUE, 9};
    private static final boolean[] BOOL = {true, false, false, true, true, false};

    @ParameterizedTest
    @CsvSource({"HDF5,false", "HDF5,true", "ZARR,false", "ZARR,true"})
    void everyTypeRoundTrips(Container container, boolean compress) throws Exception {
        Path store = dir.resolve("typed" + (container == Container.HDF5 ? ".h5" : ".zarr"));
        try (WritableStore w = WritableStore.create(store, container)) {
            WritableGroup arrays = w.root().putGroup(".BG").putGroup("arrays");
            WritableGroup cls = arrays.putGroup("f64_2");
            cls.putAttribute("kind", "tensor");
            cls.putAttribute("datatype", "http://www.w3.org/2001/XMLSchema#double");
            cls.putTypedArray("values", new TypedArray(TypedArray.Type.F64, new long[]{4, 2}, F64,
                    List.of("seq", "d1"), Map.of("units", "m", "rows", 4L)), compress);
            cls.putTypedArray("f32", new TypedArray(TypedArray.Type.F32, new long[]{6}, F32, List.of("seq"), null), compress);
            cls.putTypedArray("seq", new TypedArray(TypedArray.Type.I64, new long[]{3, 2}, I64, null, null), compress);
            cls.putTypedArray("i32", new TypedArray(TypedArray.Type.I32, new long[]{6}, I32, null, null), compress);
            cls.putTypedArray("mask", new TypedArray(TypedArray.Type.BOOL, new long[]{2, 3}, BOOL,
                    List.of("seq", "d1"), null), compress);
            w.commit();
        }
        try (OpenStore open = OpenStore.open(store)) {
            StoreGroup arrays = open.root().group(".BG").group("arrays");
            assertEquals(List.of("f64_2"), arrays.groupNames());
            StoreGroup cls = arrays.group("f64_2");
            assertEquals("tensor", cls.stringAttribute("kind"));
            assertNull(cls.stringAttribute("absent"));

            TypedArray values = cls.typedArray("values");
            assertEquals(TypedArray.Type.F64, values.type());
            assertArrayEquals(new long[]{4, 2}, values.shape());
            double[] got = (double[]) values.values();
            for (int i = 0; i < F64.length; i++) {
                assertEquals(Double.doubleToRawLongBits(F64[i]), Double.doubleToRawLongBits(got[i]), "bits of element " + i);
            }
            assertEquals("m", values.attributes().get("units"));
            assertEquals(4L, values.attributes().get("rows"));
            if (container == Container.ZARR) {
                assertEquals(List.of("seq", "d1"), values.dimensionNames());
            }
            float[] f = (float[]) cls.typedArray("f32").values();
            for (int i = 0; i < F32.length; i++) {
                assertEquals(Float.floatToRawIntBits(F32[i]), Float.floatToRawIntBits(f[i]));
            }
            assertArrayEquals(I64, (long[]) cls.typedArray("seq").values());
            assertArrayEquals(new long[]{3, 2}, cls.typedArray("seq").shape());
            assertArrayEquals(I32, (int[]) cls.typedArray("i32").values());
            TypedArray mask = cls.typedArray("mask");
            assertEquals(TypedArray.Type.BOOL, mask.type());
            assertArrayEquals(BOOL, (boolean[]) mask.values());
            assertNull(cls.typedArray("absent"));
            assertTrue(arrays.group("absent") == null);
        }
    }
}
