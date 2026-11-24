package com.ebremer.beakgraph.store;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * One typed N-D array of {@code /.BG/arrays} (format v7, RDFARRY.md §3.5): the
 * only arrays of a store whose element type a reader honours. The values are
 * row-major in a Java array of the element type, and both containers store
 * them little-endian, as numpy reads them natively. Attributes are strings or
 * integers; the dimension names label the axes where the container can
 * (Zarr's {@code dimension_names}).
 *
 * @param values {@code double[]}, {@code float[]}, {@code long[]}, {@code int[]} (also for
 *               {@code U16}, each 0 to 65535), {@code boolean[]} or {@code String[]} (no nulls), as
 *               {@code type} says, holding the product of {@code shape} elements
 */
public record TypedArray(Type type, long[] shape, Object values, List<String> dimensionNames,
                         Map<String, Object> attributes) {

    /**
     * The element types: XSD's double, float, long, int and boolean; uint16
     * (language codes of record classes); and variable-length UTF-8 strings,
     * whose {@link #bytes} is a size estimate (an HDF5 variable-length id).
     */
    public enum Type {
        F64(8), F32(4), I64(8), I32(4), BOOL(1), U16(2), STR(16);

        private final int bytes;

        Type(int bytes) {
            this.bytes = bytes;
        }

        /** The bytes of one element. */
        public int bytes() {
            return bytes;
        }
    }

    public TypedArray {
        Objects.requireNonNull(type, "type");
        shape = shape.clone();
        long n = elementCount(shape);
        int length = checkedLength(type, values);
        if (length != n) {
            throw new IllegalArgumentException(type + " values hold " + length + " elements; the shape "
                    + java.util.Arrays.toString(shape) + " needs " + n);
        }
        dimensionNames = checkedDimensionNames(dimensionNames, shape.length);
        attributes = checkedAttributes(attributes);
    }

    /** The product of {@code shape}; IllegalArgumentException for a negative dimension. */
    public static long elementCount(long[] shape) {
        long n = 1;
        for (long d : shape) {
            if (d < 0) {
                throw new IllegalArgumentException("negative dimension in " + java.util.Arrays.toString(shape));
            }
            n = Math.multiplyExact(n, d);
        }
        return n;
    }

    /** The elements below the first dimension: one row's. */
    public static long rowElements(long[] shape) {
        long n = 1;
        for (int i = 1; i < shape.length; i++) {
            n = Math.multiplyExact(n, shape[i]);
        }
        return n;
    }

    /** How many elements {@code values} holds, checked against {@code type}'s Java array and range. */
    public static int checkedLength(Type type, Object values) {
        int length = switch (type) {
            case F64 -> ((double[]) values).length;
            case F32 -> ((float[]) values).length;
            case I64 -> ((long[]) values).length;
            case I32, U16 -> ((int[]) values).length;
            case BOOL -> ((boolean[]) values).length;
            case STR -> ((String[]) values).length;
        };
        if (type == Type.U16) {
            for (int v : (int[]) values) {
                if (v < 0 || v > 0xFFFF) {
                    throw new IllegalArgumentException("uint16 value " + v);
                }
            }
        } else if (type == Type.STR) {
            for (String v : (String[]) values) {
                Objects.requireNonNull(v, "a string element");
            }
        }
        return length;
    }

    /** A new Java array of {@code type} for {@code n} elements ({@code ""} for strings). */
    public static Object newValues(Type type, int n) {
        return switch (type) {
            case F64 -> new double[n];
            case F32 -> new float[n];
            case I64 -> new long[n];
            case I32, U16 -> new int[n];
            case BOOL -> new boolean[n];
            case STR -> {
                String[] s = new String[n];
                java.util.Arrays.fill(s, "");
                yield s;
            }
        };
    }

    /** Elements {@code [from, to)} of {@code values}, a copy. */
    public static Object slice(Type type, Object values, int from, int to) {
        return switch (type) {
            case F64 -> java.util.Arrays.copyOfRange((double[]) values, from, to);
            case F32 -> java.util.Arrays.copyOfRange((float[]) values, from, to);
            case I64 -> java.util.Arrays.copyOfRange((long[]) values, from, to);
            case I32, U16 -> java.util.Arrays.copyOfRange((int[]) values, from, to);
            case BOOL -> java.util.Arrays.copyOfRange((boolean[]) values, from, to);
            case STR -> java.util.Arrays.copyOfRange((String[]) values, from, to);
        };
    }

    /** {@code names} as an immutable list, null kept, checked against the rank. */
    public static List<String> checkedDimensionNames(List<String> names, int rank) {
        if (names == null) {
            return null;
        }
        if (names.size() != rank) {
            throw new IllegalArgumentException(names.size() + " dimension names for rank " + rank);
        }
        return List.copyOf(names);
    }

    /** {@code attributes} in name order, integers as longs; IllegalArgumentException for any other value. */
    public static Map<String, Object> checkedAttributes(Map<String, Object> attributes) {
        Map<String, Object> sorted = new TreeMap<>();
        if (attributes != null) {
            for (Map.Entry<String, Object> e : attributes.entrySet()) {
                Object v = e.getValue();
                if (v instanceof Integer i) {
                    v = i.longValue();
                }
                if (!(v instanceof String || v instanceof Long)) {
                    throw new IllegalArgumentException("attribute '" + e.getKey() + "' must be a string or an integer");
                }
                sorted.put(e.getKey(), v);
            }
        }
        return java.util.Collections.unmodifiableMap(sorted);
    }

    @Override
    public long[] shape() {
        return shape.clone();
    }

    /** The product of the shape: how many elements the array holds. */
    public long elementCount() {
        long n = 1;
        for (long d : shape) {
            n *= d;
        }
        return n;
    }

    /** The bytes of one row: the elements below the first dimension. */
    public long rowBytes() {
        long n = type.bytes();
        for (int i = 1; i < shape.length; i++) {
            n *= Math.max(1, shape[i]);
        }
        return n;
    }

    /** Its type and shape, for messages. */
    @Override
    public String toString() {
        return "TypedArray[" + type + " " + java.util.Arrays.toString(shape) + "]";
    }
}
