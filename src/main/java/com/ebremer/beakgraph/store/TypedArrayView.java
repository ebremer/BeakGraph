package com.ebremer.beakgraph.store;

/**
 * A typed array of {@code /.BG/arrays} (format v7, RDFARRY.md §3.6) read a
 * box of rows at a time, never whole: what a reader uses to take one
 * sequence's values out of a class too large to load. Raw arrays are read by
 * ranges; chunked ones decode a chunk once and keep it in the container
 * library's chunk cache. Safe to read from several threads.
 */
public interface TypedArrayView {

    /** The element type. */
    TypedArray.Type type();

    /** The shape; the first dimension is the rows. */
    long[] shape();

    /**
     * Rows {@code [first, first + count)}, row-major, in the type's Java array
     * ({@link TypedArray}'s).
     *
     * @throws IndexOutOfBoundsException if the rows lie outside the array
     */
    Object readRows(long first, long count);

    /** {@code shape} and the box of {@code count} rows from {@code first}: the selection's offset and count. */
    static long[][] box(long[] shape, long first, long count) {
        if (first < 0 || count < 0 || first + count > shape[0]) {
            throw new IndexOutOfBoundsException("rows [" + first + ", " + (first + count) + ") of "
                    + java.util.Arrays.toString(shape));
        }
        long[] offset = new long[shape.length];
        offset[0] = first;
        long[] counts = shape.clone();
        counts[0] = count;
        return new long[][]{offset, counts};
    }
}
