package com.ebremer.beakgraph.store;

/**
 * Streams one typed array of {@code /.BG/arrays} (format v7, RDFARRY.md §3.5)
 * by whole rows, for arrays too large to hold at once: {@link #append} takes
 * the next rows (along the first dimension), row-major, in the array's Java
 * element type ({@link TypedArray}'s), and {@link #close} ends the array once
 * every row is written. A writer holds at most about one chunk of rows.
 */
public interface TypedArrayWriter extends AutoCloseable {

    /**
     * Appends the next whole rows.
     *
     * @param values {@code double[]}, {@code float[]}, {@code long[]}, {@code int[]},
     *               {@code boolean[]} or {@code String[]}, as the array's type says, holding a
     *               whole number of rows
     * @throws IllegalArgumentException if they are not whole rows, or more than the shape holds
     */
    void append(Object values);

    /**
     * Ends the array.
     *
     * @throws IllegalStateException if fewer rows were appended than its shape has
     */
    @Override
    void close();
}
