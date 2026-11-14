package com.ebremer.beakgraph.hdf5;

/**
 * A byte array of a store being written, in either container: its scalar
 * integer attributes and - for an array made with its length alone
 * ({@link WritableGroup#createDataset}) - its bytes, written in order, a piece
 * at a time. An array made by {@link WritableGroup#putDataset} was given its
 * bytes whole and takes no more.
 *
 * @author Erich Bremer
 */
public interface WritableDataset {

    /**
     * Appends {@code len} bytes of {@code buf}, from {@code off}: they go to
     * the store now, at their place in the array, and {@code buf} is free to
     * be reused when this returns.
     *
     * @throws IllegalStateException if the array would then hold more bytes
     *         than it was made for (one given its bytes whole takes none)
     */
    void write(byte[] buf, int off, int len);

    /** How many bytes the array holds once it is complete. */
    long length();

    /** How many of its bytes have been written so far. */
    long written();

    /** Writes a scalar integer attribute (32-bit in HDF5). */
    void putAttribute(String name, int value);

    /** Writes a scalar integer attribute (64-bit in HDF5). */
    void putAttribute(String name, long value);
}
