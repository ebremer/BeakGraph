package com.ebremer.beakgraph.hdf5;

/**
 * A group of a store being written ({@code com.ebremer.beakgraph.store.WritableStore}),
 * in either container. It offers exactly what a BeakGraph store holds
 * (SPECIFICATIONS.md §3): child groups, 1-D byte arrays, and scalar integer
 * attributes. In HDF5 an attribute's width is the Java type's ({@code int} is
 * 32-bit, {@code long} 64-bit - the widths §3 item 4 lists); in Zarr both
 * are JSON integers.
 *
 * @author Erich Bremer
 */
public interface WritableGroup {

    /** Creates (and returns) a child group. */
    WritableGroup putGroup(String name);

    /**
     * Creates a 1-D array of bytes holding {@code data}, stored as the
     * readers require: contiguous and without filters in HDF5, uncompressed
     * in Zarr. The array may be kept until the next object is added to the
     * store: do not change it.
     */
    WritableDataset putDataset(String name, byte[] data);

    /**
     * Creates a 1-D array of {@code length} bytes, stored like one made by
     * {@link #putDataset}, whose bytes follow in order, a piece at a time
     * ({@link WritableDataset#write}). Each piece goes to the store as it is
     * given, so an array of any size passes through a buffer of the caller's
     * choosing: this is how the disk-based writers
     * ({@code com.ebremer.beakgraph.huge}) hand over arrays that never fit the
     * heap. Write them all: the HDF5 container would read bytes never written
     * as zeros, and the Zarr container refuses to complete a store with an
     * array short of its length.
     *
     * @param length how many bytes the array holds; not negative
     */
    WritableDataset createDataset(String name, long length);

    /** Writes a scalar integer attribute (32-bit in HDF5). */
    void putAttribute(String name, int value);

    /** Writes a scalar integer attribute (64-bit in HDF5). */
    void putAttribute(String name, long value);
}
