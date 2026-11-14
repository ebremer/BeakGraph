package com.ebremer.beakgraph.hdf5;

import com.ebremer.falcon.hdf5.Hdf5Writer;
import com.ebremer.falcon.hdf5.datatype.Datatype;

/**
 * A group of a {@link WritableHdfFile}. It offers exactly what BeakGraph's
 * container profile holds (SPECIFICATIONS.md §3): child groups, 1-D byte
 * datasets, and scalar integer attributes whose HDF5 width is the Java type's
 * ({@code int} is 32-bit, {@code long} 64-bit - the widths §3 item 4 lists).
 *
 * @author Erich Bremer
 */
public final class WritableGroup {

    static final long[] SCALAR = new long[0];

    private final Hdf5Writer.GroupWriter group;

    WritableGroup(Hdf5Writer.GroupWriter group) {
        this.group = group;
    }

    /** Creates (and returns) a child group. */
    public WritableGroup putGroup(String name) {
        return new WritableGroup(group.group(name));
    }

    /**
     * Creates a 1-D dataset of 8-bit signed integers holding {@code data}, in
     * contiguous layout and without filters, as the readers require. The array
     * is kept until the next object is added to the file: do not change it.
     */
    public WritableDataset putDataset(String name, byte[] data) {
        return new WritableDataset(group.byteDataset(name, data, new long[]{data.length}), data.length, data.length);
    }

    /**
     * Creates a 1-D dataset of {@code length} 8-bit signed integers, in
     * contiguous layout and without filters like one made by
     * {@link #putDataset}, whose bytes follow in order, a piece at a time
     * ({@link WritableDataset#write}). Falcon puts each piece in the file as it
     * is given, so a dataset of any size passes through a buffer of the
     * caller's choosing: this is how the disk-based writers
     * ({@code com.ebremer.beakgraph.huge}) hand over arrays that never fit the
     * heap. Bytes never written would read as zeros; write them all.
     *
     * @param length how many bytes the dataset holds; not negative
     */
    public WritableDataset createDataset(String name, long length) {
        return new WritableDataset(group.createDataset(name, Datatype.int8(), length), length, 0);
    }

    /** Writes a scalar 32-bit signed integer attribute. */
    public void putAttribute(String name, int value) {
        group.intAttribute(name, new int[]{value}, SCALAR);
    }

    /** Writes a scalar 64-bit signed integer attribute. */
    public void putAttribute(String name, long value) {
        group.attribute(name, Datatype.int64(), SCALAR, new long[]{value});
    }
}
