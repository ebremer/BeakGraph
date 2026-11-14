package com.ebremer.beakgraph.hdf5;

import com.ebremer.falcon.hdf5.Hdf5Writer;
import com.ebremer.falcon.hdf5.datatype.Datatype;

/**
 * A dataset of a {@link WritableHdfFile}, made by
 * {@link WritableGroup#putDataset}: its scalar integer attributes, whose HDF5
 * width is the Java type's ({@code int} is 32-bit, {@code long} 64-bit).
 *
 * @author Erich Bremer
 */
public final class WritableDataset {

    private final Hdf5Writer.DatasetWriter dataset;

    WritableDataset(Hdf5Writer.DatasetWriter dataset) {
        this.dataset = dataset;
    }

    /** Writes a scalar 32-bit signed integer attribute. */
    public void putAttribute(String name, int value) {
        dataset.intAttribute(name, new int[]{value}, WritableGroup.SCALAR);
    }

    /** Writes a scalar 64-bit signed integer attribute. */
    public void putAttribute(String name, long value) {
        dataset.attribute(name, Datatype.int64(), WritableGroup.SCALAR, new long[]{value});
    }
}
