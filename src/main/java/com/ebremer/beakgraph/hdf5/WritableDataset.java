package com.ebremer.beakgraph.hdf5;

import com.ebremer.falcon.hdf5.Hdf5Writer;
import com.ebremer.falcon.hdf5.datatype.Datatype;
import java.util.Arrays;
import java.util.Objects;

/**
 * A dataset of a {@link WritableHdfFile}: its scalar integer attributes, whose
 * HDF5 width is the Java type's ({@code int} is 32-bit, {@code long} 64-bit),
 * and - for a dataset made with its length alone
 * ({@link WritableGroup#createDataset}) - its bytes, written in order, a piece
 * at a time. A dataset made by {@link WritableGroup#putDataset} was given its
 * bytes whole and takes no more.
 *
 * @author Erich Bremer
 */
public final class WritableDataset {

    private final Hdf5Writer.DatasetWriter dataset;
    /** How many bytes the dataset holds. */
    private final long length;
    /** How many of them have been written: all of them, for a dataset given its bytes whole. */
    private long written;

    WritableDataset(Hdf5Writer.DatasetWriter dataset, long length, long written) {
        this.dataset = dataset;
        this.length = length;
        this.written = written;
    }

    /**
     * Appends {@code len} bytes of {@code buf}, from {@code off}: they go to
     * the file now, at their place in the dataset, and {@code buf} is free to
     * be reused when this returns.
     *
     * @throws IllegalStateException if the dataset would then hold more bytes
     *         than it was made for (one given its bytes whole takes none)
     */
    public void write(byte[] buf, int off, int len) {
        Objects.checkFromIndexSize(off, len, buf.length);
        if (len > length - written) {
            throw new IllegalStateException((written + len) + " bytes written to a dataset of " + length);
        }
        if (len == 0) {
            return;
        }
        // Falcon takes a piece as an array of exactly its bytes.
        byte[] piece = (off == 0 && len == buf.length) ? buf : Arrays.copyOfRange(buf, off, off + len);
        dataset.writeRaw(new long[]{written}, new long[]{len}, piece);
        written += len;
    }

    /** How many bytes the dataset holds once it is complete. */
    public long length() {
        return length;
    }

    /** How many of its bytes have been written so far. */
    public long written() {
        return written;
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
