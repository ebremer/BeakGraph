package com.ebremer.beakgraph.huge;

import java.io.IOException;

/**
 * A writable HDF5 file whose datasets are populated incrementally (piece by
 * piece) instead of from a single in-memory array.
 *
 * <p>This is the backend seam of the huge (disk-based) BeakGraph writer. jHDF's
 * write API ({@code WritableHdfFile.putDataset(name, data)}) requires the whole
 * dataset in RAM, which is exactly the limit the huge writer removes, so the
 * default implementation is {@link NativeHdf5File}, backed by the HDF Group's
 * native library (https://github.com/HDFGroup/hdf5) which supports writing a
 * dataset in chunks. When jHDF gains equivalent streaming/chunked writing, a
 * jHDF-backed implementation of these interfaces can be swapped in via
 * {@link StreamingHdf5#setProvider} without touching any writer code.
 *
 * <p>Implementations MUST produce files the existing jHDF-based readers can
 * open: 1-D 8-bit datasets with CONTIGUOUS layout (the readers cast to
 * {@code io.jhdf.api.dataset.ContiguousDataset} and memory-map via
 * {@code getBuffer()}), and scalar 32-bit / 64-bit integer attributes that jHDF
 * surfaces as {@link Integer} / {@link Long}.
 *
 * @author Erich Bremer
 */
public interface StreamingHdf5File extends AutoCloseable {

    /** The root group of the file. */
    StreamingHdf5Group rootGroup();

    @Override
    void close() throws IOException;
}
