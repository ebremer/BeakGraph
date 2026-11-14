package com.ebremer.beakgraph.huge;

import java.io.IOException;

/**
 * A writable HDF5 file whose datasets are populated incrementally (piece by
 * piece) instead of from a single in-memory array.
 *
 * <p>This is the backend seam of the huge (disk-based) BeakGraph writer. The
 * in-memory writers hand each dataset over whole
 * ({@code WritableGroup.putDataset(name, data)}), which is exactly the limit
 * the huge writer removes. The implementation is {@link FalconHdf5File}:
 * Falcon (https://github.com/ebremer/falcon), the pure-Java HDF5 library of
 * the readers and the in-memory writers, writes a dataset in pieces as well as
 * whole. Another implementation of these interfaces can be swapped in via
 * {@link StreamingHdf5#setProvider} without touching any writer code.
 *
 * <p>Implementations MUST produce files the readers can open: 1-D 8-bit
 * datasets with CONTIGUOUS layout (the readers memory-map each dataset's
 * bytes where they lie in the file), and scalar 32-bit / 64-bit integer
 * attributes (SPECIFICATIONS.md §3).
 *
 * @author Erich Bremer
 */
public interface StreamingHdf5File extends AutoCloseable {

    /** The root group of the file. */
    StreamingHdf5Group rootGroup();

    @Override
    void close() throws IOException;
}
