package com.ebremer.beakgraph.store;

import com.ebremer.beakgraph.io.RandomAccessBytes;

/**
 * A named byte array of an open store (SPECIFICATIONS.md §3): its bytes,
 * read where they lie, and its integer attributes. In HDF5 a contiguous
 * dataset of the file; in Zarr an array of uncompressed bytes.
 *
 * @author Erich Bremer
 */
public interface StoreArray {

    /** The array's path in the store, e.g. {@code /.BG/GSPO/So}. */
    String path();

    /**
     * The array's bytes. Of a store opened from local files they are a view
     * of a memory mapping - nothing is copied or read ahead, and the view
     * lasts until the store is closed; of a store read through a channel or
     * a remote store they are read a range at a time.
     */
    RandomAccessBytes bytes();

    /** Whether the array carries attribute {@code name}. */
    boolean hasAttribute(String name);

    /** Integer attribute {@code name}, whatever width the container stores it in. */
    long attribute(String name);
}
