package com.ebremer.beakgraph.huge;

import java.io.IOException;
import java.util.Iterator;

/**
 * A disk-backed sorter of records of type {@code T}: records stream in via
 * {@link #add}, and {@link #sorted()} - callable once - returns them in order.
 * Implementations may buffer, spill, and merge however they like; RAM must
 * stay bounded regardless of record count.
 *
 * <p>Extracted from {@link ExternalSorter} so the build pipeline can run with
 * pluggable sorting engines: the sequential external merge sort (-method 1)
 * or the hugeUltra package's parallel/primitive sorters (-method 4).
 */
public interface RecordSorter<T> extends AutoCloseable {

    /** A sorted record stream that owns its temp files; must be closed. */
    interface SortedCursor<T> extends Iterator<T>, AutoCloseable {
        @Override
        void close();
    }

    void add(T record) throws IOException;

    /** Total records added so far. */
    long size();

    /** Finishes ingestion and returns the fully sorted stream (single use). */
    SortedCursor<T> sorted() throws IOException;

    @Override
    void close();
}
