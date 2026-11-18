package com.ebremer.beakgraph.huge;

import java.io.IOException;
import java.util.List;

/**
 * A {@link RecordSorter} whose sorted records can also be read as ranges of
 * the order, each range by a thread of its own. One merge of all the runs is
 * one thread's work however many cores there are; the ranges of a sorter are
 * merged side by side instead, and what is made of the sorted records - the
 * dictionaries and the ids of a build - is made a range at a time with them.
 *
 * <p>{@link #ranges()} ends ingestion as {@link #sorted()} does, and a sorter
 * is read one way or the other, once.
 */
public interface RangeSorter<T> extends RecordSorter<T> {

    /** Finishes ingestion and returns the sorted records as ranges to open. */
    Ranges<T> ranges() throws IOException;

    /** The sorted records of one sorter. Its methods may be called from several threads at once. */
    interface Ranges<T> {

        /**
         * Records to choose range bounds from: each is the first of a stretch
         * of the sorted records, with that stretch's weight (the bytes it
         * takes in its run, or an estimate of them).
         */
        List<Sample<T>> samples();

        /**
         * The records {@code r} with {@code from <= r < to} in the sorter's
         * order, sorted; a null bound is an open end. The ranges opened for
         * bounds that follow one another hold every record once.
         */
        SortedCursor<T> open(T from, T to) throws IOException;

        /** Ranges that may be open at once: each holds a file per run. */
        int maxOpen();
    }

    /** One record of the sorted stream and the weight of the stretch it starts. */
    record Sample<T>(T record, long weight) {}
}
