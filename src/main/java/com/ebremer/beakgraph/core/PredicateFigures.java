package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexCounts;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A predicate's figures in one stored graph - its rows, distinct objects and distinct subjects -
 * from the indexes, for the join reorder ({@link GraphCounts}). Held by the store's root graph and
 * shared by every view: views are made per query, and the store does not change, so each figure is
 * read once for the store's lifetime.
 *
 * <p>Rows and objects come from GPOS exactly. Distinct subjects are counted exactly - the
 * predicate's subjects read and de-duplicated - up to {@value #EXACT_SUBJECTS} rows; beyond that
 * they are estimated from a sample of the graph's (subject, predicate) pairs
 * ({@link IndexCounts#samplePairs}), taken once per graph.
 */
final class PredicateFigures {

    /** Up to this many rows in a graph, a predicate's distinct subjects are counted exactly. */
    static final int EXACT_SUBJECTS = 1 << 16;

    /** The (subject, predicate) pairs a graph's sample reads. */
    static final int SAMPLE = 1 << 14;

    private final HDF5Reader reader;
    private final ConcurrentHashMap<Long, long[]> figures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, IndexCounts.PairSample> samples = new ConcurrentHashMap<>();

    PredicateFigures(HDF5Reader reader) {
        this.reader = reader;
    }

    /**
     * {@code {rows, distinct objects, distinct subjects}} of predicate id {@code pi} in graph
     * {@code gi} (zeros when either is absent), or null when the store's indexes cannot say.
     */
    long[] of(long gi, long pi) {
        if (gi < 1 || pi < 1) {
            return new long[] {0, 0, 0};
        }
        long[] f = figures.get(key(gi, pi));
        if (f == null) {
            f = read(gi, pi);
            if (f == null) {
                return null;
            }
            figures.putIfAbsent(key(gi, pi), f);
        }
        return f;
    }

    private long[] read(long gi, long pi) {
        long[] ro = IndexCounts.predicateRowsAndObjects(reader, gi, pi);
        if (ro == null) {
            return null;
        }
        long rows = ro[0], objects = ro[1];
        if (rows == 0) {
            return new long[] {0, 0, 0};
        }
        long subjects;
        if (rows <= EXACT_SUBJECTS) {
            subjects = IndexCounts.distinctSubjectsWithPredicate(reader, gi, pi);
        } else {
            IndexCounts.PairSample sample = samples.computeIfAbsent(gi, g -> IndexCounts.samplePairs(reader, g, SAMPLE));
            subjects = (sample == null) ? -1 : Math.round(sample.subjects(pi));
        }
        if (subjects < 0) {
            return null;
        }
        // A subject has at most one row per object: at least rows ÷ objects of them, at most rows.
        long least = Math.max(1, (rows + objects - 1) / objects);
        return new long[] {rows, objects, Math.max(least, Math.min(rows, subjects))};
    }

    private static long key(long gi, long pi) {
        return (gi << 32) ^ pi;
    }
}
