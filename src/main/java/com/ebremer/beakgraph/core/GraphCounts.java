package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexCounts;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.graph.Node;

/**
 * What the stored graphs a query reads hold, summed over them, read from their indexes
 * ({@link IndexCounts}) - the statistics {@link BGReorderTransform} plans joins with. Each figure
 * is a few {@code select1} calls and binary searches a graph, no row read, except a predicate's
 * distinct subjects ({@link PredicateFigures}, which keeps them for the store's lifetime).
 *
 * <p>Distinct terms are summed graph by graph, so a term in two graphs counts twice; the reads that
 * follow visit the graphs one by one too.
 */
final class GraphCounts implements BGReorderTransform.Counts {

    private static final long[] UNKNOWN = {-1, -1, -1};

    private final HDF5Reader reader;
    private final PositionalDictionaryReader dict;
    private final PredicateFigures figures;
    private final long[] graphIds;
    private final ConcurrentHashMap<Node, long[]> predicates = new ConcurrentHashMap<>();
    private volatile long[] totals;

    GraphCounts(HDF5Reader reader, PositionalDictionaryReader dict, PredicateFigures figures, long[] graphIds) {
        this.reader = reader;
        this.dict = dict;
        this.figures = figures;
        this.graphIds = graphIds;
    }

    @Override
    public long matches(Node predicate, Node object) {
        if (object == null) {
            return predicate(predicate)[0];
        }
        long pi = dict.getPredicates().locate(predicate);
        if (pi < 1) return 0;
        long oi = dict.getObjects().locate(object);
        if (oi < 1) return 0;
        long sum = 0;
        for (long gi : graphIds) {
            long n = IndexCounts.quadsWithPredicate(reader, gi, pi, oi);
            if (n < 0) return -1;
            sum += n;
        }
        return sum;
    }

    /** Sequence patterns' estimates, by predicate, pattern and the pattern's bound variables. */
    private final ConcurrentHashMap<java.util.List<Object>, double[]> sequences = new ConcurrentHashMap<>();

    /**
     * {@code {rows read, rows out}} of a scan of {@code ?s <predicate> <pattern>} over the graphs: the rows
     * whose object is one of the prefix's sequences, counted from GPOS (two binary searches a graph), times
     * the shares {@link com.ebremer.beakgraph.hdf5.jena.SequenceMatcher#estimate} works out from the
     * element index, or guesses without one.
     */
    @Override
    public double[] sequence(Node predicate, Node pattern, java.util.Set<Node> bound) {
        java.util.TreeSet<String> boundHere = new java.util.TreeSet<>();
        for (Node v : bound) {
            if (v.isVariable()) {
                boundHere.add(v.getName());
            }
        }
        return sequences.computeIfAbsent(java.util.List.of(predicate, pattern, boundHere), k -> {
            long pi = dict.getPredicates().locate(predicate);
            if (pi < 1) return new double[] {0, 0};
            var est = com.ebremer.beakgraph.hdf5.jena.SequenceMatcher.estimate(pattern, dict,
                    v -> boundHere.contains(v.getName()));
            if (est == null) return new double[] {0, 0};
            long rows = 0;
            for (long gi : graphIds) {
                long n = IndexCounts.quadsWithPredicateInObjects(reader, gi, pi, est.firstObjectId(), est.lastObjectId());
                if (n < 0) return null;
                rows += n;
            }
            return new double[] {rows * est.readShare(), rows * est.share() * est.placements()};
        });
    }

    @Override
    public long objects(Node predicate) {
        return predicate(predicate)[1];
    }

    @Override
    public long subjects(Node predicate) {
        return predicate(predicate)[2];
    }

    @Override
    public long quads() {
        return totals()[0];
    }

    @Override
    public long subjects() {
        return totals()[1];
    }

    @Override
    public long predicates() {
        return totals()[2];
    }

    /** {@code {rows, distinct objects, distinct subjects}} of {@code predicate} in the graphs, or {@code {-1, -1, -1}}. */
    private long[] predicate(Node predicate) {
        return predicates.computeIfAbsent(predicate, p -> {
            long pi = dict.getPredicates().locate(p);
            if (pi < 1) return new long[] {0, 0, 0};
            long[] sum = new long[3];
            for (long gi : graphIds) {
                long[] f = figures.of(gi, pi);
                if (f == null) return UNKNOWN;
                for (int i = 0; i < 3; i++) {
                    sum[i] += f[i];
                }
            }
            return sum;
        });
    }

    /** {@code {quads, distinct subjects, distinct predicates}} of the graphs, each -1 when not countable. */
    private long[] totals() {
        long[] t = totals;
        if (t == null) {
            long quads = 0, subjects = 0, preds = 0;
            for (long gi : graphIds) {
                long q = IndexCounts.quads(reader, gi);
                long s = IndexCounts.distinctSubjects(reader, gi);
                long p = IndexCounts.distinctPredicates(reader, gi);
                quads = (q < 0 || quads < 0) ? -1 : quads + q;
                subjects = (s < 0 || subjects < 0) ? -1 : subjects + s;
                preds = (p < 0 || preds < 0) ? -1 : preds + p;
            }
            totals = t = new long[] {quads, subjects, preds};
        }
        return t;
    }
}
