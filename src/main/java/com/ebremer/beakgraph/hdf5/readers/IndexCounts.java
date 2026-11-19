package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.jena.RangeSelect;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Exact per-graph counts computed from index structure alone - no row scan.
 *
 * <p>Both indexes group rows per graph at their first level, so a graph's block
 * boundaries (a pair of {@code select1} calls) directly yield:
 * <ul>
 *   <li>{@code distinctSubjects}: the GSPO S-level range under a graph lists each
 *       of the graph's subjects exactly once (sorted) - its size IS
 *       {@code COUNT(DISTINCT ?s)}.</li>
 *   <li>{@code distinctPredicates}: likewise the GPOS P-level range.</li>
 *   <li>{@code quads}: descending GSPO's three levels to the graph's O-level range
 *       gives the graph's row count - and index rows are de-duplicated quads, so
 *       this IS {@code COUNT(*)} over {@code ?s ?p ?o} (and the graph's triple
 *       count, since triples are unique within a graph).</li>
 * </ul>
 *
 * <p>The writer pads each empty first-level slot with a single all-zero dummy row
 * so {@code select1(id)} addresses every id directly; a range whose first value is
 * 0 is therefore an empty graph, not one row. Every method returns -1 when the
 * answer is not directly computable - union graph (cross-graph de-duplication) or
 * a missing index - and callers fall back to scanning.
 */
public final class IndexCounts {

    private IndexCounts() {}

    /** Exact quad count of {@code graph} from GSPO, or -1 when not index-answerable. */
    public static long quads(HDF5Reader reader, Node graph) {
        return quads(reader, graph, Index.GSPO);
    }

    /**
     * Exact quad count of {@code graph} descending the three levels of
     * {@code index}, or -1 when not index-answerable. Both indexes hold the
     * same rows, so GSPO and GPOS must agree; {@code -verify -deep} compares
     * them (BG-147).
     */
    public static long quads(HDF5Reader reader, Node graph, Index index) {
        long gi = resolveGraph(reader, graph);
        if (gi == UNSUPPORTED) return -1;
        return quads(reader, gi, index);
    }

    /** As {@link #quads(HDF5Reader, Node)}, for the graph with dictionary id {@code gi} (a walk over the graph list). */
    public static long quads(HDF5Reader reader, long gi) {
        return quads(reader, gi, Index.GSPO);
    }

    private static long quads(HDF5Reader reader, long gi, Index index) {
        IndexReader ir = reader.getIndexReader(index);
        if (ir == null) return -1;
        String order = index.name();
        long[] l1 = RangeSelect.firstLevelRange(ir, order.charAt(1), gi);
        if (l1 == null) return 0;
        // Child positions spanned by parents [start..end]: blocks are contiguous,
        // so the range runs from parent start's first child to the position just
        // before parent (end+1)'s first child - and identically one level down.
        long[] l2 = RangeSelect.childRange(ir, order.charAt(2), l1[0], l1[1]);
        if (l2 == null) return 0;
        long[] l3 = RangeSelect.childRange(ir, order.charAt(3), l2[0], l2[1]);
        if (l3 == null) return 0;
        return l3[1] - l3[0] + 1;
    }

    /** Exact {@code COUNT(DISTINCT ?s)} over {@code { ?s ?p ?o }} in {@code graph}, or -1. */
    public static long distinctSubjects(HDF5Reader reader, Node graph) {
        return firstLevelCount(reader, graph, Index.GSPO, 'S');
    }

    /** Exact {@code COUNT(DISTINCT ?p)} over {@code { ?s ?p ?o }} in {@code graph}, or -1. */
    public static long distinctPredicates(HDF5Reader reader, Node graph) {
        return firstLevelCount(reader, graph, Index.GPOS, 'P');
    }

    /** As {@link #distinctSubjects(HDF5Reader, Node)}, for the graph with dictionary id {@code gi}. */
    public static long distinctSubjects(HDF5Reader reader, long gi) {
        return firstLevelCount(reader, gi, Index.GSPO, 'S');
    }

    /** As {@link #distinctPredicates(HDF5Reader, Node)}, for the graph with dictionary id {@code gi}. */
    public static long distinctPredicates(HDF5Reader reader, long gi) {
        return firstLevelCount(reader, gi, Index.GPOS, 'P');
    }

    /** What {@link #quadsByPredicate} reports: a predicate's dictionary id and the quads of the graph that have it. */
    @FunctionalInterface
    public interface PredicateCount {
        void accept(long predicateId, long quads);
    }

    /**
     * The quads of {@code graph} predicate by predicate - what
     * {@code SELECT ?p (COUNT(*) AS ?n) { ?s ?p ?o } GROUP BY ?p} asks for -
     * from GPOS alone: a predicate's rows are the S-level entries under all of
     * its objects, and both levels are contiguous, so a predicate's count is
     * the distance between where its rows begin and where the next
     * predicate's do. Two {@code select1} calls a predicate, no row read.
     *
     * @return false when not index-answerable (the union graph, a missing
     *         index): nothing was reported, and the caller scans instead
     */
    public static boolean quadsByPredicate(HDF5Reader reader, Node graph, PredicateCount sink) {
        long gi = resolveGraph(reader, graph);
        if (gi == UNSUPPORTED) return false;
        return quadsByPredicate(reader, gi, sink);
    }

    /** As {@link #quadsByPredicate(HDF5Reader, Node, PredicateCount)}, for the graph with dictionary id {@code gi}. */
    public static boolean quadsByPredicate(HDF5Reader reader, long gi, PredicateCount sink) {
        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        if (gpos == null) return false;
        long[] predicates = RangeSelect.firstLevelRange(gpos, 'P', gi);
        if (predicates == null) return true; // no such graph, or one without rows
        BitPackedUnSignedLongBuffer sp = gpos.getIDBuffer('P');
        long from = firstRow(gpos, predicates[0]);
        for (long p = predicates[0]; p <= predicates[1]; p++) {
            long to = firstRow(gpos, p + 1);
            long pid = sp.get(p);
            if (pid >= 1 && to > from) {
                sink.accept(pid, to - from);
            }
            from = to;
        }
        return true;
    }

    /**
     * Exact number of quads in graph {@code gi} with predicate id {@code pi}
     * and, when {@code oi >= 1}, object id {@code oi} - the matches of
     * {@code ?s <p> <o>} (or {@code ?s <p> ?o}) in that graph - from GPOS
     * alone: the predicate's position by binary search in the graph's P-level
     * block, the object's in the predicate's O-level block (the walk
     * {@code BGIteratorOS} makes), then the size of the rows under it. A few
     * {@code select1} calls and two binary searches, no row read. What the
     * join reorder costs a pattern by.
     *
     * @return the count (0 for an absent graph, predicate or object), or -1
     *         without a GPOS index
     */
    public static long quadsWithPredicate(HDF5Reader reader, long gi, long pi, long oi) {
        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        if (gpos == null) return -1;
        if (pi < 1) return 0;
        long[] predicates = RangeSelect.firstLevelRange(gpos, 'P', gi);
        if (predicates == null) return 0;
        long p = gpos.getIDBuffer('P').binarySearch(predicates[0], predicates[1], pi);
        if (p < 0) return 0;
        if (oi < 1) {
            return firstRow(gpos, p + 1) - firstRow(gpos, p);
        }
        long[] objects = RangeSelect.childRange(gpos, 'O', p, p);
        if (objects == null) return 0;
        long o = gpos.getIDBuffer('O').binarySearch(objects[0], objects[1], oi);
        if (o < 0) return 0;
        long[] rows = RangeSelect.childRange(gpos, 'S', o, o);
        return (rows == null) ? 0 : rows[1] - rows[0] + 1;
    }

    /**
     * The quads of graph {@code gi} with predicate id {@code pi}, and their
     * distinct objects - {@code {rows, objects}} - from GPOS alone: the
     * predicate's position by binary search in the graph's P-level block, its
     * O-level block (one entry per distinct object), and the rows under it.
     * Rows ÷ objects is how many subjects a bound object joins to on average:
     * the fan-out of {@code ?s <p> ?o} once {@code ?o} is bound, which the
     * join reorder needs to see that {@code ?m sockg:of ?param} turns each of
     * 156 parameters into some 20,000 measurements.
     *
     * @return {@code {0, 0}} for an absent graph or predicate, or null without
     *         a GPOS index
     */
    public static long[] predicateRowsAndObjects(HDF5Reader reader, long gi, long pi) {
        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        if (gpos == null) return null;
        long[] none = {0, 0};
        if (pi < 1) return none;
        long[] predicates = RangeSelect.firstLevelRange(gpos, 'P', gi);
        if (predicates == null) return none;
        long p = gpos.getIDBuffer('P').binarySearch(predicates[0], predicates[1], pi);
        if (p < 0) return none;
        long[] objects = RangeSelect.childRange(gpos, 'O', p, p);
        if (objects == null) return none;
        return new long[] {firstRow(gpos, p + 1) - firstRow(gpos, p), objects[1] - objects[0] + 1};
    }

    /**
     * Exact distinct subjects of predicate id {@code pi} in graph {@code gi}:
     * the subjects of its GPOS rows, read and de-duplicated - so for a
     * predicate with few rows; {@link #samplePairs} estimates it for any.
     * Rows ÷ subjects is how many objects a bound subject joins to:
     * {@code ?cell part_of ?organ} gives ubergraph's cell types 66 each,
     * where dividing by every subject in the graph made it 1.
     *
     * @return the count (0 for an absent graph or predicate), or -1 without a
     *         GPOS index
     */
    public static long distinctSubjectsWithPredicate(HDF5Reader reader, long gi, long pi) {
        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        if (gpos == null) return -1;
        if (pi < 1) return 0;
        long[] predicates = RangeSelect.firstLevelRange(gpos, 'P', gi);
        if (predicates == null) return 0;
        long p = gpos.getIDBuffer('P').binarySearch(predicates[0], predicates[1], pi);
        if (p < 0) return 0;
        long[] objects = RangeSelect.childRange(gpos, 'O', p, p);
        if (objects == null) return 0;
        long[] rows = RangeSelect.childRange(gpos, 'S', objects[0], objects[1]);
        if (rows == null) return 0;
        BitPackedUnSignedLongBuffer subjects = gpos.getIDBuffer('S');
        long[] ids = new long[Math.toIntExact(rows[1] - rows[0] + 1)];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = subjects.get(rows[0] + i);
        }
        java.util.Arrays.sort(ids);
        long distinct = 0;
        for (int i = 0; i < ids.length; i++) {
            if (i == 0 || ids[i] != ids[i - 1]) distinct++;
        }
        return distinct;
    }

    /** The contiguous runs of pairs {@link #samplePairs} reads: one page or two each, however large the graph. */
    public static final int SAMPLE_RUN = 64;

    /**
     * A sample of graph {@code gi}'s (subject, predicate) pairs - GSPO's P level
     * under the graph, one entry per distinct pair - from which
     * {@link PairSample#subjects} estimates any predicate's distinct subjects:
     * the share of the pairs that have it, times the pairs. A predicate's pairs
     * ARE its distinct subjects, so this is a share to estimate, not a distinct
     * count. Every pair when there are at most {@code size} (then exact).
     *
     * <p>Otherwise the sample is {@code size / }{@value #SAMPLE_RUN} runs of
     * {@value #SAMPLE_RUN} consecutive pairs, spread evenly over the graph. The
     * level is memory-mapped, so each read of a cold page is a fault: 16,384
     * pairs read one by one took 8 s on the 4-billion-type graph of the OKN
     * store, the same number in runs a few hundred faults.
     *
     * @return the sample (none for an absent graph), or null without a GSPO index
     */
    public static PairSample samplePairs(HDF5Reader reader, long gi, int size) {
        IndexReader gspo = reader.getIndexReader(Index.GSPO);
        if (gspo == null) return null;
        long[] subjects = RangeSelect.firstLevelRange(gspo, 'S', gi);
        long[] pairs = (subjects == null) ? null : RangeSelect.childRange(gspo, 'P', subjects[0], subjects[1]);
        if (pairs == null) return new PairSample(0, new long[0]);
        long n = pairs[1] - pairs[0] + 1;
        BitPackedUnSignedLongBuffer predicates = gspo.getIDBuffer('P');
        long[] sample;
        if (n <= Math.max(size, SAMPLE_RUN)) {
            sample = new long[(int) n];
            for (int i = 0; i < sample.length; i++) {
                sample[i] = predicates.get(pairs[0] + i);
            }
        } else {
            int runs = Math.max(1, size / SAMPLE_RUN);
            sample = new long[runs * SAMPLE_RUN];
            long stride = (n - SAMPLE_RUN) / runs;
            for (int r = 0; r < runs; r++) {
                long from = pairs[0] + r * stride + stride / 2;
                for (int i = 0; i < SAMPLE_RUN; i++) {
                    sample[r * SAMPLE_RUN + i] = predicates.get(from + i);
                }
            }
        }
        java.util.Arrays.sort(sample);
        return new PairSample(n, sample);
    }

    /** A graph's (subject, predicate) pair count and the sorted predicate ids of a systematic sample of them. */
    public record PairSample(long pairs, long[] sample) {

        /** True when every pair was read: then {@link #subjects} is exact. */
        public boolean exact() {
            return sample.length == pairs;
        }

        /**
         * Estimated distinct subjects of predicate id {@code pi}: its share of the
         * sample times the pairs - exact when every pair was read. A predicate the
         * sample missed counts as half a hit.
         */
        public double subjects(long pi) {
            if (sample.length == 0) return 0;
            int lo = java.util.Arrays.binarySearch(sample, pi);
            if (lo < 0) return exact() ? 0 : 0.5 * pairs / sample.length;
            int hi = lo;
            while (lo > 0 && sample[lo - 1] == pi) lo--;
            while (hi + 1 < sample.length && sample[hi + 1] == pi) hi++;
            return (double) (hi - lo + 1) * pairs / sample.length;
        }
    }

    /**
     * The S-level position at which the rows of the predicate at P-level
     * position {@code p} begin; for the position after the last predicate,
     * the S level's length.
     */
    private static long firstRow(IndexReader gpos, long p) {
        return firstChild(gpos, 'S', firstChild(gpos, 'O', p));
    }

    /** Where the children of the entry at {@code parent} begin at level {@code component}: its length for a parent past the last (the addressing of {@code RangeSelect.childRange}). */
    private static long firstChild(IndexReader ir, char component, long parent) {
        long start = RangeSelect.blockStart(ir.getDirectory(component), ir.getBitmapBuffer(component), parent + 1);
        return (start == -1) ? ir.getIDBuffer(component).getNumEntries() : start;
    }

    private static long firstLevelCount(HDF5Reader reader, Node graph, Index index, char component) {
        long gi = resolveGraph(reader, graph);
        if (gi == UNSUPPORTED) return -1;
        return firstLevelCount(reader, gi, index, component);
    }

    private static long firstLevelCount(HDF5Reader reader, long gi, Index index, char component) {
        IndexReader ir = reader.getIndexReader(index);
        if (ir == null) return -1;
        long[] range = RangeSelect.firstLevelRange(ir, component, gi);
        return (range == null) ? 0 : (range[1] - range[0] + 1);
    }

    private static final long UNSUPPORTED = Long.MIN_VALUE;

    /**
     * The graph's id for first-level select1 addressing; 0/-1 for "no such graph"
     * (counts 0), {@link #UNSUPPORTED} for graphs these counts cannot answer.
     */
    private static long resolveGraph(HDF5Reader reader, Node graph) {
        if (graph == null || Quad.isUnionGraph(graph)) {
            return UNSUPPORTED; // union needs cross-graph de-duplication
        }
        Node g = Quad.isDefaultGraph(graph) ? Quad.defaultGraphIRI : graph;
        return reader.getDictionary().getGraphs().locate(g);
    }

}
