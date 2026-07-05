package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
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

    /** Exact quad count of {@code graph}, or -1 when not index-answerable. */
    public static long quads(HDF5Reader reader, Node graph) {
        long gi = resolveGraph(reader, graph);
        if (gi == UNSUPPORTED) return -1;
        IndexReader gspo = reader.getIndexReader(Index.GSPO);
        if (gspo == null) return -1;
        long[] s = firstLevelRange(gspo, 'S', gi);
        if (s == null) return 0;
        // Predicate positions spanned by subjects [sStart..sEnd]: blocks are contiguous,
        // so the range runs from subject sStart's first predicate to the position just
        // before subject (sEnd+1)'s first predicate - and identically P -> O below.
        long[] p = childRange(gspo, 'P', s[0], s[1]);
        if (p == null) return 0;
        long[] o = childRange(gspo, 'O', p[0], p[1]);
        if (o == null) return 0;
        return o[1] - o[0] + 1;
    }

    /** Exact {@code COUNT(DISTINCT ?s)} over {@code { ?s ?p ?o }} in {@code graph}, or -1. */
    public static long distinctSubjects(HDF5Reader reader, Node graph) {
        return firstLevelCount(reader, graph, Index.GSPO, 'S');
    }

    /** Exact {@code COUNT(DISTINCT ?p)} over {@code { ?s ?p ?o }} in {@code graph}, or -1. */
    public static long distinctPredicates(HDF5Reader reader, Node graph) {
        return firstLevelCount(reader, graph, Index.GPOS, 'P');
    }

    private static long firstLevelCount(HDF5Reader reader, Node graph, Index index, char component) {
        long gi = resolveGraph(reader, graph);
        if (gi == UNSUPPORTED) return -1;
        IndexReader ir = reader.getIndexReader(index);
        if (ir == null) return -1;
        long[] range = firstLevelRange(ir, component, gi);
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

    /**
     * The inclusive position range of graph {@code gi}'s block at the index's first
     * level, or null when the graph has no rows (absent id or padding-only block).
     */
    private static long[] firstLevelRange(IndexReader ir, char component, long gi) {
        if (gi < 1) return null;
        BitPackedUnSignedLongBuffer bitmap = ir.getBitmapBuffer(component);
        BitPackedUnSignedLongBuffer ids = ir.getIDBuffer(component);
        long start = select1(ir.getDirectory(component), bitmap, gi);
        if (start == -1) return null;
        long next = select1(ir.getDirectory(component), bitmap, gi + 1);
        long end = (next == -1) ? ids.getNumEntries() - 1 : next - 1;
        if (start > end) return null;
        if (ids.get(start) == 0) return null; // padding row: the graph is empty
        return new long[]{start, end};
    }

    /**
     * The inclusive position range at a child level spanned by parent positions
     * [{@code parentStart}..{@code parentEnd}]: parent position i's child block
     * starts at the (i+1)-th set bit (the addressing every BGIterator uses).
     */
    private static long[] childRange(IndexReader ir, char component, long parentStart, long parentEnd) {
        BitPackedUnSignedLongBuffer bitmap = ir.getBitmapBuffer(component);
        BitPackedUnSignedLongBuffer ids = ir.getIDBuffer(component);
        long start = select1(ir.getDirectory(component), bitmap, parentStart + 1);
        if (start == -1) return null;
        long next = select1(ir.getDirectory(component), bitmap, parentEnd + 2);
        long end = (next == -1) ? ids.getNumEntries() - 1 : next - 1;
        if (start > end) return null;
        return new long[]{start, end};
    }

    private static long select1(HDTBitmapDirectory dir, BitPackedUnSignedLongBuffer fallback, long rank) {
        if (rank < 1) return -1;
        return (dir != null) ? dir.select1(rank) : fallback.select1(rank);
    }
}
