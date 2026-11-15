package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans a {@link ParallelScan} for the scan-shaped first pattern of a BGP, or
 * declines (null) so the caller keeps the ordinary sequential path.
 *
 * <p>Both indexes are positional, so a scan splits into independent chunks by
 * position range - each chunk owns complete parent blocks' rows (a subject's
 * whole P/O sub-tree lives with its subject position; an object's subject block
 * with its object position), so chunking never splits or duplicates a row.
 * Eligible shapes, deliberately narrow:
 * <ul>
 *   <li>{@code ?s ?p ?o} (all unbound, distinct) - GSPO full scan chunked by
 *       the graph's subject-level positions ({@link BGIteratorSPO_All}).</li>
 *   <li>{@code ?s <p> ?o} (concrete predicate, s/o unbound distinct) - GPOS
 *       scan chunked by the predicate's object-level positions
 *       ({@link BGIteratorPOS}).</li>
 * </ul>
 * Anything else - union graph (serialized dedup), bound/repeated variables,
 * concrete s/o (index lookups, not scans), sub-threshold ranges, a store read
 * through a channel (HTTP: one lock, one cache) - stays sequential; so does a
 * pattern re-executed per outer row (see {@code PatternMatchBG.solveFirst}).
 * Range FILTERs remain eligible, but only bounds on the CHUNKED position prune
 * chunks (object bounds for the POS route, which narrows each chunk's object
 * slice by binary search; subject bounds for the SPO route); bounds on the
 * other positions are applied per row inside every chunk, so such a filtered
 * scan costs the same total work as the sequential scan, spread across
 * threads. The bounds themselves are resolved once per store and pattern
 * shape ({@link RangeBounds}), not once per chunk.
 *
 * <p>Sizing: parallelize when the chunkable position range is at least
 * {@code -Dbeakgraph.scan.parallel.threshold} (default 65536; 0 or negative
 * disables), splitting into roughly 2x-processors chunks of at least 16k
 * positions each.
 */
final class ScanChunks {

    private static final Logger logger = LoggerFactory.getLogger(ScanChunks.class);
    private static final long MIN_CHUNK = 16_384;

    private ScanChunks() {}

    /** The position range from which a scan is planned in parallel; 0 or less plans none. */
    static long threshold() {
        return Long.getLong("beakgraph.scan.parallel.threshold", 65_536L);
    }

    /** A parallel scan for this pattern, or null to use the sequential path. */
    static ParallelScan tryParallel(BeakGraph bg, BindingNodeId b0, Triple triple, ExprList filter, ExecutionContext execCxt) {
        long threshold = threshold();
        if (threshold <= 0) return null;
        if (!(bg.getReader() instanceof HDF5Reader reader)) return null;
        if (!(reader.getDictionary() instanceof PositionalDictionaryReader dict)) return null;
        Node ng = bg.getNamedGraph();
        if (ng == null || Quad.isUnionGraph(ng) || bg.isGraphSetView()) return null; // union dedup is a serial point
        Node g = Quad.isDefaultGraph(ng) ? Quad.defaultGraphIRI : ng;
        long gi = dict.getGraphs().locate(g);
        if (gi < 1) return null; // absent graph: the sequential path answers empty immediately
        return plan(reader, dict, gi, g, b0, triple, filter, threshold, execCxt);
    }

    /**
     * As {@link #tryParallel(BeakGraph, BindingNodeId, Triple, ExprList, ExecutionContext)},
     * for one graph of a walk over the dataset's named graphs
     * ({@link GraphVarBG}): the graph is known by its id, and {@code b0}
     * already binds {@code graphVar} to it, which is how the chunks' iterators
     * find the graph and why every row carries it.
     *
     * @param threshold {@link #threshold()}, read once for the whole walk
     */
    static ParallelScan tryParallel(HDF5Reader reader, PositionalDictionaryReader dict, long gi, Var graphVar,
            BindingNodeId b0, Triple triple, long threshold, ExecutionContext execCxt) {
        if (threshold <= 0 || gi < 1) return null;
        return plan(reader, dict, gi, graphVar, b0, triple, null, threshold, execCxt);
    }

    /**
     * @param g the graph as the chunks' quad pattern names it: its term, or a
     *          variable {@code b0} binds to graph {@code gi}
     */
    private static ParallelScan plan(HDF5Reader reader, PositionalDictionaryReader dict, long gi, Node g,
            BindingNodeId b0, Triple triple, ExprList filter, long threshold, ExecutionContext execCxt) {
        // A channel-backed (HTTP) store serializes every read behind one lock
        // and one block cache: chunk workers would only contend on it while
        // thrashing the cache across their disjoint regions (BG-247).
        if (reader.isChannelBacked()) return null;

        Node s = triple.getSubject();
        Node p = triple.getPredicate();
        Node o = triple.getObject();
        if (!isUnboundVar(s, b0) || !isUnboundVar(o, b0)) return null;
        if (s.equals(o)) return null;

        NodeTable nodeTable = reader.getNodeTable();

        if (isUnboundVar(p, b0)) {
            if (p.equals(s) || p.equals(o)) return null;
            // Full scan: chunk the graph's GSPO subject-level positions.
            IndexReader gspo = reader.getIndexReader(Index.GSPO);
            if (gspo == null) return null;
            long[] range = levelRange(gspo, 'S', gi);
            if (range == null || range[1] - range[0] + 1 < threshold) return null;
            Quad quad = new Quad(g, s, p, o);
            return build("SPO", range,
                    (lo, hi) -> new BGIteratorSPO_All(dict, gspo, b0, quad, filter, nodeTable, lo, hi),
                    execCxt);
        }

        if (p.isConcrete()) {
            // Predicate scan: chunk the (graph, predicate) GPOS object-level positions.
            IndexReader gpos = reader.getIndexReader(Index.GPOS);
            if (gpos == null) return null;
            long pi = dict.getPredicates().locate(p);
            if (pi < 1) return null;
            long[] pRange = levelRange(gpos, 'P', gi);
            if (pRange == null) return null;
            long pIndex = gpos.getIDBuffer('P').binarySearch(pRange[0], pRange[1], pi);
            if (pIndex < 0) return null;
            BitPackedUnSignedLongBuffer bo = gpos.getBitmapBuffer('O');
            long oStart = RangeSelect.blockStart(gpos.getDirectory('O'), bo, pIndex + 1);
            if (oStart == -1) return null;
            long oEnd = RangeSelect.blockEnd(gpos.getDirectory('O'), bo, pIndex + 1, oStart);
            if (oStart > oEnd || oEnd - oStart + 1 < threshold) return null;
            Quad quad = new Quad(g, s, p, o);
            return build("POS", new long[]{oStart, oEnd},
                    (lo, hi) -> new BGIteratorPOS(dict, gpos, b0, quad, filter, nodeTable, lo, hi),
                    execCxt);
        }

        return null;
    }

    private static boolean isUnboundVar(Node n, BindingNodeId b0) {
        return n.isVariable() && (b0 == null || !b0.containsKey(Var.alloc(n)));
    }

    /** The graph's block at an index's first level, or null when absent / empty / padding (see RangeSelect). */
    private static long[] levelRange(IndexReader ir, char component, long gi) {
        return RangeSelect.firstLevelRange(ir, component, gi);
    }

    private interface ChunkFactory {
        Iterator<BindingNodeId> create(long lo, long hi);
    }

    private static ParallelScan build(String kind, long[] range, ChunkFactory factory, ExecutionContext execCxt) {
        long size = range[1] - range[0] + 1;
        int procs = Runtime.getRuntime().availableProcessors();
        // -Dbeakgraph.scan.parallel.minchunk lowers the floor so a small store
        // still splits into several chunks (the two-scan operator shapes and the
        // cancel path in ParallelScanTest need more than one worker per scan).
        long minChunk = Long.getLong("beakgraph.scan.parallel.minchunk", MIN_CHUNK);
        long chunkSize = Math.max(Math.max(1, minChunk), size / (2L * procs));
        int chunks = (int) Math.min(4L * procs, (size + chunkSize - 1) / chunkSize);
        List<Supplier<Iterator<BindingNodeId>>> suppliers = new ArrayList<>(chunks);
        long per = (size + chunks - 1) / chunks;
        for (int c = 0; c < chunks; c++) {
            long lo = range[0] + c * per;
            long hi = Math.min(range[1], lo + per - 1);
            if (lo > hi) break;
            suppliers.add(() -> factory.create(lo, hi));
        }
        logger.debug("Parallel {} scan: {} positions in {} chunks", kind, size, suppliers.size());
        return new ParallelScan(suppliers, execCxt.getCancelSignal());
    }
}
