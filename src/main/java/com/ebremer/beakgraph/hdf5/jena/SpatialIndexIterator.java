package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import static com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder.HILBERT_CELL_NS;
import static com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder.MAX_INDEX_SCALE;
import com.ebremer.beakgraph.utils.ImageTools;
import com.ebremer.halcyon.hilbert.HilbertSpace;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.davidmoten.hilbert.Range;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recall-safe spatial candidate generator. The writer stores, per geometry
 * part, the Hilbert indices of the whole cells covering its bbox at the
 * coarsest scale where that cover is small (hal:hilbertCell{scale}). This
 * iterator covers the QUERY region's bbox with Hilbert ranges at every scale -
 * with the same floor snapping the writer uses - and collects every subject
 * whose stored cell falls inside a range, by scanning the GPOS index directly
 * with value-range pushdown (no SPARQL sub-execution, no leaked executions).
 * <p>
 * Two bboxes that overlap share at least one whole cell at any scale, so a
 * stored geometry overlapping the query region is ALWAYS produced: candidates
 * are a superset (bbox-cover overlap), never a miss. Both sides clamp their
 * bbox into the non-negative Hilbert domain with the same monotone projection,
 * which preserves that overlap for coordinates in negative space too (they
 * cluster on the axis cells - coarse, but recall-safe). The geof:sfIntersects
 * filter stays in the plan and removes the false positives with real JTS
 * geometry. Candidates are deduplicated across scales and ranges, so one
 * geometry yields one row.
 * <p>
 * The candidate collection - up to 31 scales x 256 ranges of GPOS scans - runs
 * at the FIRST {@link #hasNext()}, not in the constructor, and checks the
 * execution's cancel signal between ranges and every {@value #CANCEL_CHECK_ROWS}
 * rows, throwing {@link QueryCancelledException}. Constructing the iterator
 * is free, so the abortable wrappers the solver installs around the chain
 * are in place before any work starts and a timeout or abort stops the
 * collection instead of waiting for it (BG-71).
 */
public class SpatialIndexIterator implements Iterator<BindingNodeId> {
    private static final Logger logger = LoggerFactory.getLogger(SpatialIndexIterator.class);
    /** Cap on query-side range fragmentation; merging beyond the cap only widens the superset. */
    private static final int MAX_QUERY_RANGES = 256;
    /**
     * Cap on the cell-box perimeter handed to the curve's range query. The
     * davidmoten query is the perimeter algorithm - it materialises one boxed
     * index per perimeter cell and sorts them - so a region that clamps to the
     * whole [0, 2^31) domain would walk ~2^33 cells at scale 0: tens of
     * gigabytes and hours, inside the iterator constructor, before any
     * cancel or timeout check. Boxes over the budget are covered on a coarser
     * block curve instead (see {@link #coverRanges}). 2^14 keeps a scale's
     * query at a few thousand cells per side and a few milliseconds.
     */
    static final long PERIMETER_BUDGET = 1L << 14;
    /** Rows drained between cancel checks inside one range scan. */
    static final int CANCEL_CHECK_ROWS = 4096;

    private final Iterator<BindingNodeId> input;
    private final BeakGraph bGraph;
    private final Var targetVar;
    private final PatternMatchBG.SpatialContext context;
    private final AtomicBoolean cancelSignal; // the execution's; may be null
    private Iterator<BindingNodeId> outputIterator; // built at the first hasNext()

    /** Number of seeded chains built - tests pin that the index is used, not just the JTS filter. */
    public static final java.util.concurrent.atomic.AtomicLong HITS = new java.util.concurrent.atomic.AtomicLong();

    /**
     * @param cancelSignal the execution's cancel signal ({@code ExecutionContext.getCancelSignal()}),
     *                     or null when there is none; consulted while collecting candidates
     */
    public SpatialIndexIterator(Iterator<BindingNodeId> input, BeakGraph bGraph, Var targetVar,
                                PatternMatchBG.SpatialContext context, AtomicBoolean cancelSignal) {
        HITS.incrementAndGet();
        this.input = input;
        this.bGraph = bGraph;
        this.targetVar = targetVar;
        this.context = context;
        this.cancelSignal = cancelSignal;
    }

    private Iterator<BindingNodeId> output() {
        Iterator<BindingNodeId> out = outputIterator;
        if (out == null) {
            NodeTable nodeTable = bGraph.getReader().getNodeTable();
            // The candidate set depends only on the (constant) query region, so it is
            // computed once and reused for every incoming row - the previous version
            // re-ran every range sub-query per parent binding.
            final List<Long> candidates;
            Envelope env = queryEnvelope(context.searchRegionWKT);
            if (env != null && bGraph.getReader() instanceof HDF5Reader hdf5) {
                candidates = collectCandidates(hdf5, env, cancelSignal);
            } else {
                candidates = List.of();
            }
            logger.trace("sfIntersects region {} -> {} candidate geometries", context.searchRegionWKT, candidates.size());
            out = Iter.flatMap(input, parent ->
                Iter.removeNulls(Iter.map(candidates.iterator(), id -> {
                    BindingNodeId child = new BindingNodeId(parent);
                    // A conflicting pre-existing binding for the target var drops the row.
                    return child.putCompatible(targetVar, NodeId.pack(NodeType.SUBJECT, id), nodeTable) ? child : null;
                })));
            outputIterator = out;
        }
        return out;
    }

    private static void checkCancelled(AtomicBoolean cancelSignal) {
        if (cancelSignal != null && cancelSignal.get()) {
            throw new QueryCancelledException();
        }
    }

    /**
     * Whether this store carries the Hilbert cell index the seeding relies on:
     * an HDF5 reader with a GPOS index, the {@code urn:x-beakgraph:Spatial}
     * graph, and at least one {@code hal:hilbertCell{scale}} predicate. Stores
     * built without {@code setSpatial(true)} (the CLI default) have none of
     * these; seeding them would replace the input chain with an EMPTY candidate
     * set and silently answer every sfIntersects query with zero rows. Callers
     * must leave the chain untouched instead, so the geof:sfIntersects OpFilter
     * alone answers the query with real JTS geometry - slower, never wrong.
     */
    static boolean isAvailable(BeakGraph bGraph) {
        if (!(bGraph.getReader() instanceof HDF5Reader hdf5)) {
            return false;
        }
        if (hdf5.getIndexReader(Index.GPOS) == null) {
            return false;
        }
        PositionalDictionaryReader dict = (PositionalDictionaryReader) hdf5.getDictionary();
        if (dict.getGraphs().locate(Params.SPATIAL) < 1) {
            return false;
        }
        for (int s = 0; s <= MAX_INDEX_SCALE; s++) {
            if (dict.getPredicates().locate(NodeFactory.createURI(HILBERT_CELL_NS + s)) >= 1) {
                return true;
            }
        }
        return false;
    }

    private static Envelope queryEnvelope(String wkt) {
        try {
            Geometry g = new WKTReader().read(ImageTools.stripCrs(wkt));
            return g.isEmpty() ? null : g.getEnvelopeInternal();
        } catch (Exception e) {
            logger.warn("Unparseable sfIntersects search region; no index candidates: {}", e.getMessage());
            return null;
        }
    }

    /** Distinct entity ids of subjects whose stored bbox cells overlap the region's cover. */
    private static List<Long> collectCandidates(HDF5Reader reader, Envelope env, AtomicBoolean cancelSignal) {
        LinkedHashSet<Long> out = new LinkedHashSet<>();
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        if (gpos == null) {
            return List.of();
        }
        NodeTable nodeTable = reader.getNodeTable();
        // Clamp into the [0, 2^31) Hilbert domain with the SAME monotone
        // projection the writer applies to geometry bboxes: two boxes that
        // overlap still overlap after clamping, so a query region in negative
        // (or beyond-2^31) space finds the equally clamped geometries there via
        // the domain-edge cells instead of silently returning nothing. False
        // positives are removed by the sfIntersects verification stage.
        long minX = HilbertSpace.clampToDomain((long) Math.floor(env.getMinX()));
        long maxX = HilbertSpace.clampToDomain((long) Math.floor(env.getMaxX()));
        long minY = HilbertSpace.clampToDomain((long) Math.floor(env.getMinY()));
        long maxY = HilbertSpace.clampToDomain((long) Math.floor(env.getMaxY()));
        Var sVar = Var.alloc("hilbertCellSubject");
        Var oVar = Var.alloc("hilbertCellValue");
        for (int s = 0; s <= MAX_INDEX_SCALE; s++) {
            Node pred = NodeFactory.createURI(HILBERT_CELL_NS + s);
            if (dict.getPredicates().locate(pred) < 1) {
                continue; // no geometry stored at this scale
            }
            long cell = 1L << s;
            long[] lo = {Math.floorDiv(minX, cell), Math.floorDiv(minY, cell)};
            long[] hi = {Math.floorDiv(maxX, cell), Math.floorDiv(maxY, cell)};
            for (Range r : coverRanges(lo, hi)) {
                checkCancelled(cancelSignal);
                ExprList bounds = new ExprList();
                bounds.add(new E_GreaterThanOrEqual(new ExprVar(oVar),
                        NodeValue.makeNode(NodeFactory.createLiteralByValue(r.low()))));
                bounds.add(new E_LessThanOrEqual(new ExprVar(oVar),
                        NodeValue.makeNode(NodeFactory.createLiteralByValue(r.high()))));
                Quad pattern = new Quad(Params.SPATIAL, sVar, pred, oVar);
                BGIteratorPOS it = new BGIteratorPOS(dict, gpos, new BindingNodeId(), pattern, bounds, nodeTable);
                int rows = 0;
                while (it.hasNext()) {
                    long sid = it.next().get(sVar);
                    if (sid != NodeId.NONE) {
                        out.add(NodeId.id(sid));
                    }
                    if (++rows == CANCEL_CHECK_ROWS) {
                        rows = 0;
                        checkCancelled(cancelSignal);
                    }
                }
            }
        }
        return new ArrayList<>(out);
    }

    /**
     * Hilbert range cover of the cell box [lo, hi], merged and capped
     * (superset-safe). A box whose perimeter exceeds {@link #PERIMETER_BUDGET}
     * is covered on the curve over its 2^k-aligned blocks, with k the smallest
     * shift that brings the block box under budget, and each block range is
     * scaled back to cell indices. The curve is self-similar, so the block
     * cover is an exact superset of the cell cover: recall is unchanged, the
     * extra candidates are removed by the sfIntersects verification, and the
     * work is bounded by the budget whatever the region's extent.
     */
    static List<Range> coverRanges(long[] lo, long[] hi) {
        int k = 0;
        long[] l = lo.clone();
        long[] h = hi.clone();
        while (2 * ((h[0] - l[0] + 1) + (h[1] - l[1] + 1)) > PERIMETER_BUDGET) {
            k++;
            l[0] >>= 1;
            l[1] >>= 1;
            h[0] >>= 1;
            h[1] >>= 1;
        }
        ArrayList<Range> ranges = new ArrayList<>();
        for (Range r : HilbertSpace.blocks(k).query(l, h)) {
            ranges.add(k == 0 ? r : new Range(r.low() << (2 * k), ((r.high() + 1) << (2 * k)) - 1));
        }
        ranges.sort((a, b) -> Long.compare(a.low(), b.low()));
        ArrayList<Range> merged = new ArrayList<>();
        Range last = null;
        for (Range r : ranges) {
            if (last != null && r.low() <= last.high() + 1) {
                last = new Range(last.low(), Math.max(last.high(), r.high()));
            } else {
                if (last != null) {
                    merged.add(last);
                }
                last = r;
            }
        }
        if (last != null) {
            merged.add(last);
        }
        if (merged.size() > MAX_QUERY_RANGES) {
            // Collapse to one spanning range: a wider superset costs extra candidate
            // scanning, never recall.
            Range one = new Range(merged.get(0).low(), merged.get(merged.size() - 1).high());
            merged = new ArrayList<>(Collections.singletonList(one));
        }
        return merged;
    }

    @Override
    public boolean hasNext() {
        return output().hasNext();
    }

    @Override
    public BindingNodeId next() {
        return output().next();
    }
}
