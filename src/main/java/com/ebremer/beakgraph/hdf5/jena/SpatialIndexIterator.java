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
import org.apache.jena.atlas.iterator.Iter;
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
 * are a superset (bbox-cover overlap), never a miss. The geof:sfIntersects
 * filter stays in the plan and removes the false positives with real JTS
 * geometry. Candidates are deduplicated across scales and ranges, so one
 * geometry yields one row.
 */
public class SpatialIndexIterator implements Iterator<BindingNodeId> {
    private static final Logger logger = LoggerFactory.getLogger(SpatialIndexIterator.class);
    /** Cap on query-side range fragmentation; merging beyond the cap only widens the superset. */
    private static final int MAX_QUERY_RANGES = 256;

    private final Iterator<BindingNodeId> outputIterator;

    public SpatialIndexIterator(Iterator<BindingNodeId> input, BeakGraph bGraph, Var targetVar, PatternMatchBG.SpatialContext context) {
        NodeTable nodeTable = bGraph.getReader().getNodeTable();
        // The candidate set depends only on the (constant) query region, so it is
        // computed once and reused for every incoming row - the previous version
        // re-ran every range sub-query per parent binding.
        final List<Long> candidates;
        Envelope env = queryEnvelope(context.searchRegionWKT);
        if (env != null && bGraph.getReader() instanceof HDF5Reader hdf5) {
            candidates = collectCandidates(hdf5, env);
        } else {
            candidates = List.of();
        }
        logger.trace("sfIntersects region {} -> {} candidate geometries", context.searchRegionWKT, candidates.size());
        this.outputIterator = Iter.flatMap(input, parent ->
            Iter.removeNulls(Iter.map(candidates.iterator(), id -> {
                BindingNodeId child = new BindingNodeId(parent);
                // A conflicting pre-existing binding for the target var drops the row.
                return child.putCompatible(targetVar, new NodeId(id, NodeType.SUBJECT), nodeTable) ? child : null;
            })));
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
    private static List<Long> collectCandidates(HDF5Reader reader, Envelope env) {
        LinkedHashSet<Long> out = new LinkedHashSet<>();
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        if (gpos == null) {
            return List.of();
        }
        NodeTable nodeTable = reader.getNodeTable();
        long minX = Math.max(0, (long) Math.floor(env.getMinX()));
        long maxX = (long) Math.floor(env.getMaxX());
        long minY = Math.max(0, (long) Math.floor(env.getMinY()));
        long maxY = (long) Math.floor(env.getMaxY());
        if (maxX < 0 || maxY < 0) {
            return List.of();
        }
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
                ExprList bounds = new ExprList();
                bounds.add(new E_GreaterThanOrEqual(new ExprVar(oVar),
                        NodeValue.makeNode(NodeFactory.createLiteralByValue(r.low()))));
                bounds.add(new E_LessThanOrEqual(new ExprVar(oVar),
                        NodeValue.makeNode(NodeFactory.createLiteralByValue(r.high()))));
                Quad pattern = new Quad(Params.SPATIAL, sVar, pred, oVar);
                BGIteratorPOS it = new BGIteratorPOS(dict, gpos, new BindingNodeId(), pattern, bounds, nodeTable);
                while (it.hasNext()) {
                    NodeId sid = it.next().get(sVar);
                    if (sid != null) {
                        out.add(sid.getId());
                    }
                }
            }
        }
        return new ArrayList<>(out);
    }

    /** Hilbert range cover of the cell box [lo, hi], merged and capped (superset-safe). */
    private static List<Range> coverRanges(long[] lo, long[] hi) {
        ArrayList<Range> ranges = new ArrayList<>();
        for (Range r : HilbertSpace.hc.query(lo, hi)) {
            ranges.add(r);
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
        return outputIterator.hasNext();
    }

    @Override
    public BindingNodeId next() {
        return outputIterator.next();
    }
}
