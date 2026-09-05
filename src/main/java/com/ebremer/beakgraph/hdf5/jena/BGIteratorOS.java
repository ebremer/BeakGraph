package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;

/**
 * Iterator for GPOS index where G, P, and O are bound, finding S.
 * Structure: Graph -> Predicate -> Object -> Subject
 */
public class BGIteratorOS implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final BitPackedUnSignedLongBuffer Ss;
    private long i;
    private long j;
    private long gi, pi, oi;
    private boolean hasNext = false;
    private boolean subBound = false;
    private long minSubId = 1; // Updated to 1 to gracefully skip dummy IDs
    private long maxSubId = Long.MAX_VALUE;

    // Row-emission plan, computed once: G/P/O packed ids are constant, only S varies.
    private Var gVar, pVar, oVar, sVar;
    private long gId, pId, oId;

    public BGIteratorOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;

        // GPOS Structure
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        BitPackedUnSignedLongBuffer Bo = reader.getBitmapBuffer('O');
        BitPackedUnSignedLongBuffer So = reader.getIDBuffer('O');
        BitPackedUnSignedLongBuffer Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');

        HDTBitmapDirectory dirP = reader.getDirectory('P');
        HDTBitmapDirectory dirO = reader.getDirectory('O');
        HDTBitmapDirectory dirS = reader.getDirectory('S');

        if (filter != null && !filter.isEmpty()) analyzeFilters(filter, dict, quad);

        // Resolve Graph
        if (quad.getGraph().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getGraph())) : NodeId.NONE;
            if (bound == NodeId.NONE) return;
            gi = NodeId.id(bound);
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) return;

        // Resolve Predicate
        if (quad.getPredicate().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getPredicate())) : NodeId.NONE;
            if (bound == NodeId.NONE) return;
            pi = NodeId.id(bound);
        } else {
            pi = dict.getPredicates().locate(quad.getPredicate());
        }
        if (pi < 1) return;

        // Resolve Object
        if (quad.getObject().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getObject())) : NodeId.NONE;
            if (bound == NodeId.NONE) return;
            oi = NodeId.id(bound);
        } else {
            oi = dict.getObjects().locate(quad.getObject());
        }
        if (oi < 1) return;

        // Resolve Subject Filter
        long specificSubId = -1;
        if (quad.getSubject().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getSubject())) : NodeId.NONE;
            subBound = (bound != NodeId.NONE);
            if (subBound) specificSubId = NodeId.id(bound);
        } else {
            subBound = true;
            specificSubId = dict.getSubjects().locate(quad.getSubject());
        }
        if (subBound && specificSubId < 1) return;

        // --- Traverse GPOS ---

        // A. Level 2: Predicate Range for G
        long pStart = RangeSelect.blockStart(dirP, Bp, gi);
        if (pStart == -1) return;
        long pEnd = RangeSelect.blockEnd(dirP, Bp, gi, pStart);
        if (pStart > pEnd) return;

        long pIndex = Sp.binarySearch(pStart, pEnd, pi);
        if (pIndex < 0) return;

        // C. Level 3: Object Range for P
        long oStart = RangeSelect.blockStart(dirO, Bo, pIndex + 1);
        if (oStart == -1) return;
        long oEnd = RangeSelect.blockEnd(dirO, Bo, pIndex + 1, oStart);
        if (oStart > oEnd) return;

        long oIndex = So.binarySearch(oStart, oEnd, oi);
        if (oIndex < 0) return;

        // E. Level 4: Subject Range for O
        long sStart = RangeSelect.blockStart(dirS, Bs, oIndex + 1);
        if (sStart == -1) return;
        long sEnd = RangeSelect.blockEnd(dirS, Bs, oIndex + 1, sStart);
        if (sStart > sEnd) return;

        this.i = sStart;
        this.j = sEnd + 1;

        // F. Initialize
        if (specificSubId > 0) {
            // The subject list under one (G,P,O) group is sorted ascending (GPOS
            // ordering), so binary-search it instead of the previous linear scan.
            long lo = i, hi = j - 1, found = -1;
            while (lo <= hi) {
                long mid = (lo + hi) >>> 1;
                long v = Ss.get(mid);
                if (v == specificSubId) { found = mid; break; }
                if (v < specificSubId) lo = mid + 1; else hi = mid - 1;
            }
            if (found >= 0) {
                i = found;
                hasNext = true;
            }
        } else {
            advanceToNextValid();
        }

        if (hasNext) {
            planRowEmission(quad);
        }
    }

    /** See BGIteratorSO.planRowEmission: constants pre-built, only S varies per row. */
    private void planRowEmission(Quad quad) {
        if (quad.getGraph().isVariable()) {
            Var v = Var.alloc(quad.getGraph());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                gVar = v;
                gId = NodeId.pack(NodeType.GRAPH, gi);
            }
        }
        if (quad.getPredicate().isVariable()) {
            Var v = Var.alloc(quad.getPredicate());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                pVar = v;
                pId = NodeId.pack(NodeType.PREDICATE, pi);
            }
        }
        if (quad.getObject().isVariable()) {
            Var v = Var.alloc(quad.getObject());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                oVar = v;
                oId = NodeId.pack(NodeType.OBJECT, oi);
            }
        }
        if (quad.getSubject().isVariable()) {
            Var v = Var.alloc(quad.getSubject());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                sVar = v;
            }
        }
    }

    private void advanceToNextValid() {
        hasNext = false;
        while (i < j) {
            long subId = Ss.get(i);

            if (subId < minSubId) {
                i++;
                continue;
            }
            if (subId > maxSubId) {
                i++;
                continue;
            }
            hasNext = true;
            return;
        }
    }

    private void analyzeFilters(ExprList filter, PositionalDictionaryReader dict, Quad quad) {
        // Only ordering comparisons become range hints (see FilterBounds); every
        // other function in the FILTER is evaluated by the enclosing OpFilter.
        FilterBounds.scan(filter, (var, op, value) -> applyBound(var, op, value, dict, quad));
    }

    private void applyBound(Var var, String op, Node value, PositionalDictionaryReader dict, Quad quad) {
        if (!var.equals(quad.getSubject())) return;
        // Snap the bound to the edges of the whole value-equal cluster (degenerates
        // to the plain insertion point for non-literal constants); see ValueCluster.
        ValueCluster.Bounds c = ValueCluster.of(dict.getSubjects(), value);
        switch (op) {
            case ">" -> {
                 long target = c.firstGT();
                 if (Long.compareUnsigned(target, minSubId) > 0) minSubId = target;
            }
            case ">=" -> {
                 if (Long.compareUnsigned(c.firstGE(), minSubId) > 0) minSubId = c.firstGE();
            }
            case "<" -> {
                 long target = c.lastLT();
                 if (Long.compareUnsigned(target, maxSubId) < 0) maxSubId = target;
            }
            case "<=" -> {
                 long target = c.lastLE();
                 if (Long.compareUnsigned(target, maxSubId) < 0) maxSubId = target;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();
        BindingNodeId result = new BindingNodeId(this.parentBinding);
        long currentSubjectId = Ss.get(i);
        if (gVar != null) result.put(gVar, gId);
        if (pVar != null) result.put(pVar, pId);
        if (oVar != null) result.put(oVar, oId);
        if (sVar != null) result.put(sVar, NodeId.pack(NodeType.SUBJECT, currentSubjectId));
        i++;
        if (i < j) {
            if (subBound) hasNext = false;
            else advanceToNextValid();
        } else {
            hasNext = false;
        }
        return result;
    }
}
