package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.Dictionary;
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
 * Iterator for GSPO index where G, S, and P are bound (or fixed), finding O.
 * Optimized with Object-level range filtering and binary search.
 */
public class BGIteratorSO implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final BitPackedUnSignedLongBuffer So;
    private final NodeTable nodeTable;

    private long i;  // current object index
    private long j;  // end object index (inclusive)
    private long gi, si, pi;
    private boolean hasNext = false;

    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;

    // Compound-term pattern in the object position (<<( ?a :b ?c )>>,
    // ( 1 ?x ... ), ObjectPattern): candidates in the clamped range match per
    // row; failures are skipped through the ttPending look-ahead, and a
    // sequence pattern's further matches of one row wait in `more`. Null for
    // every other shape, keeping the ordinary emit path branch-identical.
    private ObjectPattern ttMatcher;
    private BindingNodeId ttPending;
    private java.util.ArrayDeque<BindingNodeId> more;

    // Row-emission plan, computed once: which variables each row binds, with the
    // constant G/S/P packed NodeIds pre-built (only the object id varies per row).
    private Var gVar, sVar, pVar, oVar;
    private long gId, sId, pId;
    // True when the planned variables are pairwise distinct: none is in the parent
    // (planRowEmission), so each is new to every row and is bound without a lookup.
    private boolean distinctVars;

    public BGIteratorSO(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this(dict, reader, bnid, quad, filter, nodeTable, -1);
    }

    /**
     * @param presetGi the graph's dictionary id when the caller already holds
     *                 it (a walk over the columnar graph list, BG-259); -1 to
     *                 resolve the graph from the pattern and binding
     */
    BGIteratorSO(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable, long presetGi) {
        this.parentBinding = bnid;
        this.nodeTable = nodeTable;

        // GSPO Structure mapping
        BitPackedUnSignedLongBuffer Ss = reader.getIDBuffer('S');
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        BitPackedUnSignedLongBuffer Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');

        HDTBitmapDirectory dirP = reader.getDirectory('P');
        HDTBitmapDirectory dirO = reader.getDirectory('O');

        RangeBounds bounds = RangeBounds.of(filter, quad, dict);
        minObjId = bounds.minO;
        maxObjId = bounds.maxO;

        // Resolve Graph
        gi = (presetGi >= 1) ? presetGi : resolveNode(quad.getGraph(), dict.getGraphs(), bnid);
        if (gi < 1) return;

        // Resolve Subject
        si = resolveNode(quad.getSubject(), dict.getSubjects(), bnid);
        if (si < 1) return;

        // Resolve Predicate
        pi = resolveNode(quad.getPredicate(), dict.getPredicates(), bnid);
        if (pi < 1) return;

        // --- Traverse GSPO ---

        // A. Find Subject Index under Graph
        long[] graphBlock = reader.graphBlock('S', gi);
        if (graphBlock == null) return;
        long sIndex = Ss.binarySearch(graphBlock[0], graphBlock[1], si);
        if (sIndex < 0) return;

        // B. Find Predicate Index under Subject
        long pStart = RangeSelect.blockStart(dirP, Bp, sIndex + 1);
        if (pStart == -1) return;
        long pEnd = RangeSelect.blockEnd(dirP, Bp, sIndex + 1, pStart);
        if (pStart > pEnd) return;
        long pIndex = Sp.binarySearch(pStart, pEnd, pi);
        if (pIndex < 0) return;

        // C. Find Object Range for Predicate
        long rawOStart = RangeSelect.blockStart(dirO, Bo, pIndex + 1);
        if (rawOStart == -1) return;
        long rawOEnd = RangeSelect.blockEnd(dirO, Bo, pIndex + 1, rawOStart);
        if (rawOStart > rawOEnd) return;

        // D. Apply Specific Object Bound or Range Filters
        // Distinguish "object is an unbound variable" from "object is a concrete
        // term (or bound variable)": resolveNode returns -1 for both an unbound
        // variable and a term missing from the dictionary, and a missing term must
        // yield no match - not a full range scan (which fabricated phantom rows,
        // e.g. ASK with a non-existent object answered true).
        Node oNode = quad.getObject();
        boolean oTTPattern = ObjectPattern.isPattern(oNode);
        if (oTTPattern) {
            ttMatcher = ObjectPattern.compile(oNode, dict);
            if (ttMatcher == null) {
                return; // pattern cannot match anything in this store
            }
            // The ids the pattern can match are a contiguous band of the
            // object space (the triple terms, or the sequences holding its
            // constant prefix) - clamp the scan to it (a store without such
            // terms empties the range immediately via the lowerBound below).
            minObjId = Math.max(minObjId, ttMatcher.firstObjectId());
            maxObjId = Math.min(maxObjId, ttMatcher.lastObjectId());
            if (ttMatcher.multiple()) {
                more = new java.util.ArrayDeque<>();
            }
        }
        boolean oUnbound = oTTPattern
                || (oNode.isVariable() && (bnid == null || !bnid.containsKey(Var.alloc(oNode))));

        if (oUnbound) {
            // Case: Object is a variable, apply min/max ID range filters
            this.i = (minObjId <= 0) ? rawOStart : So.lowerBound(rawOStart, rawOEnd, minObjId);
            this.j = (maxObjId == Long.MAX_VALUE) ? rawOEnd : So.upperBound(rawOStart, rawOEnd, maxObjId);

            if (this.i != -1 && this.i <= this.j) {
                this.hasNext = true;
            }
        } else {
            // Case: Object is bound (e.g., G, S, P, O are all known, just checking existence)
            long specificObjId = resolveNode(oNode, dict.getObjects(), bnid);
            if (specificObjId < 1) return; // concrete term absent from this store -> no match
            long foundIdx = So.binarySearch(rawOStart, rawOEnd, specificObjId);
            if (foundIdx >= 0) {
                this.i = foundIdx;
                this.j = foundIdx;
                this.hasNext = true;
            }
        }

        if (hasNext) {
            planRowEmission(quad);
        }
    }

    /**
     * Precomputes which variables each row binds - the pattern and the parent
     * binding are fixed for this iterator's lifetime, so the per-row work
     * reduces to appends of pre-built NodeIds (only the object id varies).
     */
    private void planRowEmission(Quad quad) {
        if (quad.getGraph().isVariable()) {
            Var v = Var.alloc(quad.getGraph());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                gVar = v;
                gId = NodeId.pack(NodeType.GRAPH, gi);
            }
        }
        if (quad.getSubject().isVariable()) {
            Var v = Var.alloc(quad.getSubject());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                sVar = v;
                sId = NodeId.pack(NodeType.SUBJECT, si);
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
            }
        }
        distinctVars = RowVars.distinct(gVar, sVar, pVar, oVar);
    }

    private long resolveNode(Node node, Dictionary dictionary, BindingNodeId bnid) {
        if (node.isVariable()) {
            if (bnid != null) {
                long bound = bnid.get(Var.alloc(node));
                if (bound != NodeId.NONE) {
                    return NodeId.id(bound);
                }
            }
            return -1; // Variable is unbound
        }
        return dictionary.locate(node);
    }

    @Override
    public boolean hasNext() {
        if (ttMatcher == null) {
            return hasNext;
        }
        primeTT();
        return ttPending != null;
    }

    @Override
    public BindingNodeId next() {
        if (ttMatcher != null) {
            primeTT();
            if (ttPending == null) throw new NoSuchElementException();
            BindingNodeId r = ttPending;
            ttPending = null;
            return r;
        }
        if (!hasNext) throw new NoSuchElementException();
        BindingNodeId result = new BindingNodeId(this.parentBinding);
        if (distinctVars) {
            if (gVar != null) result.bind(gVar, gId);
            if (sVar != null) result.bind(sVar, sId);
            if (pVar != null) result.bind(pVar, pId);
            if (oVar != null) result.bind(oVar, NodeId.pack(NodeType.OBJECT, So.get(i)));
        } else {
            if (gVar != null) result.put(gVar, gId);
            if (sVar != null) result.put(sVar, sId);
            if (pVar != null) result.put(pVar, pId);
            if (oVar != null) result.put(oVar, NodeId.pack(NodeType.OBJECT, So.get(i)));
        }
        i++;
        hasNext = (i <= j);
        return result;
    }

    /**
     * Look-ahead for triple-term patterns: walks the clamped object range and
     * keeps the first candidate that unifies (embedded variables bound into
     * chained layers); non-unifying candidates are skipped so hasNext() only
     * answers true when next() really has a row.
     */
    private void primeTT() {
        while (ttPending == null) {
            if (more != null && !more.isEmpty()) {
                ttPending = more.poll();
                return;
            }
            if (!hasNext) {
                return;
            }
            long oid = So.get(i);
            i++;
            hasNext = (i <= j);
            BindingNodeId row = new BindingNodeId(this.parentBinding);
            if (gVar != null) row.put(gVar, gId);
            if (sVar != null) row.put(sVar, sId);
            if (pVar != null) row.put(pVar, pId);
            ttPending = ttMatcher.match(oid, row, nodeTable, more);
        }
    }
}
