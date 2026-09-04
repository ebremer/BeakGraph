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
 * Iterator for GPOS index where G and P are bound, finding O and S.
 * Structure: Graph -> Predicate -> Object -> Subject
 * Optimized with Object-level range filtering.
 */
public class BGIteratorPOS implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final BitPackedUnSignedLongBuffer Bs, Ss, So;
    private final HDTBitmapDirectory dirS;
    private long gi, pi;
    private long oStart, oEnd, curOIndex;
    private long sStart, sEnd, curSIndex;
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;
    private boolean hasNext = false;
    private final NodeTable nodeTable;

    // Row-emission plan (see BGIteratorSO): pattern and parent binding are fixed,
    // so the vars each row binds are computed once. Object and subject vary per row.
    private Var oVar, sVar;

    // Triple-term pattern in the object position: rows whose object id fails
    // unification are dropped in computeNext (the same skip path repeated-var
    // conflicts already use). Null for every other pattern shape.
    private TripleTermMatcher ttMatcher;

    public BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this(dict, reader, bnid, quad, filter, nodeTable, -1, Long.MAX_VALUE);
    }

    /**
     * Range-restricted variant for parallel scanning (see ScanChunks): only
     * object POSITIONS within [oPosLo, oPosHi] (intersected with this
     * predicate's filter-narrowed object range) are walked. An object's whole
     * subject block belongs to the chunk owning its position.
     */
    BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable, long oPosLo, long oPosHi) {
        this.parentBinding = bnid;
        this.nodeTable = nodeTable;

        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        BitPackedUnSignedLongBuffer Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');

        HDTBitmapDirectory dirP = reader.getDirectory('P');
        HDTBitmapDirectory dirO = reader.getDirectory('O');
        this.dirS = reader.getDirectory('S');

        // Analyze filters specifically for the Object variable
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }

        // Triple-term pattern in the object position: clamp the object range to
        // the triple-term suffix of the object space (empties instantly on a
        // triple-term-free store) and unify per candidate in computeNext.
        if (TripleTermMatcher.isPattern(quad.getObject())) {
            ttMatcher = TripleTermMatcher.compile(quad.getObject(), dict);
            if (ttMatcher == null) {
                return; // pattern cannot match anything in this store
            }
            minObjId = Math.max(minObjId, dict.firstTripleTermObjectId());
        }

        // 1. Resolve Graph
        if (quad.getGraph().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getGraph())) : NodeId.NONE;
            if (bound == NodeId.NONE) throw new IllegalStateException("BGIteratorPOS requires Graph to be bound.");
            gi = NodeId.id(bound);
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) return;

        // 2. Resolve Predicate
        if (quad.getPredicate().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getPredicate())) : NodeId.NONE;
            if (bound == NodeId.NONE) return;
            pi = NodeId.id(bound);
        } else {
            pi = dict.getPredicates().locate(quad.getPredicate());
        }
        if (pi < 1) return;

        // --- Traverse GPOS ---

        // A. Find Predicate Index under Graph
        long pRangeStart = RangeSelect.blockStart(dirP, Bp, gi);
        if (pRangeStart == -1) return;
        long pRangeEnd = RangeSelect.blockEnd(dirP, Bp, gi, pRangeStart);
        if (pRangeStart > pRangeEnd) return;
        long pIndex = Sp.binarySearch(pRangeStart, pRangeEnd, pi);
        if (pIndex < 0) return;

        // B. Determine raw Object Range for this Predicate
        long rawOStart = RangeSelect.blockStart(dirO, Bo, pIndex + 1);
        if (rawOStart == -1) return;
        long rawOEnd = RangeSelect.blockEnd(dirO, Bo, pIndex + 1, rawOStart);
        if (rawOStart > rawOEnd) return;

        // C. APPLY FILTER: Narrow the Object Range using Binary Search
        // upperBound already returns the last index whose value is <= maxObjId
        // (inclusive), so it is used as oEnd directly - subtracting 1 dropped the
        // boundary object group from FILTER(?o <= X) results.
        this.oStart = (minObjId <= 0) ? rawOStart : So.lowerBound(rawOStart, rawOEnd, minObjId);
        this.oEnd = (maxObjId == Long.MAX_VALUE) ? rawOEnd : So.upperBound(rawOStart, rawOEnd, maxObjId);

        // Parallel-chunk clamp: restrict to this chunk's slice of the object range.
        if (oPosLo > oStart) oStart = oPosLo;
        if (oPosHi < oEnd) oEnd = oPosHi;

        if (oStart > oEnd || oStart < 0) return;

        // D. Initialize Nested Iteration
        this.curOIndex = oStart;
        setupSubjectRange(true);
        advanceToNextValid();

        if (hasNext) {
            if (quad.getObject().isVariable()) oVar = Var.alloc(quad.getObject());
            if (quad.getSubject().isVariable()) sVar = Var.alloc(quad.getSubject());
        }
    }

    /**
     * Positions [sStart..sEnd] on the subject block of object index {@code curOIndex}.
     * Blocks tile the S level contiguously, so after the first (select1-based)
     * placement each advance to the NEXT object index starts exactly at the previous
     * block's end + 1 - no select at all; only the new end is found (a short forward
     * bitmap scan with a directory fallback for giant blocks).
     */
    private void setupSubjectRange(boolean first) {
        if (curOIndex > oEnd) {
            sStart = -1;
            return;
        }
        if (first) {
            this.sStart = RangeSelect.blockStart(dirS, Bs, curOIndex + 1);
            if (sStart == -1) return;
        } else {
            this.sStart = this.sEnd + 1;
        }
        this.sEnd = RangeSelect.blockEnd(dirS, Bs, curOIndex + 1, sStart);
        this.curSIndex = sStart;
    }

    private void advanceToNextValid() {
        hasNext = false;
        while (curOIndex <= oEnd) {
            if (curSIndex <= sEnd && curSIndex != -1 && sStart != -1) {
                hasNext = true;
                return;
            }
            curOIndex++;
            if (curOIndex <= oEnd) {
                // Re-anchor with a full select if the previous placement failed
                // (never expected mid-range; carry forward from a stale end would
                // corrupt the walk).
                setupSubjectRange(sStart == -1);
            }
        }
    }

    private void analyzeFilters(ExprList filter, PositionalDictionaryReader dict, Quad quad) {
        // Only ordering comparisons become range hints (see FilterBounds); every
        // other function in the FILTER is evaluated by the enclosing OpFilter.
        FilterBounds.scan(filter, (var, op, value) -> applyBound(var, op, value, dict, quad));
    }

    private void applyBound(Var var, String op, Node value, PositionalDictionaryReader dict, Quad quad) {
        if (!var.equals(quad.getObject())) return;
        // Snap the bound to the edges of the whole value-equal cluster: value-equal
        // but term-distinct literals ("5"^^xsd:int vs "5"^^xsd:integer) occupy
        // adjacent distinct ids, and the raw exact-term insertion point can land
        // inside that cluster, silently dropping qualifying boundary rows.
        long[] c = ValueCluster.of(dict.getObjects(), value);
        switch (op) {
            case ">" -> {
                 long target = c[1] + 1;
                 if (Long.compareUnsigned(target, minObjId) > 0) minObjId = target;
            }
            case ">=" -> {
                 if (Long.compareUnsigned(c[0], minObjId) > 0) minObjId = c[0];
            }
            case "<" -> {
                 long target = c[0] - 1;
                 if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
            }
            case "<=" -> {
                 long target = c[1];
                 if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
            }
        }
    }

    // Look-ahead: the next deliverable row, or null. Rows whose repeated-variable
    // bindings conflict are skipped here, so hasNext() only answers true when
    // next() really has a row to return.
    private BindingNodeId pending = null;
    private boolean primed = false;

    /**
     * Builds the next row whose bindings are all compatible. A variable repeated in
     * the pattern (e.g. ?x <p> ?x as subject and object) must bind to the same term
     * in both positions; putCompatible reports the conflict and the row is skipped
     * rather than emitted with the first value.
     */
    private BindingNodeId computeNext() {
        while (hasNext) {
            BindingNodeId result = new BindingNodeId(this.parentBinding);
            boolean ok = true;
            if (oVar != null) {
                ok = result.putCompatible(oVar, NodeId.pack(NodeType.OBJECT, So.get(curOIndex)), nodeTable);
            }
            if (ok && ttMatcher != null) {
                // Unify the candidate object id against the triple-term pattern
                // BEFORE the subject binds, so a repeated variable spanning the
                // two (?x inside the term and as pattern subject) is checked by
                // putCompatible against the embedded binding.
                BindingNodeId unified = ttMatcher.matchAndBind(So.get(curOIndex), result, nodeTable);
                if (unified == null) {
                    ok = false;
                } else {
                    result = unified;
                }
            }
            if (ok && sVar != null) {
                ok = result.putCompatible(sVar, NodeId.pack(NodeType.SUBJECT, Ss.get(curSIndex)), nodeTable);
            }
            curSIndex++;
            advanceToNextValid();
            if (ok) return result;
        }
        return null;
    }

    private void prime() {
        if (!primed) {
            primed = true;
            pending = computeNext();
        }
    }

    @Override
    public boolean hasNext() {
        prime();
        return pending != null;
    }

    @Override
    public BindingNodeId next() {
        prime();
        if (pending == null) throw new NoSuchElementException();
        BindingNodeId r = pending;
        pending = computeNext();
        return r;
    }
}
