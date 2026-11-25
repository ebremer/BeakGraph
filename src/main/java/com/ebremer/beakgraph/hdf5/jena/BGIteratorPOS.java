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
    private long blockEnd;                   // where the current object's whole subject block ends: the next begins after it
    private long sPosLo = 0;                 // the subject positions this iterator reads: all of them, or one
    private long sPosHi = Long.MAX_VALUE;    // part's (see the constructor that takes them)
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;
    private boolean hasNext = false;
    private final NodeTable nodeTable;

    // Row-emission plan (see BGIteratorSO): pattern and parent binding are fixed,
    // so the vars each row binds are computed once. Object and subject vary per row.
    private Var oVar, sVar;
    // Whether each is new to every row - in no layer of the parent, and not the
    // other one again (?x <p> ?x) - so that it is bound without a lookup. A
    // variable that is not keeps putCompatible, which is what says a row with
    // two values for it is no row.
    private boolean oFresh, sFresh;

    // Compound-term pattern in the object position (a triple term or a
    // sequence with variables inside, ObjectPattern): rows whose object id
    // fails to match are dropped in computeNext (the same skip path
    // repeated-var conflicts already use). Null for every other pattern shape.
    private ObjectPattern matcher;
    // A pattern's further matches of one row (a sequence pattern's gaps), handed out before the scan moves on.
    private java.util.ArrayDeque<BindingNodeId> more;

    public BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this(dict, reader, bnid, quad, filter, nodeTable, -1, Long.MAX_VALUE, 0, Long.MAX_VALUE, -1);
    }

    /**
     * @param presetGi the graph's dictionary id when the caller already holds
     *                 it (a walk over the columnar graph list, BG-259); -1 to
     *                 resolve the graph from the pattern and binding
     */
    BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable, long presetGi) {
        this(dict, reader, bnid, quad, filter, nodeTable, -1, Long.MAX_VALUE, 0, Long.MAX_VALUE, presetGi);
    }

    /**
     * Range-restricted variant for parallel scanning (see ScanChunks): only
     * object POSITIONS within [oPosLo, oPosHi] (intersected with this
     * predicate's filter-narrowed object range) are walked. An object's whole
     * subject block belongs to the chunk owning its position.
     */
    BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable, long oPosLo, long oPosHi) {
        this(dict, reader, bnid, quad, filter, nodeTable, oPosLo, oPosHi, 0, Long.MAX_VALUE, -1);
    }

    /**
     * A part of the scan cut out by SUBJECT positions, for a predicate with
     * few values and many rows, whose objects are too few to divide by: the
     * rows at subject positions [sPosLo, sPosHi] of the objects at positions
     * [oPosLo, oPosHi] - the objects whose blocks those subject positions lie
     * in. A block that reaches past either end is read as far as the end.
     */
    BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable,
            long oPosLo, long oPosHi, long sPosLo, long sPosHi) {
        this(dict, reader, bnid, quad, filter, nodeTable, oPosLo, oPosHi, sPosLo, sPosHi, -1);
    }

    private BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable,
            long oPosLo, long oPosHi, long sPosLo, long sPosHi, long presetGi) {
        this.parentBinding = bnid;
        this.nodeTable = nodeTable;
        this.sPosLo = sPosLo;
        this.sPosHi = sPosHi;

        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        BitPackedUnSignedLongBuffer Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');

        HDTBitmapDirectory dirO = reader.getDirectory('O');
        this.dirS = reader.getDirectory('S');

        // Analyze filters specifically for the Object variable
        RangeBounds bounds = RangeBounds.of(filter, quad, dict);
        minObjId = bounds.minO;
        maxObjId = bounds.maxO;

        // Compound-term pattern in the object position: clamp the object range
        // to the ids it can match - the triple-term band, or the sequences
        // holding its constant prefix - (empty at once on a store without
        // such terms) and match per candidate in computeNext.
        if (ObjectPattern.isPattern(quad.getObject())) {
            matcher = ObjectPattern.compile(quad.getObject(), dict);
            if (matcher == null) {
                return; // pattern cannot match anything in this store
            }
            minObjId = Math.max(minObjId, matcher.firstObjectId());
            maxObjId = Math.min(maxObjId, matcher.lastObjectId());
            if (matcher.multiple()) {
                more = new java.util.ArrayDeque<>();
            }
        }

        // 1. Resolve Graph
        if (presetGi >= 1) {
            gi = presetGi;
        } else if (quad.getGraph().isVariable()) {
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
        long[] graphBlock = reader.graphBlock('P', gi);
        if (graphBlock == null) return;
        long pIndex = Sp.binarySearch(graphBlock[0], graphBlock[1], pi);
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
            oFresh = RowVars.fresh(parentBinding, oVar);
            // A compound-term pattern may hold the subject's variable, which it then checks against the subject's id.
            sFresh = matcher == null && RowVars.fresh(parentBinding, sVar, oVar);
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
            this.sStart = this.blockEnd + 1;
        }
        this.blockEnd = RangeSelect.blockEnd(dirS, Bs, curOIndex + 1, sStart);
        // The part of the block this iterator reads: all of it, unless the scan was divided by subject positions.
        this.sEnd = Math.min(blockEnd, sPosHi);
        if (sStart < sPosLo) sStart = sPosLo;
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
        if (more != null && !more.isEmpty()) {
            return more.poll();
        }
        while (hasNext) {
            BindingNodeId result = new BindingNodeId(this.parentBinding);
            boolean ok = true;
            if (oFresh) {
                result.bind(oVar, NodeId.pack(NodeType.OBJECT, So.get(curOIndex)));
            } else if (oVar != null) {
                ok = result.putCompatible(oVar, NodeId.pack(NodeType.OBJECT, So.get(curOIndex)), nodeTable);
            }
            if (ok && sFresh) {
                result.bind(sVar, NodeId.pack(NodeType.SUBJECT, Ss.get(curSIndex)));
            } else if (ok && sVar != null) {
                ok = result.putCompatible(sVar, NodeId.pack(NodeType.SUBJECT, Ss.get(curSIndex)), nodeTable);
            }
            if (ok && matcher != null) {
                // Match the candidate object id against the pattern AFTER the
                // subject binds, so that every match carries it; a variable
                // spanning the two (?x inside the term and as the pattern's
                // subject) is checked by putCompatible against the subject.
                BindingNodeId matched = matcher.match(So.get(curOIndex), result, nodeTable, more);
                if (matched == null) {
                    ok = false;
                } else {
                    result = matched;
                }
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
