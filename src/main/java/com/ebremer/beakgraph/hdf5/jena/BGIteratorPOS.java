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
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprList;

/**
 * Iterator for GPOS index where G and P are bound, finding O and S.
 * Structure: Graph -> Predicate -> Object -> Subject
 * Optimized with Object-level range filtering.
 */
public class BGIteratorPOS implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    private final BitPackedUnSignedLongBuffer Bp, Sp, Bo, So, Bs, Ss;
    // Accelerated rank/select directories used for select1; raw B*/S* still used for get().
    private final HDTBitmapDirectory dirP, dirO, dirS;
    private long gi, pi;
    private long oStart, oEnd, curOIndex;
    private long sStart, sEnd, curSIndex;
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;
    private boolean hasNext = false;
    private PositionalDictionaryReader dict;
    private final NodeTable nodeTable;

    public BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        this.dict = dict;
        this.nodeTable = nodeTable;
        
        this.Bp = reader.getBitmapBuffer('P'); 
        this.Sp = reader.getIDBuffer('P');     
        this.Bo = reader.getBitmapBuffer('O'); 
        this.So = reader.getIDBuffer('O');     
        this.Bs = reader.getBitmapBuffer('S'); 
        this.Ss = reader.getIDBuffer('S');     
        
        this.dirP = reader.getDirectory('P');
        this.dirO = reader.getDirectory('O');
        this.dirS = reader.getDirectory('S');

        // Analyze filters specifically for the Object variable
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }
        
        // 1. Resolve Graph
        if (quad.getGraph().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getGraph()))) gi = bnid.get(Var.alloc(quad.getGraph())).getId();
            else throw new IllegalStateException("BGIteratorPOS requires Graph to be bound."); //return; 
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) return;
        
        // 2. Resolve Predicate
        if (quad.getPredicate().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getPredicate()))) pi = bnid.get(Var.alloc(quad.getPredicate())).getId();
            else return; 
        } else {
            pi = dict.getPredicates().locate(quad.getPredicate());
        }
        if (pi < 1) return;

        // --- Traverse GPOS ---

        // A. Find Predicate Index under Graph
        long pRangeStart = select1Safe(dirP, Bp,gi);
        long nextGraphStart = select1Safe(dirP, Bp,gi + 1);
        long pRangeEnd = (nextGraphStart == -1) ? (Sp.getNumEntries() - 1) : (nextGraphStart - 1);
        
        if (pRangeStart == -1 || pRangeStart > pRangeEnd) return;
        long pIndex = Sp.binarySearch(pRangeStart, pRangeEnd, pi);
        if (pIndex < 0) return;

        // B. Determine raw Object Range for this Predicate
        long rawOStart = select1Safe(dirO, Bo,pIndex + 1);
        long nextPStart = select1Safe(dirO, Bo,pIndex + 2);
        long rawOEnd = (nextPStart == -1) ? (So.getNumEntries() - 1) : (nextPStart - 1);
        
        if (rawOStart == -1 || rawOStart > rawOEnd) return;

        // C. APPLY FILTER: Narrow the Object Range using Binary Search
        // upperBound already returns the last index whose value is <= maxObjId
        // (inclusive), so it is used as oEnd directly - subtracting 1 dropped the
        // boundary object group from FILTER(?o <= X) results.
        this.oStart = (minObjId <= 0) ? rawOStart : So.lowerBound(rawOStart, rawOEnd, minObjId);
        this.oEnd = (maxObjId == Long.MAX_VALUE) ? rawOEnd : So.upperBound(rawOStart, rawOEnd, maxObjId);

        if (oStart > oEnd || oStart < 0) return;
        
        // D. Initialize Nested Iteration
        this.curOIndex = oStart;
        setupSubjectRange();
        advanceToNextValid();
    }

    private void setupSubjectRange() {
        if (curOIndex > oEnd) {
            sStart = -1;
            return;
        }
        this.sStart = select1Safe(dirS, Bs,curOIndex + 1);
        long nextOStart = select1Safe(dirS, Bs,curOIndex + 2);
        this.sEnd = (nextOStart == -1) ? (Ss.getNumEntries() - 1) : (nextOStart - 1);
        this.curSIndex = sStart;
    }

    private void advanceToNextValid() {
        hasNext = false;
        while (curOIndex <= oEnd) {
            if (curSIndex <= sEnd && curSIndex != -1) {
                hasNext = true;
                return;
            }
            curOIndex++;
            if (curOIndex <= oEnd) {
                setupSubjectRange();
            }
        }
    }

    private void analyzeFilters(ExprList filter, PositionalDictionaryReader dict, Quad quad) {
        for (Expr expr : filter.getList()) {
            if (expr instanceof ExprFunction2 func) {
                Expr left = func.getArg1();
                Expr right = func.getArg2();
                String opcode = func.getOpName();
                if (left.isVariable() && right.isConstant()) {
                    applyBound(left.asVar(), opcode, right.getConstant().asNode(), dict, quad);
                } else if (left.isConstant() && right.isVariable()) {
                    applyBound(right.asVar(), flipOp(opcode), left.getConstant().asNode(), dict, quad);
                }
            }
        }
    }

    private String flipOp(String op) {
        return switch (op) {
            case ">" -> "<";
            case "<" -> ">";
            case ">=" -> "<=";
            case "<=" -> ">=";
            default -> op;
        };
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

    private long select1Safe(HDTBitmapDirectory dir, BitPackedUnSignedLongBuffer fallback, long rank) {
        if (rank < 1) return -1;
        // Accelerated O(log n) select via the superblock/block directory when present;
        // fall back to the buffer's linear scan only for indexes written without it.
        return (dir != null) ? dir.select1(rank) : fallback.select1(rank);
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
            long currentObjectId = So.get(curOIndex);
            long currentSubjectId = Ss.get(curSIndex);
            boolean ok = true;
            if (queryQuad.getObject().isVariable()) {
                ok = result.putCompatible(Var.alloc(queryQuad.getObject()), new NodeId(currentObjectId, NodeType.OBJECT), nodeTable);
            }
            if (ok && queryQuad.getSubject().isVariable()) {
                ok = result.putCompatible(Var.alloc(queryQuad.getSubject()), new NodeId(currentSubjectId, NodeType.SUBJECT), nodeTable);
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
