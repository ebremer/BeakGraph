package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
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
    private long gi, pi;
    private long oStart, oEnd, curOIndex;
    private long sStart, sEnd, curSIndex;
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;
    private boolean hasNext = false;
    private PositionalDictionaryReader dict;

    public BGIteratorPOS(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        //IO.println("BGIteratorPOS : " + quad);
        this.parentBinding = bnid;
        this.queryQuad = quad;
        this.dict = dict;
        
        this.Bp = reader.getBitmapBuffer('P'); 
        this.Sp = reader.getIDBuffer('P');     
        this.Bo = reader.getBitmapBuffer('O'); 
        this.So = reader.getIDBuffer('O');     
        this.Bs = reader.getBitmapBuffer('S'); 
        this.Ss = reader.getIDBuffer('S');     
        
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
        long pRangeStart = select1Safe(Bp, gi);
        long nextGraphStart = select1Safe(Bp, gi + 1);
        long pRangeEnd = (nextGraphStart == -1) ? (Sp.getNumEntries() - 1) : (nextGraphStart - 1);
        
        if (pRangeStart == -1 || pRangeStart > pRangeEnd) return;
        long pIndex = Sp.binarySearch(pRangeStart, pRangeEnd, pi);
        if (pIndex < 0) return;

        // B. Determine raw Object Range for this Predicate
        long rawOStart = select1Safe(Bo, pIndex + 1);
        long nextPStart = select1Safe(Bo, pIndex + 2);
        long rawOEnd = (nextPStart == -1) ? (So.getNumEntries() - 1) : (nextPStart - 1);
        
        if (rawOStart == -1 || rawOStart > rawOEnd) return;

        // C. APPLY FILTER: Narrow the Object Range using Binary Search
        this.oStart = (minObjId <= 0) ? rawOStart : So.lowerBound(rawOStart, rawOEnd, minObjId);
        this.oEnd = (maxObjId == Long.MAX_VALUE) ? rawOEnd : So.upperBound(rawOStart, rawOEnd, maxObjId) - 1;

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
        this.sStart = select1Safe(Bs, curOIndex + 1);
        long nextOStart = select1Safe(Bs, curOIndex + 2);
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
        long rawResult = dict.getObjects().search(value);
        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);
        
        switch (op) {
            case ">" -> {
                 long target = found ? id + 1 : id;
                 if (Long.compareUnsigned(target, minObjId) > 0) minObjId = target;
            }
            case ">=" -> {
                 if (Long.compareUnsigned(id, minObjId) > 0) minObjId = id;
            }
            case "<" -> {
                 if (id == 0) { maxObjId = 0; minObjId = 1; } 
                 else {
                     long target = id - 1;
                     if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
                 }
            }
            case "<=" -> {
                 long target = found ? id : id - 1;
                 if (id == 0 && !found) { maxObjId = 0; minObjId = 1; } 
                 else {
                     if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
                 }
            }
        }
    }

    private long select1Safe(BitPackedUnSignedLongBuffer buffer, long rank) {
        if (rank < 1) return -1;
        return buffer.select1(rank);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();
        BindingNodeId result = new BindingNodeId(this.parentBinding);
        long currentObjectId = So.get(curOIndex);
        long currentSubjectId = Ss.get(curSIndex);
       // IO.println("POS : "+dict.getSubjects().extract(currentSubjectId)+"   "+dict.getObjects().extract(currentObjectId));
        //if (queryQuad.getGraph().isVariable()) result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        //if (queryQuad.getPredicate().isVariable()) result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        //if (queryQuad.getObject().isVariable()) result.put(Var.alloc(queryQuad.getObject()), new NodeId(currentObjectId, NodeType.OBJECT));
        //if (queryQuad.getSubject().isVariable()) result.put(Var.alloc(queryQuad.getSubject()), new NodeId(currentSubjectId, NodeType.SUBJECT));
        if (queryQuad.getObject().isVariable()) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(currentObjectId, NodeType.OBJECT));
        }
        if (queryQuad.getSubject().isVariable()) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(currentSubjectId, NodeType.SUBJECT));
        }
        curSIndex++;
        IO.println(String.format("POS : %s --- %s ----> [%d] %s", dict.getSubjects().extract(currentSubjectId), dict.getPredicates().extract(pi), currentObjectId, dict.getObjects().extract(currentObjectId))); 
        advanceToNextValid();
        return result;
    }
}
