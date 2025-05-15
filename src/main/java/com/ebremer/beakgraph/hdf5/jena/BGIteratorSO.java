package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.FiveSectionDictionaryReader;
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
 * Iterates over Objects for a bound (Graph, Subject, Predicate).
 * Uses GSPO Index (G->S->P->O).
 * Updated to use raw Bitmap Buffers instead of HDTBitmapDirectory for consistency.
 */
public class BGIteratorSO implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final FiveSectionDictionaryReader dict;
    private final Quad queryQuad;
    
    // Raw Buffers for Object Level
    private final BitPackedUnSignedLongBuffer Bo; 
    private final BitPackedUnSignedLongBuffer So; 
    
    private long i; 
    private long j; 
    private long gi;
    private long si;
    private long pi;
    private long oi = -1;
    private boolean hasNext = false;
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;

    public BGIteratorSO(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        this.dict = dict;
        
        // Load raw buffers for Object level
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }
        
        // 1. Locate Graph
        gi = dict.getGraphs().locate(quad.getGraph());
        if (gi < 1) return;
        
        // 2. Locate Subject
        si = dict.getSubjects().locate(quad.getSubject());
        if (si < 1) return;
        
        // 3. Locate Predicate
        pi = dict.getPredicates().locate(quad.getPredicate());
        if (pi < 1) return;
        
        // 4. Locate Object (if bound)
        if (!quad.getObject().isVariable()) {
            oi = dict.getObjects().locate(quad.getObject());
            if (oi < 1) return;
            if (oi < minObjId || oi > maxObjId) return;
        }

        // --- Navigate GSPO Index using Raw Buffers ---

        // Level 1: Subject (S)
        BitPackedUnSignedLongBuffer Bs = reader.getBitmapBuffer('S');
        // Get Subject ID buffer (not needed for select, but needed for value check)
        BitPackedUnSignedLongBuffer Ss = reader.getIDBuffer('S');
        
        long sStart = (gi == 1) ? 0 : Bs.select1(gi - 1) + 1;
        long sEnd = Bs.select1(gi);
        if (sStart == -1 || sEnd == -1 || sStart > sEnd) return;
        
        // Find the specific Subject ID in the range
        long sIndex = findIdInRange(Ss, dict.getSubjects(), si, sStart, sEnd);
        if (sIndex < 0) return;

        // Level 2: Predicate (P)
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');

        long sRank = sIndex + 1; 
        long pStart = (sRank == 1) ? 0 : Bp.select1(sRank - 1) + 1;
        long pEnd = Bp.select1(sRank);
        if (pStart == -1 || pEnd == -1 || pStart > pEnd) return;
        
        // Find the specific Predicate ID in the range
        long pIndex = findIdInRange(Sp, dict.getPredicates(), pi, pStart, pEnd);
        if (pIndex < 0) return;

        // Level 3: Object (O)
        // Bo and So already loaded
        long pRank = pIndex + 1;
        long oStart = (pRank == 1) ? 0 : Bo.select1(pRank - 1) + 1;
        long oEnd = Bo.select1(pRank);
        if (oStart == -1 || oEnd == -1 || oStart > oEnd) return;

        this.i = oStart;
        this.j = oEnd + 1;

        if (oi > 0) {
            long foundIndex = findIdInRange(So, dict.getObjects(), oi, i, j - 1);
            if (foundIndex < 0) {
                hasNext = false;
            } else {
                i = foundIndex;
                j = foundIndex + 1;
                hasNext = true;
            }
        } else {
            while (i < j && So.get(i) < minObjId) {
                i++;
            }
            hasNext = i < j && So.get(i) <= maxObjId;
        }
    }
    
    private void analyzeFilters(ExprList filter, FiveSectionDictionaryReader dict, Quad quad) {
        for (Expr expr : filter.getList()) {
            if (expr instanceof ExprFunction2 func) {
                Expr left = func.getArg1();
                Expr right = func.getArg2();
                String opcode = func.getOpName();
                if (left.isVariable() && right.isConstant()) {
                    Var var = left.asVar();
                    Node value = right.getConstant().asNode();
                    applyBound(var, opcode, value, dict, quad);
                } else if (left.isConstant() && right.isVariable()) {
                    Var var = right.asVar();
                    Node value = left.getConstant().asNode();
                    applyBound(var, flipOp(opcode), value, dict, quad);
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

    private void applyBound(Var var, String op, Node value, FiveSectionDictionaryReader dict, Quad quad) {
        if (!var.equals(quad.getObject())) return;
        long rawResult = dict.getObjects().search(value);
        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);
        switch (op) {
            case ">" -> {
                long bound = found ? id + 1 : Math.max(0, id - 1);
                minObjId = Math.max(minObjId, bound);
            }
            case ">=" -> {
                long bound = found ? id : Math.max(0, id - 1);
                minObjId = Math.max(minObjId, bound);
            }
            case "<" -> maxObjId = Math.min(maxObjId, id - 1);
            case "<=" -> {
                long bound = found ? id : id;
                maxObjId = Math.min(maxObjId, bound);
            }
        }
    }
    
    private long findIdInRange(BitPackedUnSignedLongBuffer buffer, Dictionary d, long target, long start, long end) {
        long low = start;
        long high = end; 
        
        while (low <= high) {
            long mid = (low + high) >>> 1;
            long midID = buffer.get(mid);
            
            if (midID == target) return mid;
            
            if (midID < target) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return -1;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();        
        BindingNodeId result = new BindingNodeId(this.parentBinding);
        long currentObjectId = So.get(i); 
        
        if (queryQuad.getGraph().isVariable()) {
            result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        }
        if (queryQuad.getSubject().isVariable()) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(si, NodeType.SUBJECT));
        }
        if (queryQuad.getPredicate().isVariable()) {
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        }
        if (queryQuad.getObject().isVariable()) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(currentObjectId, NodeType.OBJECT));
        }
        i++;        
        if (i < j) {
            hasNext = So.get(i) <= maxObjId;
        } else {
            hasNext = false;
        }        
        return result;
    }
}