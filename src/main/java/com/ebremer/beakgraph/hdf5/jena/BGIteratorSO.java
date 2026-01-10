package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
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
 * Iterator for GSPO index where G, S, and P are bound, finding O.
 * Structure: Graph -> Subject -> Predicate -> Object
 */
public class BGIteratorSO implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    
    private final BitPackedUnSignedLongBuffer Bs, Ss, Bp, Sp, Bo, So;
    
    private long i; 
    private long j;
    private long gi, si, pi;
    private boolean hasNext = false;
    
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;

    public BGIteratorSO(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        //IO.println("BGIteratorSO (GSPO) Init: " + quad);
        this.parentBinding = bnid;
        this.queryQuad = quad;
        
        // GSPO Structure mapping
        this.Bs = reader.getBitmapBuffer('S'); 
        this.Ss = reader.getIDBuffer('S');     
        this.Bp = reader.getBitmapBuffer('P'); 
        this.Sp = reader.getIDBuffer('P');     
        this.Bo = reader.getBitmapBuffer('O'); 
        this.So = reader.getIDBuffer('O');     
        
        if (filter != null && !filter.isEmpty()) analyzeFilters(filter, dict, quad);
        
        // 1. Resolve Graph
        if (quad.getGraph().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getGraph()))) gi = bnid.get(Var.alloc(quad.getGraph())).getId();
            else return; 
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) return;
        
        // 2. Resolve Subject
        if (quad.getSubject().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getSubject()))) si = bnid.get(Var.alloc(quad.getSubject())).getId();
            else return; 
        } else {
            si = dict.getSubjects().locate(quad.getSubject());
        }
        if (si < 1) return;

        // 3. Resolve Predicate
        if (quad.getPredicate().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getPredicate()))) pi = bnid.get(Var.alloc(quad.getPredicate())).getId();
            else return; 
        } else {
            pi = dict.getPredicates().locate(quad.getPredicate());
        }
        if (pi < 1) return;

        // 4. Resolve Object Filter (The target variable)
        long specificObjId = -1;
        boolean isObjBound = !quad.getObject().isVariable() || (bnid != null && bnid.containsKey(Var.alloc(quad.getObject())));
        if (isObjBound) {
             if (quad.getObject().isVariable()) specificObjId = bnid.get(Var.alloc(quad.getObject())).getId();
             else specificObjId = dict.getObjects().locate(quad.getObject());
             if (specificObjId < 1) return;
        }

        // --- Traverse GSPO ---

        // A. Level 2: Subject Range for Graph
        long sStart = select1Safe(Bs, gi);
        long nextGraphStart = select1Safe(Bs, gi + 1);
        long sEnd = (nextGraphStart == -1) ? (Ss.getNumEntries() - 1) : (nextGraphStart - 1);
        if (sStart == -1 || sStart > sEnd) return;
        
        // B. Find Subject Index
        /*
        long sIndex = -1;
        for (long k = sStart; k <= sEnd; k++) {
            if (Ss.get(k) == si) {
                sIndex = k;
                break;
            }
        }*/
        long sIndex = Ss.binarySearch(sStart, sEnd, si);
        if (sIndex < 0) return;       

        // C. Level 3: Predicate Range for Subject
        long pStart = select1Safe(Bp, sIndex + 1);
        long nextSStart = select1Safe(Bp, sIndex + 2);
        long pEnd = (nextSStart == -1) ? (Sp.getNumEntries() - 1) : (nextSStart - 1);
        if (pStart == -1 || pStart > pEnd) return;
        
        // D. Find Predicate Index
        long pIndex = -1;
        for (long k = pStart; k <= pEnd; k++) {
            if (Sp.get(k) == pi) {
                pIndex = k;
                break;
            }
        }
        if (pIndex < 0) return;
        

        // E. Level 4: Object Range for Predicate
        long oStart = select1Safe(Bo, pIndex + 1);
        long nextPStart = select1Safe(Bo, pIndex + 2);
        long oEnd = (nextPStart == -1) ? (So.getNumEntries() - 1) : (nextPStart - 1);
        if (oStart == -1 || oStart > oEnd) return;
        
        this.i = oStart;
        this.j = oEnd + 1;

        // F. Initialize
        if (specificObjId > 0) {
            boolean found = false;
            for (long k = i; k < j; k++) {
                if (So.get(k) == specificObjId) {
                    i = k;
                    found = true;
                    break;
                }
            }
            hasNext = found;
        } else {
            advanceToNextValid();
        }
    }

    private long select1Safe(BitPackedUnSignedLongBuffer buffer, long rank) {
        if (rank < 1) return -1;
        return buffer.select1(rank);
    }
    
    private void advanceToNextValid() {
        hasNext = false;
        while (i < j) {
            long objId = So.get(i);
            if (objId < minObjId || objId > maxObjId) {
                i++;
                continue;
            }
            hasNext = true;
            return;
        }
    }

    private void analyzeFilters(ExprList filter, FiveSectionDictionaryReader dict, Quad quad) {
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
            case ">" -> "<"; case "<" -> ">"; case ">=" -> "<="; case "<=" -> ">="; default -> op;
        };
    }

    private void applyBound(Var var, String op, Node value, FiveSectionDictionaryReader dict, Quad quad) {
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
                 if (id == 0) { maxObjId = 0; minObjId = 1; } else {
                     long target = id - 1;
                     if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
                 }
            }
            case "<=" -> {
                 long target = found ? id : id - 1;
                 if (id == 0 && !found) { maxObjId = 0; minObjId = 1; } else {
                     if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
                 }
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
        long currentObjId = So.get(i);
        if (queryQuad.getGraph().isVariable()) result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        if (queryQuad.getSubject().isVariable()) result.put(Var.alloc(queryQuad.getSubject()), new NodeId(si, NodeType.SUBJECT));
        if (queryQuad.getPredicate().isVariable()) result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        if (queryQuad.getObject().isVariable()) result.put(Var.alloc(queryQuad.getObject()), new NodeId(currentObjId, NodeType.OBJECT));
        // IO.println(String.format("QUAD : %d %d %d %d", gi, si, pi, currentObjId));
        i++;
        if (i < j) {
            boolean isObjBound = !queryQuad.getObject().isVariable() || (parentBinding != null && parentBinding.containsKey(Var.alloc(queryQuad.getObject())));
            if (isObjBound) hasNext = false;
            else advanceToNextValid();
        } else {
            hasNext = false;
        }
        return result;
    }
}