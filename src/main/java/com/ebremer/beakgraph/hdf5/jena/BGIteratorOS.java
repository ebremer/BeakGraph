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
 * Iterator for GPOS index where G, P, and O are bound, finding S.
 * Structure: Graph -> Predicate -> Object -> Subject
 */
public class BGIteratorOS implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    
    private final BitPackedUnSignedLongBuffer Bp, Sp, Bo, So, Bs, Ss;
    
    private long i; 
    private long j;
    private long gi, pi, oi;
    private boolean hasNext = false;
    
    private long minSubId = 0;
    private long maxSubId = Long.MAX_VALUE;

    public BGIteratorOS(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
    //    IO.println("BGIteratorOS Init: " + quad);
        this.parentBinding = bnid;
        this.queryQuad = quad;
        
        // GPOS Structure
        this.Bp = reader.getBitmapBuffer('P'); 
        this.Sp = reader.getIDBuffer('P');     
        this.Bo = reader.getBitmapBuffer('O'); 
        this.So = reader.getIDBuffer('O');     
        this.Bs = reader.getBitmapBuffer('S'); 
        this.Ss = reader.getIDBuffer('S');     
        
        if (filter != null && !filter.isEmpty()) analyzeFilters(filter, dict, quad);
        
        // 1. Resolve Graph
        if (quad.getGraph().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getGraph()))) gi = bnid.get(Var.alloc(quad.getGraph())).getId();
            else return; 
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) { IO.println("OS: Graph not found"); return; }
        
        // 2. Resolve Predicate
        if (quad.getPredicate().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getPredicate()))) pi = bnid.get(Var.alloc(quad.getPredicate())).getId();
            else return; 
        } else {
            pi = dict.getPredicates().locate(quad.getPredicate());
        }
        if (pi < 1) { IO.println("OS: Predicate not found"); return; }

        // 3. Resolve Object
        if (quad.getObject().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getObject()))) oi = bnid.get(Var.alloc(quad.getObject())).getId();
            else return; 
        } else {
            oi = dict.getObjects().locate(quad.getObject());
        }
        if (oi < 1) { IO.println("OS: Object not found: " + quad.getObject()); return; }

        // 4. Resolve Subject Filter
        long specificSubId = -1;
        boolean isSubBound = !quad.getSubject().isVariable() || (bnid != null && bnid.containsKey(Var.alloc(quad.getSubject())));
        if (isSubBound) {
             if (quad.getSubject().isVariable()) specificSubId = bnid.get(Var.alloc(quad.getSubject())).getId();
             else specificSubId = dict.getSubjects().locate(quad.getSubject());
             
             if (specificSubId < 1) { IO.println("OS: Bound Subject not found"); return; }
        }

     //   IO.println("OS Search: G=" + gi + " P=" + pi + " O=" + oi);

        // --- Traverse GPOS ---

        // A. Level 2: Predicate Range for G
        long pStart = select1Safe(Bp, gi);
        long nextGraphStart = select1Safe(Bp, gi + 1);
        long pEnd = (nextGraphStart == -1) ? (Sp.getNumEntries() - 1) : (nextGraphStart - 1);
        
        if (pStart == -1 || pStart > pEnd) { IO.println("OS: No Predicates for Graph"); return; }
        
        // B. Find Predicate Index
        long pIndex = -1;
        for (long k = pStart; k <= pEnd; k++) {
            if (Sp.get(k) == pi) {
                pIndex = k;
                break;
            }
        }
        if (pIndex < 0) { IO.println("OS: Predicate " + pi + " not found in Graph " + gi); return; }

        // C. Level 3: Object Range for P
        long oStart = select1Safe(Bo, pIndex + 1);
        long nextPStart = select1Safe(Bo, pIndex + 2);
        long oEnd = (nextPStart == -1) ? (So.getNumEntries() - 1) : (nextPStart - 1);
        
        if (oStart == -1 || oStart > oEnd) return;
        
        // D. Find Object Index
        long oIndex = -1;
        for (long k = oStart; k <= oEnd; k++) {
            if (So.get(k) == oi) {
                oIndex = k;
                break;
            }
        }
        if (oIndex < 0) { IO.println("OS: Object " + oi + " not found under Predicate " + pi); return; }

        // E. Level 4: Subject Range for O
        long sStart = select1Safe(Bs, oIndex + 1);
        long nextOStart = select1Safe(Bs, oIndex + 2);
        long sEnd = (nextOStart == -1) ? (Ss.getNumEntries() - 1) : (nextOStart - 1);
        
        if (sStart == -1 || sStart > sEnd) { IO.println("OS: No Subjects for Object " + oi); return; }
        
        this.i = sStart;
        this.j = sEnd + 1;
        
     //   IO.println("OS: Found Subject Range [" + sStart + " - " + sEnd + "]");

        // F. Initialize
        if (specificSubId > 0) {
            boolean found = false;
            for (long k = i; k < j; k++) {
                if (Ss.get(k) == specificSubId) {
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
            long subId = Ss.get(i);
            
            if (subId < minSubId) {
                i++;
                continue;
            }
            // Removed strict optimization for safety
            if (subId > maxSubId) {
                i++;
                continue; 
            }
            
            // IO.println("OS Yielding Subject: " + subId);
            hasNext = true;
            return;
        }
    }

    // ... Filter methods (copy from previous classes) ...
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
        if (!var.equals(quad.getSubject())) return;
        long rawResult = dict.getSubjects().search(value);
        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);
        switch (op) {
            case ">" -> {
                 long target = found ? id + 1 : id;
                 if (Long.compareUnsigned(target, minSubId) > 0) minSubId = target;
            }
            case ">=" -> {
                 if (Long.compareUnsigned(id, minSubId) > 0) minSubId = id;
            }
            case "<" -> {
                 if (id == 0) { maxSubId = 0; minSubId = 1; } else {
                     long target = id - 1;
                     if (Long.compareUnsigned(target, maxSubId) < 0) maxSubId = target;
                 }
            }
            case "<=" -> {
                 long target = found ? id : id - 1;
                 if (id == 0 && !found) { maxSubId = 0; minSubId = 1; } else {
                     if (Long.compareUnsigned(target, maxSubId) < 0) maxSubId = target;
                 }
            }
        }
    }

    @Override
    public boolean hasNext() {
        //IO.println("BGIteratorOS - hasNext");
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();
        BindingNodeId result = new BindingNodeId(this.parentBinding);
        long currentSubjectId = Ss.get(i);
        if (queryQuad.getGraph().isVariable()) result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        if (queryQuad.getPredicate().isVariable()) result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        if (queryQuad.getObject().isVariable()) result.put(Var.alloc(queryQuad.getObject()), new NodeId(oi, NodeType.OBJECT));
        if (queryQuad.getSubject().isVariable()) result.put(Var.alloc(queryQuad.getSubject()), new NodeId(currentSubjectId, NodeType.SUBJECT));
        i++;
        if (i < j) {
            boolean isSubBound = !queryQuad.getSubject().isVariable() || (parentBinding != null && parentBinding.containsKey(Var.alloc(queryQuad.getSubject())));
            if (isSubBound) hasNext = false;
            else advanceToNextValid();
        } else {
            hasNext = false;
        }
        return result;
    }
}