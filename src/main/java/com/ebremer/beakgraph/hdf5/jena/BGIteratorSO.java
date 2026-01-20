package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.Dictionary;
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
 * Iterator for GSPO index where G, S, and P are bound (or fixed), finding O.
 * Optimized with Object-level range filtering and binary search.
 */
public class BGIteratorSO implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    private final BitPackedUnSignedLongBuffer Bs, Ss, Bp, Sp, Bo, So;
    //private final PositionalDictionaryReader dict;
    
    private long i;  // current object index
    private long j;  // end object index (inclusive)
    private long gi, si, pi;
    private boolean hasNext = false;
    
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;

    public BGIteratorSO(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        //this.dict = dict;

        // GSPO Structure mapping
        this.Bs = reader.getBitmapBuffer('S'); 
        this.Ss = reader.getIDBuffer('S');     
        this.Bp = reader.getBitmapBuffer('P'); 
        this.Sp = reader.getIDBuffer('P');     
        this.Bo = reader.getBitmapBuffer('O'); 
        this.So = reader.getIDBuffer('O');     
        
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }
        
        // Resolve Graph
        gi = resolveNode(quad.getGraph(), dict.getGraphs(), bnid);
        if (gi < 1) return;
        
        // Resolve Subject
        si = resolveNode(quad.getSubject(), dict.getSubjects(), bnid);
        if (si < 1) return;

        // Resolve Predicate
        pi = resolveNode(quad.getPredicate(), dict.getPredicates(), bnid);
        if (pi < 1) return;

        // --- Traverse GSPO ---

        // A. Find Subject Index under Graph
        long sStart = select1Safe(Bs, gi);
        long nextGraphStart = select1Safe(Bs, gi + 1);
        long sEnd = (nextGraphStart == -1) ? (Ss.getNumEntries() - 1) : (nextGraphStart - 1);
        
        if (sStart == -1 || sStart > sEnd) return;
        long sIndex = Ss.binarySearch(sStart, sEnd, si);
        if (sIndex < 0) return;

        // B. Find Predicate Index under Subject
        long pStart = select1Safe(Bp, sIndex + 1);
        long nextSStart = select1Safe(Bp, sIndex + 2);
        long pEnd = (nextSStart == -1) ? (Sp.getNumEntries() - 1) : (nextSStart - 1);
        
        if (pStart == -1 || pStart > pEnd) return;
        long pIndex = Sp.binarySearch(pStart, pEnd, pi);
        if (pIndex < 0) return;

        // C. Find Object Range for Predicate
        long rawOStart = select1Safe(Bo, pIndex + 1);
        long nextPStart = select1Safe(Bo, pIndex + 2);
        long rawOEnd = (nextPStart == -1) ? (So.getNumEntries() - 1) : (nextPStart - 1);
        
        if (rawOStart == -1 || rawOStart > rawOEnd) return;

        // D. Apply Specific Object Bound or Range Filters
        long specificObjId = resolveNode(quad.getObject(), dict.getObjects(), bnid);

        if (specificObjId > 0) {
            // Case: Object is bound (e.g., G, S, P, O are all known, just checking existence)
            long foundIdx = So.binarySearch(rawOStart, rawOEnd, specificObjId);
            if (foundIdx >= 0) {
                this.i = foundIdx;
                this.j = foundIdx;
                this.hasNext = true;
            }
        } else {
            // Case: Object is a variable, apply min/max ID range filters
            this.i = (minObjId <= 0) ? rawOStart : So.lowerBound(rawOStart, rawOEnd, minObjId);
            this.j = (maxObjId == Long.MAX_VALUE) ? rawOEnd : So.upperBound(rawOStart, rawOEnd, maxObjId);
            
            if (this.i != -1 && this.i <= this.j) {
                this.hasNext = true;
            }
        }
    }

    private long resolveNode(Node node, Dictionary dictionary, BindingNodeId bnid) {
        if (node.isVariable()) {
            Var v = Var.alloc(node);
            if (bnid != null && bnid.containsKey(v)) {
                return bnid.get(v).getId();
            }
            return -1; // Variable is unbound
        }
        return dictionary.locate(node);
    }

    private long select1Safe(BitPackedUnSignedLongBuffer buffer, long rank) {
        if (rank < 1) return -1;
        return buffer.select1(rank);
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
            case ">" -> "<"; case "<" -> ">"; case ">=" -> "<="; case "<=" -> ">="; default -> op;
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

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();        
        BindingNodeId result = new BindingNodeId(this.parentBinding);
        long currentObjId = So.get(i);
        if (queryQuad.getGraph().isVariable() && !result.containsKey(Var.alloc(queryQuad.getGraph()))) 
            result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        if (queryQuad.getSubject().isVariable() && !result.containsKey(Var.alloc(queryQuad.getSubject()))) 
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(si, NodeType.SUBJECT));
        if (queryQuad.getPredicate().isVariable() && !result.containsKey(Var.alloc(queryQuad.getPredicate()))) 
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        if (queryQuad.getObject().isVariable()) { 
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(currentObjId, NodeType.OBJECT));
        }
        i++;
        hasNext = (i <= j);
        return result;
    }
}
