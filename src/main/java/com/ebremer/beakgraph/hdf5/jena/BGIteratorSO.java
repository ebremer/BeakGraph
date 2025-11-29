package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.FiveSectionDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprList;

public class BGIteratorSO implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    private final BitPackedUnSignedLongBuffer So;
    private long i; 
    private final long j;
    private long gi;
    private long si;
    private long pi;
    private boolean hasNext = false;
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;

    public BGIteratorSO(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        BitPackedUnSignedLongBuffer Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');        
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }
        if (quad.getGraph().isVariable() && bnid.containsKey(Var.alloc(quad.getGraph()))) {
            gi = bnid.get(Var.alloc(quad.getGraph())).getId();
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) { this.j = 0; return; }
        if (quad.getSubject().isVariable() && bnid.containsKey(Var.alloc(quad.getSubject()))) {
            si = bnid.get(Var.alloc(quad.getSubject())).getId();
        } else {
            si = dict.getSubjects().locate(quad.getSubject());
        }
        if (si < 1) { this.j = 0; return; }
        if (quad.getPredicate().isVariable() && bnid.containsKey(Var.alloc(quad.getPredicate()))) {
            pi = bnid.get(Var.alloc(quad.getPredicate())).getId();
        } else {
            pi = dict.getPredicates().locate(quad.getPredicate());
        }
        if (pi < 1) { this.j = 0; return; }
        long specificObjId = -1;
        boolean isObjBound = !quad.getObject().isVariable() || bnid.containsKey(Var.alloc(quad.getObject()));        
        if (isObjBound) {
            if (quad.getObject().isVariable()) {
                specificObjId = bnid.get(Var.alloc(quad.getObject())).getId();
            } else {
                specificObjId = dict.getObjects().locate(quad.getObject());
            }
            if (specificObjId < 1 || specificObjId < minObjId || specificObjId > maxObjId) {
                this.j = 0; return; 
            }
        }
        BitPackedUnSignedLongBuffer Bs = reader.getBitmapBuffer('S');
        BitPackedUnSignedLongBuffer Ss = reader.getIDBuffer('S');
        long sStart = (gi == 1) ? 0 : Bs.select1(gi - 1) + 1;
        long sEnd = Bs.select1(gi);
        if (sStart > sEnd) { this.j = 0; return; }
        long sIndex = findIdInRange(Ss, dict.getSubjects(), si, sStart, sEnd);
        if (sIndex < 0) { this.j = 0; return; }
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        long sRank = sIndex + 1; 
        long pStart = (sRank == 1) ? 0 : Bp.select1(sRank - 1) + 1;
        long pEnd = Bp.select1(sRank);
        if (pStart > pEnd) { this.j = 0; return; }        
        long pIndex = findIdInRange(Sp, dict.getPredicates(), pi, pStart, pEnd);
        if (pIndex < 0) { this.j = 0; return; }
        long pRank = pIndex + 1;
        long oStart = (pRank == 1) ? 0 : Bo.select1(pRank - 1) + 1;
        long oEnd = Bo.select1(pRank);
        if (oStart > oEnd) {
            this.j = 0;
            return;
        }
        this.i = oStart;
        this.j = oEnd + 1;        
        if (specificObjId > 0) {
            long foundIndex = findIdInRange(So, dict.getObjects(), specificObjId, i, j - 1);
            if (foundIndex >= 0) {
                i = foundIndex;
                hasNext = true; 
            } else {
                hasNext = false;
            }
        } else {
            while ((i < j) && (So.get(i) < minObjId)) {
                i++;
            }
            hasNext = ((i < j) && (So.get(i) <= maxObjId));
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
            case ">" -> "<";
            case "<" -> ">";
            case ">=" -> "<=";
            case "<=" -> ">=";
            default -> op;
        };
    }

    private void applyBound(Var var, String op, org.apache.jena.graph.Node value, FiveSectionDictionaryReader dict, Quad quad) {
        if (!var.equals(quad.getObject())) return;
        long rawResult = dict.getObjects().search(value);
        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);
        switch (op) {
            case ">" -> {
                long bound = found ? id + 1 : id;
                minObjId = Math.max(minObjId, bound);
            }
            case ">=" -> {
                long bound = id;
                minObjId = Math.max(minObjId, bound);
            }
            case "<" -> {
                long bound = id - 1;
                maxObjId = Math.min(maxObjId, bound);
            }
            case "<=" -> {
                long bound = found ? id : id - 1;
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
        
        if (queryQuad.getGraph().isVariable() && !result.containsKey(Var.alloc(queryQuad.getGraph()))) {
            result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        }
        if (queryQuad.getSubject().isVariable() && !result.containsKey(Var.alloc(queryQuad.getSubject()))) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(si, NodeType.SUBJECT));
        }
        if (queryQuad.getPredicate().isVariable() && !result.containsKey(Var.alloc(queryQuad.getPredicate()))) {
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        }
        if (queryQuad.getObject().isVariable() && !result.containsKey(Var.alloc(queryQuad.getObject()))) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(currentObjectId, NodeType.OBJECT));
        }
        i++;
        if (i < j) {
            boolean isObjBound = !queryQuad.getObject().isVariable() || (parentBinding != null && parentBinding.containsKey(Var.alloc(queryQuad.getObject())));
            if (isObjBound) {
                 hasNext = false;
            } else {
                 hasNext = So.get(i) <= maxObjId;
            }
        } else {
            hasNext = false;
        }        
        return result;
    }
}