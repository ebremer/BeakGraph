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

public class BGIteratorPOS implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    private final BitPackedUnSignedLongBuffer Bo;
    private final BitPackedUnSignedLongBuffer So;
    private final BitPackedUnSignedLongBuffer Bs;
    private final BitPackedUnSignedLongBuffer Ss;
    private long currentOIndex;
    private long endOIndex;
    private long currentSIndex;
    private long endSIndex;
    private long gi, pi;
    private boolean hasNext = false;
    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;
    private long minSubId = 0;
    private long maxSubId = Long.MAX_VALUE;

    public BGIteratorPOS(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }
        gi = dict.getGraphs().locate(quad.getGraph());
        pi = dict.getPredicates().locate(quad.getPredicate());
        if (gi < 1 || pi < 1) return;
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        long pStart = (gi == 1) ? 0 : Bp.select1(gi - 1) + 1;
        long pEnd = Bp.select1(gi);
        if (pStart == -1 || pEnd == -1 || pStart > pEnd) return;
        long pPos = -1;
        for (long k = pStart; k <= pEnd; k++) {
            if (Sp.get(k) == pi) {
                pPos = k;
                break;
            }
        }
        if (pPos == -1) return;
        long pRank = pPos + 1; 
        this.currentOIndex = (pRank == 1) ? 0 : Bo.select1(pRank - 1) + 1;
        this.endOIndex = Bo.select1(pRank);
        
        if (currentOIndex != -1 && endOIndex != -1 && currentOIndex <= endOIndex) {
            prepareNextSubjectRange();
        }
    }

    private void analyzeFilters(ExprList filter, FiveSectionDictionaryReader dict, Quad quad) {
        for (Expr expr : filter.getList()) {
            if (expr instanceof ExprFunction2 func) {
                Expr left = func.getArg1();
                Expr right = func.getArg2();                
                String opcode = func.getOpName();                
                // Check if the pattern is: ?var OP Constant
                if (left.isVariable() && right.isConstant()) {
                    Var var = left.asVar();
                    Node value = right.getConstant().asNode();
                    applyBound(var, opcode, value, dict, quad);
                } 
                // Check if the pattern is: Constant OP ?var (Reverse logic)
                else if (left.isConstant() && right.isVariable()) {
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
        boolean isSubject = var.equals(quad.getSubject());
        boolean isObject = var.equals(quad.getObject());
        if (!isSubject && !isObject) return;
        long rawResult;
        if (isSubject) {
            rawResult = dict.getSubjects().search(value);
        } else {
            rawResult = dict.getObjects().search(value);
        }
        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);
        if (isSubject) {
            updateBounds(op, id, found, true);
        } else {
            updateBounds(op, id, found, false);
        }
    }

    private void updateBounds(String op, long id, boolean found, boolean isSubject) {
        long newMin = isSubject ? minSubId : minObjId;
        long newMax = isSubject ? maxSubId : maxObjId;
        switch (op) {
            case ">" -> {
                long bound = found ? id + 1 : Math.max(0, id - 1);
                newMin = Math.max(newMin, bound);
            }
            case ">=" -> {
                long bound = found ? id : Math.max(0, id - 1);
                newMin = Math.max(newMin, bound);
            }
            case "<" -> {
                newMax = Math.min(newMax, id - 1);
            }
            case "<=" -> {
                long bound = found ? id : id;
                newMax = Math.min(newMax, bound);
            }
        }

        if (isSubject) {
            minSubId = newMin;
            maxSubId = newMax;
        } else {
            minObjId = newMin;
            maxObjId = newMax;
        }
    }
    
    private void prepareNextSubjectRange() {
        hasNext = false;
        while (currentOIndex <= endOIndex) {
            long objId = So.get(currentOIndex);
            if (objId < minObjId) {
                currentOIndex++; 
                continue;
            }
            if (objId > maxObjId) {
                return; 
            }
            long oRank = currentOIndex + 1;
            long sStart = (oRank == 1) ? 0 : Bs.select1(oRank - 1) + 1;
            long sEnd = Bs.select1(oRank);
            if (sStart != -1 && sEnd != -1 && sStart <= sEnd) {
                this.currentSIndex = sStart;
                this.endSIndex = sEnd;
                if (advanceToNextValidSubject()) {
                    this.hasNext = true;
                    return;
                }
            }
            currentOIndex++;
        }
    }

    private boolean advanceToNextValidSubject() {
        while (currentSIndex <= endSIndex) {
            long subId = Ss.get(currentSIndex);            
            if (subId < minSubId) {
                currentSIndex++;
                continue;
            }
            return subId <= maxSubId; 
        }
        return false;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();
        BindingNodeId result = new BindingNodeId(parentBinding);        
        long currentObjId = So.get(currentOIndex);
        long currentSubId = Ss.get(currentSIndex);
        if (queryQuad.getGraph().isVariable()) {
            result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        }
        if (queryQuad.getPredicate().isVariable()) {
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        }
        if (queryQuad.getObject().isVariable()) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(currentObjId, NodeType.OBJECT));
        }
        if (queryQuad.getSubject().isVariable()) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(currentSubId, NodeType.SUBJECT));
        }
        currentSIndex++;
        if (advanceToNextValidSubject()) {
            return result;
        }
        currentOIndex++;
        prepareNextSubjectRange();
        return result;
    }
}
