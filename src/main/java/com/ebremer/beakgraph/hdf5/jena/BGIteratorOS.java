package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.core.Dictionary;
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

public class BGIteratorOS implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    private final BitPackedUnSignedLongBuffer Bs;
    private final BitPackedUnSignedLongBuffer Ss;
    private long i; 
    private long j; 
    private long gi, pi, oi;
    private boolean hasNext = false;
    private long minSubId = 0;
    private long maxSubId = Long.MAX_VALUE;

    public BGIteratorOS(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        //IO.println("BGIteratorOS "+quad);
        this.parentBinding = bnid;
        this.queryQuad = quad;
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        this.i = 0;
        this.j = 0;
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }
        gi = dict.getGraphs().locate(quad.getGraph());
        if (gi < 1) return;
        pi = dict.getPredicates().locate(quad.getPredicate());
        if (pi < 1) return;
        oi = dict.getObjects().locate(quad.getObject());
        if (oi < 1) return;
        long si = (!quad.getSubject().isVariable()) ? dict.getSubjects().locate(quad.getSubject()) : -1;
        if (!quad.getSubject().isVariable() && si < 1) return;
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        long pStart = (gi == 1) ? 0 : Bp.select1(gi - 1) + 1;
        long pEnd = Bp.select1(gi);
        if (pStart == -1 || pEnd == -1 || pStart > pEnd) return;
        long pPos = findIdInRange(Sp, dict.getPredicates(), pi, pStart, pEnd);
        if (pPos < 0) return;
        BitPackedUnSignedLongBuffer Bo = reader.getBitmapBuffer('O');
        BitPackedUnSignedLongBuffer So = reader.getIDBuffer('O');
        long pRank = pPos + 1; 
        long oStart = (pRank == 1) ? 0 : Bo.select1(pRank - 1) + 1;
        long oEnd = Bo.select1(pRank);
        if (oStart == -1 || oEnd == -1 || oStart > oEnd) return;
        long oPos = findIdInRange(So, dict.getObjects(), oi, oStart, oEnd);
        if (oPos < 0) return;
        long oRank = oPos + 1;
        long sStart = (oRank == 1) ? 0 : Bs.select1(oRank - 1) + 1;
        long sEnd = Bs.select1(oRank);
        if (sStart == -1 || sEnd == -1 || sStart > sEnd) return;
        this.i = sStart;
        this.j = sEnd + 1;
        if (si > 0) {
            long foundIndex = findIdInRange(Ss, dict.getSubjects(), si, i, j - 1);
            if (foundIndex < 0) {
                hasNext = false;
            } else {
                if (si >= minSubId && si <= maxSubId) {
                    i = foundIndex;
                    j = foundIndex + 1;
                    hasNext = true;
                } else {
                    hasNext = false;
                }
            }
        } else {
            while (i < j && Ss.get(i) < minSubId) {
                i++;
            }
            hasNext = (i < j) && (Ss.get(i) <= maxSubId);
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
        if (!var.equals(quad.getSubject())) return;
        long rawResult = dict.getSubjects().search(value);
        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);
        switch (op) {
            case ">" -> minSubId = Math.max(minSubId, found ? id + 1 : id);
            case ">=" -> minSubId = Math.max(minSubId, id);
            case "<" -> maxSubId = Math.min(maxSubId, id - 1);
            case "<=" -> maxSubId = Math.min(maxSubId, found ? id : id - 1);
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
        long currentSubjectId = Ss.get(i);
        if (queryQuad.getGraph().isVariable()) {
            result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        }
        if (queryQuad.getPredicate().isVariable()) {
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(pi, NodeType.PREDICATE));
        }
        if (queryQuad.getObject().isVariable()) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(oi, NodeType.OBJECT));
        }
        if (queryQuad.getSubject().isVariable()) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(currentSubjectId, NodeType.SUBJECT));
        }
        i++;
        if (i < j) {
            long nextId = Ss.get(i);
            hasNext = nextId <= maxSubId;
        } else {
            hasNext = false;
        }
        return result;
    }
}
