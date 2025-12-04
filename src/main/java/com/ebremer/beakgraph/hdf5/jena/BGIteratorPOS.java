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
 * Iterator for GPOS index where G and P are bound (or filtered), iterating S and O.
 * Corrected for 1-based select1 (Start-Bit) logic and compilation errors.
 */
public class BGIteratorPOS implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    
    private final BitPackedUnSignedLongBuffer Bo, So, Bs, Ss;
    
    private long currentOIndex; 
    private long endOIndex;     
    private long currentSIndex; 
    private long endSIndex;     
    
    // Removed final to fix compilation error "variable might not have been initialized"
    private long gi, pi;
    private boolean hasNext = false;
    
    private long minObjId = 0, maxObjId = Long.MAX_VALUE;
    private long minSubId = 0, maxSubId = Long.MAX_VALUE;

    public BGIteratorPOS(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        
        // GPOS Structure: G -> P -> O -> S
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }
        
        // 1. Resolve Graph (G)
        if (quad.getGraph().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getGraph()))) {
                gi = bnid.get(Var.alloc(quad.getGraph())).getId();
            } else {
                return; 
            }
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) return;
        
        // 2. Resolve Predicate (P)
        if (quad.getPredicate().isVariable()) {
            if (bnid != null && bnid.containsKey(Var.alloc(quad.getPredicate()))) {
                pi = bnid.get(Var.alloc(quad.getPredicate())).getId();
            } else {
                return; 
            }
        } else {
            pi = dict.getPredicates().locate(quad.getPredicate());
        }
        if (pi < 1) return;

        // 3. Resolve Object (O) - Optional Filter
        if (!quad.getObject().isVariable()) {
            long oid = dict.getObjects().locate(quad.getObject());
            if (oid < 1) return;
            if (oid > minObjId) minObjId = oid;
            if (oid < maxObjId) maxObjId = oid;
        } else if (bnid != null && bnid.containsKey(Var.alloc(quad.getObject()))) {
            long oid = bnid.get(Var.alloc(quad.getObject())).getId();
            if (oid > minObjId) minObjId = oid;
            if (oid < maxObjId) maxObjId = oid;
        }

        // 4. Resolve Subject (S) - Optional Filter
        if (!quad.getSubject().isVariable()) {
            long sid = dict.getSubjects().locate(quad.getSubject());
            if (sid < 1) return;
            if (sid > minSubId) minSubId = sid;
            if (sid < maxSubId) maxSubId = sid;
        } else if (bnid != null && bnid.containsKey(Var.alloc(quad.getSubject()))) {
            long sid = bnid.get(Var.alloc(quad.getSubject())).getId();
            if (sid > minSubId) minSubId = sid;
            if (sid < maxSubId) maxSubId = sid;
        }

        // --- 5. Traverse GPOS Structure (Start-Bit Logic) ---

        // A. Find Predicate Range for Graph 'gi'
        // Bp marks starts of P-lists for Graphs.
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        
        // Start: The gi-th '1' (since gi is 1-based)
        long pStart = select1Safe(Bp, gi);
        // End: The (gi+1)-th '1' marks the next graph
        long nextP = select1Safe(Bp, gi + 1);
        long pEnd = (nextP == -1) ? (Sp.getNumEntries() - 1) : (nextP - 1);
        
        if (pStart == -1 || pStart > pEnd) return;
        
        // B. Find Specific Predicate 'pi' in range
        long pIndex = -1;
        for (long k = pStart; k <= pEnd; k++) {
            if (Sp.get(k) == pi) {
                pIndex = k;
                break;
            }
        }
        if (pIndex < 0) return;

        // C. Find Object Range for Predicate 'pIndex'
        // Bo marks starts of O-lists for Predicates.
        // pIndex is 0-based absolute. We want the (pIndex+1)-th '1'.
        
        long oStart = select1Safe(Bo, pIndex + 1);
        long nextO = select1Safe(Bo, pIndex + 2);
        long oEnd = (nextO == -1) ? (So.getNumEntries() - 1) : (nextO - 1);
        
        if (oStart == -1 || oStart > oEnd) return;
        
        this.currentOIndex = oStart;
        this.endOIndex = oEnd;
        
        prepareNextSubjectRange();
    }

    private long select1Safe(BitPackedUnSignedLongBuffer buffer, long rank) {
        if (rank < 1) return -1;
        return buffer.select1(rank);
    }

    private void prepareNextSubjectRange() {
        hasNext = false;
        
        while (currentOIndex <= endOIndex) {
            long objId = So.get(currentOIndex);
            
            // Check Object Filters
            if (objId < minObjId) {
                currentOIndex++;
                continue;
            }
            if (objId > maxObjId) {
                return; // Objects are sorted, done
            }
            
            // D. Level 4: Find Subject Range for Object 'currentOIndex'
            // Bs marks starts of S-lists for Objects
            long sStart = select1Safe(Bs, currentOIndex + 1);
            long nextS = select1Safe(Bs, currentOIndex + 2);
            long sEnd = (nextS == -1) ? (Ss.getNumEntries() - 1) : (nextS - 1);
            
            if (sStart != -1 && sStart <= sEnd) {
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
            if (subId > maxSubId) {
                return false; 
            }
            
            return true;
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
            case ">" -> newMin = Math.max(newMin, found ? id + 1 : id);
            case ">=" -> newMin = Math.max(newMin, found ? id : id);
            case "<" -> newMax = Math.min(newMax, id - 1);
            case "<=" -> newMax = Math.min(newMax, found ? id : id - 1);
        }
        
        if (isSubject) {
            minSubId = newMin;
            maxSubId = newMax;
        } else {
            minObjId = newMin;
            maxObjId = newMax;
        }
    }
}