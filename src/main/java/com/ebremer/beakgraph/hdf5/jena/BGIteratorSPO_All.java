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
 * A Full-Scan Iterator for GSPO indices.
 * Traverses S -> P -> O linearly using Start-Bit (Peek-Ahead) logic.
 * Corrected for 1-based select1 bitmaps.
 */
public class BGIteratorSPO_All implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    
    private final BitPackedUnSignedLongBuffer Bs, Ss, Bp, Sp, Bo, So;
    private long idxS, endS, idxP, idxO; 
    private long curSID, curPID, curOID;
    private long resS, resP, resO;
    private final long gi; 
    private boolean hasNext = false;
    private long minSubId = 0, maxSubId = Long.MAX_VALUE;
    private long minPid = 0, maxPid = Long.MAX_VALUE;
    private long minObjId = 0, maxObjId = Long.MAX_VALUE;
    private final FiveSectionDictionaryReader dict;

    public BGIteratorSPO_All(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        this.dict = dict;
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        this.Bp = reader.getBitmapBuffer('P');
        this.Sp = reader.getIDBuffer('P');
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');

        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }

        gi = dict.getGraphs().locate(quad.getGraph());
        if (gi < 1) return;

        // -----------------------------------------------------------------
        // LEVEL 1: Subject Range (Graph Scope)
        // -----------------------------------------------------------------
        // select1 is 1-based.
        // Start of Graph gi is the gi-th '1' in Bs.
        long sStart = select1Safe(Bs, gi);
        
        // Start of Next Graph is the (gi+1)-th '1'.
        long nextGraphStart = select1Safe(Bs, gi + 1);
        long sEnd = (nextGraphStart == -1) ? (Ss.getNumEntries() - 1) : (nextGraphStart - 1);

        if (sStart == -1 || sStart > sEnd) return;

        this.idxS = sStart;
        this.endS = sEnd;

        // -----------------------------------------------------------------
        // LEVEL 2: Predicate Cursor
        // -----------------------------------------------------------------
        // Start of Predicates for Subject `idxS` is the (idxS + 1)-th '1' in Bp.
        this.idxP = select1Safe(Bp, idxS + 1);

        // -----------------------------------------------------------------
        // LEVEL 3: Object Cursor
        // -----------------------------------------------------------------
        // Start of Objects for Predicate `idxP` is the (idxP + 1)-th '1' in Bo.
        this.idxO = select1Safe(Bo, idxP + 1);

        // --- Safety Checks ---
        // If idxP or idxO are -1 (not found), it means the lists are empty or we overshot.
        // However, select1(1) should always return 0 for non-empty.
        if (idxP == -1 || idxO == -1) return;
        
        // Ensure we are within bounds
        if (idxP >= Sp.getNumEntries() || idxO >= So.getNumEntries()) return;

        // Load Initial Values
        this.curSID = Ss.get(idxS);
        this.curPID = Sp.get(idxP);
        
        advance();
    }

    private long select1Safe(BitPackedUnSignedLongBuffer buffer, long rank) {
        if (rank < 1) return -1; // 1-based rank must be >= 1
        return buffer.select1(rank);
    }

    private void advance() {
        hasNext = false;

        while (idxS <= endS) {
            // 1. Validation
            boolean isMatch = true;

            if (curSID < minSubId) {
                skipSubjectBlock();
                continue;
            } else if (curSID > maxSubId) {
                return; 
            }

            if (curPID < minPid) {
                skipPredicateBlock();
                long nextSID = (idxS <= endS) ? Ss.get(idxS) : -1;
                if (nextSID != curSID) curSID = nextSID;
                continue;
            } else if (curPID > maxPid) {
                skipSubjectBlock();
                continue;
            }

            if (idxO >= So.getNumEntries()) {
                skipPredicateBlock();
                continue;
            }
            this.curOID = So.get(idxO);
            if (curOID < minObjId || curOID > maxObjId) {
                isMatch = false;
            }

            // 2. Capture Result
            if (isMatch) {
                this.resS = curSID;
                this.resP = curPID;
                this.resO = curOID;
                hasNext = true;
            }

            // 3. Transition (Increment Cursors)
            idxO++; 
            
            boolean endOfObjectList = (idxO >= So.getNumEntries()) || (Bo.get(idxO) == 1);

            if (endOfObjectList) {
                idxP++;
                
                boolean endOfPredicateList = (idxP >= Sp.getNumEntries()) || (Bp.get(idxP) == 1);
                
                if (endOfPredicateList) {
                    idxS++;
                    if (idxS <= endS) {
                        curSID = Ss.get(idxS);
                    }
                }
                
                if (idxP < Sp.getNumEntries()) {
                      curPID = Sp.get(idxP);
                }
            }
            
            if (hasNext) return;
        }
    }
    
    private void skipPredicateBlock() {
        // Find start of NEXT predicate block
        // Current Predicate is idxP. Its start was select1(idxP+1).
        // Next Predicate is idxP+1. Its start is select1(idxP+2).
        long nextPStart = select1Safe(Bo, idxP + 2);
        idxO = (nextPStart == -1) ? So.getNumEntries() : nextPStart;
        
        idxP++;
        
        boolean endOfPredicateList = (idxP >= Sp.getNumEntries()) || (Bp.get(idxP) == 1);
        if (endOfPredicateList) {
            idxS++;
            if (idxS <= endS) curSID = Ss.get(idxS);
        }
        
        if (idxP < Sp.getNumEntries()) curPID = Sp.get(idxP);
    }

    private void skipSubjectBlock() {
        // Find start of NEXT Subject block
        // Current Subject idxS. Start was select1(idxS+1).
        // Next Subject idxS+1. Start is select1(idxS+2).
        long nextSStartP = select1Safe(Bp, idxS + 2);
        
        if (nextSStartP == -1) {
            idxP = Sp.getNumEntries();
            idxO = So.getNumEntries();
        } else {
            idxP = nextSStartP;
            // Now align Object cursor to the new Predicate
            long nextSStartO = select1Safe(Bo, idxP + 1);
            idxO = (nextSStartO == -1) ? So.getNumEntries() : nextSStartO;
        }

        idxS++;
        
        if (idxS <= endS) curSID = Ss.get(idxS);
        if (idxP < Sp.getNumEntries()) curPID = Sp.get(idxP);
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
        int type; 
        if (var.equals(quad.getSubject())) type = 1;
        else if (var.equals(quad.getPredicate())) type = 2;
        else if (var.equals(quad.getObject())) type = 3;
        else return;

        long rawResult;
        rawResult = switch (type) {
            case 1 -> dict.getSubjects().search(value);
            case 2 -> dict.getPredicates().search(value);
            default -> dict.getObjects().search(value);
        };

        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);

        long min, max;
        switch (type) {
            case 1 -> { min = minSubId; max = maxSubId; }
            case 2 -> { min = minPid; max = maxPid; }
            default -> { min = minObjId; max = maxObjId; }
        }

        switch (op) {
            case ">" -> min = Math.max(min, found ? id + 1 : id);
            case ">=" -> min = Math.max(min, id);
            case "<" -> max = Math.min(max, id - 1);
            case "<=" -> max = Math.min(max, id);
        }

        switch (type) {
            case 1 -> { minSubId = min; maxSubId = max; }
            case 2 -> { minPid = min; maxPid = max; }
            default -> { minObjId = min; maxObjId = max; }
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();

        BindingNodeId result = new BindingNodeId(parentBinding);
        
        if (queryQuad.getGraph().isVariable()) {
            result.put(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH));
        }
        if (queryQuad.getSubject().isVariable()) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(resS, NodeType.SUBJECT));
        }
        if (queryQuad.getPredicate().isVariable()) {
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(resP, NodeType.PREDICATE));
        }
        if (queryQuad.getObject().isVariable()) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(resO, NodeType.OBJECT));
        }

        advance(); 
        return result;
    }
}