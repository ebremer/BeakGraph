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
 * Handles patterns where Subject, Predicate, and Object are ALL variables (or filtered).
 * Traverses S -> P -> O linearly using bitmap signals to transition levels.
 */
public class BGIteratorSPO_All implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    
    // Buffers (GSPO Structure)
    // Level 1: Subject (Bs, Ss)
    // Level 2: Predicate (Bp, Sp)
    // Level 3: Object (Bo, So)
    private final BitPackedUnSignedLongBuffer Bs;
    private final BitPackedUnSignedLongBuffer Ss;
    private final BitPackedUnSignedLongBuffer Bp;
    private final BitPackedUnSignedLongBuffer Sp;
    private final BitPackedUnSignedLongBuffer Bo;
    private final BitPackedUnSignedLongBuffer So;

    // Cursors
    private long idxS; // Current Index in Ss (Subject List - Level 1)
    private long endS; // End Index in Ss for this Graph
    private long idxP; // Current Index in Sp (Predicate List - Level 2)
    private long idxO; // Current Index in So (Object List - Level 3)

    // Current Values
    private long curSID;
    private long curPID;
    private long curOID;

    private final long gi; // Graph ID (Fixed)
    private boolean hasNext = false;

    // Filter Bounds
    private long minSubId = 0, maxSubId = Long.MAX_VALUE;
    private long minPid = 0, maxPid = Long.MAX_VALUE;
    private long minObjId = 0, maxObjId = Long.MAX_VALUE;

    public BGIteratorSPO_All(FiveSectionDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        
        // Load buffers mapping GSPO structure
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        this.Bp = reader.getBitmapBuffer('P');
        this.Sp = reader.getIDBuffer('P');
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');

        // 1. Apply Filters
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }

        // 2. Locate Graph Range
        gi = dict.getGraphs().locate(quad.getGraph());
        if (gi < 1) return;

        // Find Subject Range for this Graph (L1 in GSPO)
        long sStart = (gi == 1) ? 0 : Bs.select1(gi - 1) + 1;
        long sEnd = Bs.select1(gi);

        if (sStart == -1 || sEnd == -1 || sStart > sEnd) return;

        // 3. Initialize Cursors
        this.idxS = sStart;
        this.endS = sEnd;

        // Calculate initial Predicate Index based on Subject Rank (L2)
        // rank is 1-based. 
        long sRank = idxS + 1;
        this.idxP = (sRank == 1) ? 0 : Bp.select1(sRank - 1) + 1;

        // Calculate initial Object Index based on Predicate Rank (L3)
        long pRank = idxP + 1;
        this.idxO = (pRank == 1) ? 0 : Bo.select1(pRank - 1) + 1;

        // 4. Load Initial Values
        this.curSID = Ss.get(idxS);
        this.curPID = Sp.get(idxP);
        // Object loaded in advance()

        // 5. Find first valid triple
        advance();
    }

    private void advance() {
        hasNext = false;

        while (idxS <= endS) {
            // --- Level 1: Subject Check ---
            if (curSID < minSubId) {
                skipSubjectBlock();
                continue;
            }
            if (curSID > maxSubId) {
                return; // Exceeded max Subject, done with graph
            }

            // --- Level 2: Predicate Check ---
            // We are inside a specific Subject block.
            
            if (curPID < minPid) {
                skipPredicateBlock();
                // Check if skipping P moved us to a new Subject
                if (idxS > endS) return;
                if (Ss.get(idxS) != curSID) {
                    // We moved to a new Subject, restart loop to check S bounds
                    curSID = Ss.get(idxS);
                    continue;
                }
                continue;
            }
            
            if (curPID > maxPid) {
                // Skip remaining predicates for this Subject
                skipSubjectBlock();
                continue;
            }

            // --- Level 3: Object Check ---
            // We are inside a specific Predicate block.
            this.curOID = So.get(idxO);
            
            // Check O bounds
            boolean oValid = (curOID >= minObjId && curOID <= maxObjId);
            
            // Capture state before incrementing
            long validS = curSID;
            long validP = curPID;
            long validO = curOID;
            boolean ready = oValid;

            // --- Transition Logic (Bitmap Walking) ---
            // Check if this Object is the last one for the current Predicate
            long oBit = Bo.get(idxO);
            idxO++; // Move to next object slot

            if (oBit == 1) {
                // End of Object Block -> Move to next Predicate
                long pBit = Bp.get(idxP);
                idxP++; // Move to next predicate slot
                
                if (pBit == 1) {
                    // End of Predicate Block -> Move to next Subject
                    idxS++; // Move to next subject slot
                    if (idxS <= endS) {
                        curSID = Ss.get(idxS);
                    }
                }
                
                // If we are still valid, update Predicate ID
                if (idxS <= endS) { 
                     if (idxP < Sp.getNumEntries()) {
                        curPID = Sp.get(idxP);
                     }
                }
            }
            
            if (ready) {
                hasNext = true;
                this.resS = validS;
                this.resP = validP;
                this.resO = validO;
                return;
            }
        }
    }
    
    private long resS, resP, resO;

    // Helper to skip the current Predicate's block of Objects
    private void skipPredicateBlock() {
        // We need to find the next '1' in Bo starting from idxO
        // Rank = idxP + 1.
        long nextPRank = idxP + 1; 
        // The end of the current Predicate's object list is at Bo.select1(nextPRank)
        long oEnd = Bo.select1(nextPRank);
        
        // Move O cursor to start of next block
        idxO = oEnd + 1;
        
        // Move P cursor forward
        long pBit = Bp.get(idxP);
        idxP++;
        
        if (pBit == 1) {
            idxS++;
            if (idxS <= endS) curSID = Ss.get(idxS);
        }
        if (idxP < Sp.getNumEntries()) curPID = Sp.get(idxP);
    }

    // Helper to skip the current Subject's block of Predicates
    private void skipSubjectBlock() {
        // The end of the current Subject's predicate list is at Bp.select1(idxS + 1)
        long sRank = idxS + 1;
        long pEnd = Bp.select1(sRank);
        
        // The Objects corresponding to this range also need to be skipped.
        // The end of the Object list corresponds to the P-rank `pEnd + 1`.
        long oEnd = Bo.select1(pEnd + 1); // pEnd is 0-based index, rank is +1
        
        idxO = oEnd + 1;
        idxP = pEnd + 1;
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
        // Identify which component is being filtered
        int type = 0; // 1=S, 2=P, 3=O
        if (var.equals(quad.getSubject())) type = 1;
        else if (var.equals(quad.getPredicate())) type = 2;
        else if (var.equals(quad.getObject())) type = 3;
        else return;

        long rawResult;
        if (type == 1) rawResult = dict.getSubjects().search(value);
        else if (type == 2) rawResult = dict.getPredicates().search(value);
        else rawResult = dict.getObjects().search(value);

        long id = (rawResult >= 0) ? rawResult : -rawResult - 1;
        boolean found = (rawResult >= 0);

        long min = 0, max = Long.MAX_VALUE;
        
        // Get current bounds for the specific type
        if (type == 1) { min = minSubId; max = maxSubId; }
        else if (type == 2) { min = minPid; max = maxPid; }
        else { min = minObjId; max = maxObjId; }

        switch (op) {
            case ">" -> min = Math.max(min, found ? id + 1 : id);
            case ">=" -> min = Math.max(min, id);
            case "<" -> max = Math.min(max, id - 1);
            case "<=" -> max = Math.min(max, id);
        }

        // Write back
        if (type == 1) { minSubId = min; maxSubId = max; }
        else if (type == 2) { minPid = min; maxPid = max; }
        else { minObjId = min; maxObjId = max; }
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

        advance(); // Pre-load next
        return result;
    }
}