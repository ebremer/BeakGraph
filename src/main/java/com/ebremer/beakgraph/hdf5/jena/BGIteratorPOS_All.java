package com.ebremer.beakgraph.hdf5.jena;

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
 * A Full-Scan Iterator for GPOS indices.
 * Handles patterns where Predicate, Object, and Subject are ALL variables (or filtered).
 * Traverses P -> O -> S linearly using bitmap signals to transition levels.
 */
public class BGIteratorPOS_All implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final Quad queryQuad;
    
    // Buffers
    private final BitPackedUnSignedLongBuffer Bo;
    private final BitPackedUnSignedLongBuffer So;
    private final BitPackedUnSignedLongBuffer Bs;
    private final BitPackedUnSignedLongBuffer Ss;
    private final BitPackedUnSignedLongBuffer Sp;
    private final BitPackedUnSignedLongBuffer Bp;

    // Cursors
    private long idxP; // Current Index in Sp (Predicate List)
    private long endP; // End Index in Sp for this Graph
    private long idxO; // Current Index in So (Object List)
    private long idxS; // Current Index in Ss (Subject List)

    // Current Values
    private long curPID;
    private long curOID;
    private long curSID;

    private final long gi; // Graph ID (Fixed)
    private boolean hasNext = false;

    // Filter Bounds
    private long minPid = 0, maxPid = Long.MAX_VALUE;
    private long minObjId = 0, maxObjId = Long.MAX_VALUE;
    private long minSubId = 0, maxSubId = Long.MAX_VALUE;

    public BGIteratorPOS_All(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        this.Bp = reader.getBitmapBuffer('P');
        this.Sp = reader.getIDBuffer('P');

        // 1. Apply Filters
        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }

        // 2. Locate Graph Range
        gi = dict.getGraphs().locate(quad.getGraph());
        if (gi < 1) return;

        // Find Predicate Range for this Graph (L1)
        long pStart = (gi == 1) ? 0 : Bp.select1(gi - 1) + 1;
        long pEnd = Bp.select1(gi);

        if (pStart == -1 || pEnd == -1 || pStart > pEnd) return;

        // 3. Initialize Cursors
        this.idxP = pStart;
        this.endP = pEnd;

        // Calculate initial Object Index based on Predicate Rank
        // rank is 1-based. If pStart=0 (1st predicate), oStart=0.
        // Else, oStart is just after the end of the previous predicate's object block.
        long pRank = idxP + 1;
        this.idxO = (pRank == 1) ? 0 : Bo.select1(pRank - 1) + 1;

        // Calculate initial Subject Index based on Object Rank
        long oRank = idxO + 1;
        this.idxS = (oRank == 1) ? 0 : Bs.select1(oRank - 1) + 1;

        // 4. Load Initial Values
        this.curPID = Sp.get(idxP);
        this.curOID = So.get(idxO);
        // Subject loaded in advance()

        // 5. Find first valid triple
        advance();
    }

    private void advance() {
        hasNext = false;

        while (idxP <= endP) {
            // --- Level 1: Predicate Check ---
            // If current P is outside bounds, we need to skip the whole block
            if (curPID < minPid) {
                skipPredicateBlock();
                continue;
            }
            if (curPID > maxPid) {
                return; // Exceeded max Predicate, done with graph
            }

            // --- Level 2: Object Check ---
            // Note: We are inside a specific Predicate block here. 
            // We iterate Objects until Bo tells us this P block is done.
            
            if (curOID < minObjId) {
                skipObjectBlock();
                // Check if skipping the object moved us to a new Predicate
                if (idxP > endP) return;
                if (Sp.get(idxP) != curPID) {
                    // We moved to a new Predicate, restart loop to check P bounds
                    curPID = Sp.get(idxP);
                    continue;
                }
                continue;
            }
            
            if (curOID > maxObjId) {
                // Skip remaining objects for this Predicate
                skipPredicateBlock();
                continue;
            }

            // --- Level 3: Subject Check ---
            // We are inside a specific Object block.
            this.curSID = Ss.get(idxS);
            
            // Check S bounds
            boolean sValid = (curSID >= minSubId && curSID <= maxSubId);
            
            // Capture state before incrementing
            long validS = curSID;
            long validO = curOID;
            long validP = curPID;
            boolean ready = sValid;

            // --- Transition Logic (Bitmap Walking) ---
            // Check if this Subject is the last one for the current Object
            long sBit = Bs.get(idxS);
            idxS++; // Move to next subject slot

            if (sBit == 1) {
                // End of Subject Block -> Move to next Object
                long oBit = Bo.get(idxO);
                idxO++; // Move to next object slot
                
                if (oBit == 1) {
                    // End of Object Block -> Move to next Predicate
                    idxP++; // Move to next predicate slot
                    if (idxP <= endP) {
                        curPID = Sp.get(idxP);
                    }
                }
                
                // If we are still valid, update Object ID
                if (idxP <= endP) { // Ensure we didn't run off the end
                     // We might be at a new Object now (or new P and new O)
                     // But wait, idxO has advanced. Is it valid?
                     // We rely on the loop condition and `select1` logic for bounds usually,
                     // but here we are streaming. `idxO` points to the NEW object.
                     // Safety check: accessing So requires idxO < numEntries
                     if (idxO < So.getNumEntries()) {
                        curOID = So.get(idxO);
                     }
                }
            }
            
            if (ready) {
                hasNext = true;
                // We must restore the captured IDs for next() because we just advanced the cursors
                // Actually, `next()` uses the class fields. 
                // To handle this cleanly, `next()` should use the values we just validated.
                // I will store them in `next...` variables or just rely on the fact that 
                // advance() leaves the cursors pointing to the *next* candidate, 
                // so we need to return the *current* valid one.
                
                // Better pattern: `advance()` prepares `nextResult`.
                // But since we need to return a BindingNodeId, let's store the IDs.
                this.resP = validP;
                this.resO = validO;
                this.resS = validS;
                return;
            }
        }
    }
    
    private long resP, resO, resS;

    // Helper to skip the current Object's block of Subjects
    private void skipObjectBlock() {
        // We need to find the next '1' in Bs starting from idxS
        // select1 is global rank. We need relative scan.
        // Optimization: Scan locally because chunks are usually small?
        // Or use select1? To use select1, we need the current rank.
        // Rank = idxO + 1.
        long nextSRank = idxO + 1; 
        // The end of the current Object's subject list is at Bs.select1(nextSRank)
        long sEnd = Bs.select1(nextSRank);
        
        // Move S cursor to start of next block
        idxS = sEnd + 1;
        
        // Move O cursor forward
        long oBit = Bo.get(idxO);
        idxO++;
        
        if (oBit == 1) {
            idxP++;
            if (idxP <= endP) curPID = Sp.get(idxP);
        }
        if (idxO < So.getNumEntries()) curOID = So.get(idxO);
    }

    // Helper to skip the current Predicate's block of Objects
    private void skipPredicateBlock() {
        // The end of the current Predicate's object list is at Bo.select1(idxP + 1)
        long pRank = idxP + 1;
        long oEnd = Bo.select1(pRank);
        
        // The Subjects corresponding to this range also need to be skipped.
        // The number of subjects to skip is harder to calculate without chaining select1.
        // The end of the Subject list corresponds to the O-rank `oEnd + 1`.
        long sEnd = Bs.select1(oEnd + 1); // oEnd is 0-based index, rank is +1
        
        idxS = sEnd + 1;
        idxO = oEnd + 1;
        idxP++;
        
        if (idxP <= endP) curPID = Sp.get(idxP);
        if (idxO < So.getNumEntries()) curOID = So.get(idxO);
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
        // Identify which component is being filtered
        int type = 0; // 1=S, 2=P, 3=O
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

        long min = 0, max = Long.MAX_VALUE;
        
        // Get current bounds for the specific type
        switch (type) {
            case 1:
                min = minSubId;
                max = maxSubId;
                break;
            case 2:
                min = minPid;
                max = maxPid;
                break;
            default:
                min = minObjId;
                max = maxObjId;
                break;
        }

        switch (op) {
            case ">" -> min = Math.max(min, found ? id + 1 : id);
            case ">=" -> min = Math.max(min, id);
            case "<" -> max = Math.min(max, id - 1);
            case "<=" -> max = Math.min(max, id);
        }

        // Write back
        switch (type) {
            case 1:
                minSubId = min;
                maxSubId = max;
                break;
            case 2:
                minPid = min;
                maxPid = max;
                break;
            default:
                minObjId = min;
                maxObjId = max;
                break;
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
        if (queryQuad.getPredicate().isVariable()) {
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(resP, NodeType.PREDICATE));
        }
        if (queryQuad.getObject().isVariable()) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(resO, NodeType.OBJECT));
        }
        if (queryQuad.getSubject().isVariable()) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(resS, NodeType.SUBJECT));
        }

        advance(); // Pre-load next
        return result;
    }
}