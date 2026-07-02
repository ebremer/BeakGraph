package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
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
    // Accelerated rank/select directories used for select1; raw B*/S* still used for get().
    private final HDTBitmapDirectory dirS, dirP, dirO;
    private long idxS, endS, idxP, idxO;
    private long curSID, curPID, curOID;
    private long resS, resP, resO;
    private final long gi; 
    private boolean hasNext = false;
    // minSubId starts at 1, not 0: real subject ids are 1-based, and the writer pads
    // empty graph blocks with (S=0,P=0,O=0) dummy rows. Starting at 0 leaked those
    // padding rows as phantom bindings (and extract(0) throws on materialization).
    private long minSubId = 1, maxSubId = Long.MAX_VALUE;
    private long minPid = 0, maxPid = Long.MAX_VALUE;
    private long minObjId = 0, maxObjId = Long.MAX_VALUE;
    private final PositionalDictionaryReader dict;
    private final NodeTable nodeTable;

    public BGIteratorSPO_All(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.queryQuad = quad;
        this.dict = dict;
        this.nodeTable = nodeTable;
        this.Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        this.Bp = reader.getBitmapBuffer('P');
        this.Sp = reader.getIDBuffer('P');
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        this.dirS = reader.getDirectory('S');
        this.dirP = reader.getDirectory('P');
        this.dirO = reader.getDirectory('O');

        if (filter != null && !filter.isEmpty()) {
            analyzeFilters(filter, dict, quad);
        }

        // Resolve the graph the same way the other three iterators behind
        // BGIteratorMaster do: the dispatcher also routes here when the graph
        // VARIABLE is pre-bound in the BindingNodeId, and locate() on the raw
        // variable node returns -1 - silently yielding nothing for a graph
        // that exists.
        if (quad.getGraph().isVariable()) {
            gi = (bnid != null && bnid.containsKey(Var.alloc(quad.getGraph())))
                    ? bnid.get(Var.alloc(quad.getGraph())).getId()
                    : -1;
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) return;

        // Honour concrete subject / object terms named directly in the triple
        // pattern. This iterator otherwise scans every S and O in the graph, so
        // a concrete "<subject> ?p ?o" would wrongly return every triple.
        long concreteSubId = -1;
        if (quad.getSubject().isConcrete()) {
            concreteSubId = dict.getSubjects().locate(quad.getSubject());
            if (concreteSubId < 1) return;
            minSubId = Math.max(minSubId, concreteSubId);
            maxSubId = Math.min(maxSubId, concreteSubId);
        }
        if (quad.getObject().isConcrete()) {
            long oid = dict.getObjects().locate(quad.getObject());
            if (oid < 1) return;
            minObjId = Math.max(minObjId, oid);
            maxObjId = Math.min(maxObjId, oid);
        }

        // -----------------------------------------------------------------
        // LEVEL 1: Subject Range (Graph Scope)
        // -----------------------------------------------------------------
        // select1 is 1-based.
        // Start of Graph gi is the gi-th '1' in Bs.
        long sStart = select1Safe(dirS, Bs,gi);
        
        // Start of Next Graph is the (gi+1)-th '1'.
        long nextGraphStart = select1Safe(dirS, Bs,gi + 1);
        long sEnd = (nextGraphStart == -1) ? (Ss.getNumEntries() - 1) : (nextGraphStart - 1);

        if (sStart == -1 || sStart > sEnd) return;

        // For a concrete subject, binary-search the (ascending) subject list
        // for it instead of scanning the graph's subjects one block at a time.
        long firstS = sStart;
        if (concreteSubId > 0) {
            firstS = binarySearchSubject(concreteSubId, sStart, sEnd);
            if (firstS == -1) return;   // subject not present in this graph
        }
        this.idxS = firstS;
        this.endS = sEnd;

        // -----------------------------------------------------------------
        // LEVEL 2: Predicate Cursor
        // -----------------------------------------------------------------
        // Start of Predicates for Subject `idxS` is the (idxS + 1)-th '1' in Bp.
        this.idxP = select1Safe(dirP, Bp,idxS + 1);

        // -----------------------------------------------------------------
        // LEVEL 3: Object Cursor
        // -----------------------------------------------------------------
        // Start of Objects for Predicate `idxP` is the (idxP + 1)-th '1' in Bo.
        this.idxO = select1Safe(dirO, Bo,idxP + 1);

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

    private long select1Safe(HDTBitmapDirectory dir, BitPackedUnSignedLongBuffer fallback, long rank) {
        if (rank < 1) return -1; // 1-based rank must be >= 1
        // Accelerated O(log n) select via the superblock/block directory when present;
        // fall back to the buffer's linear scan only for indexes written without it.
        return (dir != null) ? dir.select1(rank) : fallback.select1(rank);
    }

    /**
     * Binary search the subject-id buffer Ss (ascending within a graph) for
     * {@code sid} in the inclusive position range [lo, hi]. Returns the buffer
     * position, or -1 if the subject is not present in that range.
     */
    private long binarySearchSubject(long sid, long lo, long hi) {
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long v = Ss.get(mid);
            if (v == sid) return mid;
            if (v < sid) lo = mid + 1;
            else hi = mid - 1;
        }
        return -1;
    }

    private void advance() {
        hasNext = false;
        while (idxS <= endS) {
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
            if (isMatch) {
                this.resS = curSID;
                this.resP = curPID;
                this.resO = curOID;
                hasNext = true;
            }
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
        long nextPStart = select1Safe(dirO, Bo,idxP + 2);
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
        long nextSStartP = select1Safe(dirP, Bp,idxS + 2);
        
        if (nextSStartP == -1) {
            idxP = Sp.getNumEntries();
            idxO = So.getNumEntries();
        } else {
            idxP = nextSStartP;
            // Now align Object cursor to the new Predicate
            long nextSStartO = select1Safe(dirO, Bo,idxP + 1);
            idxO = (nextSStartO == -1) ? So.getNumEntries() : nextSStartO;
        }

        idxS++;
        
        if (idxS <= endS) curSID = Ss.get(idxS);
        if (idxP < Sp.getNumEntries()) curPID = Sp.get(idxP);
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
        int type;
        if (var.equals(quad.getSubject())) type = 1;
        else if (var.equals(quad.getPredicate())) type = 2;
        else if (var.equals(quad.getObject())) type = 3;
        else return;

        // Snap the bound to the edges of the whole value-equal cluster: value-equal
        // but term-distinct literals ("5"^^xsd:int vs "5"^^xsd:integer) occupy
        // adjacent distinct ids, and the raw exact-term insertion point can land
        // inside that cluster, silently dropping qualifying boundary rows.
        long[] c = switch (type) {
            case 1 -> ValueCluster.of(dict.getSubjects(), value);
            case 2 -> ValueCluster.of(dict.getPredicates(), value);
            default -> ValueCluster.of(dict.getObjects(), value);
        };

        long min, max;
        switch (type) {
            case 1 -> { min = minSubId; max = maxSubId; }
            case 2 -> { min = minPid; max = maxPid; }
            default -> { min = minObjId; max = maxObjId; }
        }

        switch (op) {
            case ">" -> min = Math.max(min, c[1] + 1);
            case ">=" -> min = Math.max(min, c[0]);
            case "<" -> max = Math.min(max, c[0] - 1);
            case "<=" -> max = Math.min(max, c[1]);
        }

        switch (type) {
            case 1 -> { minSubId = min; maxSubId = max; }
            case 2 -> { minPid = min; maxPid = max; }
            default -> { minObjId = min; maxObjId = max; }
        }
    }

    // Look-ahead: the next deliverable row, or null. Rows whose repeated-variable
    // bindings conflict are skipped here, so hasNext() only answers true when
    // next() really has a row to return.
    private BindingNodeId pending = null;
    private boolean primed = false;

    /**
     * Builds the next row whose bindings are all compatible. A variable repeated in
     * the pattern (?s ?p ?s, ?x ?x ?o, ...) must bind to the same term in every
     * position it occupies; putCompatible reports a conflict and the row is skipped
     * rather than emitted with the first value. (Subject/object vs predicate repeats
     * span two id-spaces - putCompatible resolves those through the node table.)
     */
    private BindingNodeId computeNext() {
        while (hasNext) {
            BindingNodeId result = new BindingNodeId(parentBinding);
            boolean ok = true;
            if (queryQuad.getGraph().isVariable()) {
                ok = result.putCompatible(Var.alloc(queryQuad.getGraph()), new NodeId(gi, NodeType.GRAPH), nodeTable);
            }
            if (ok && queryQuad.getSubject().isVariable()) {
                ok = result.putCompatible(Var.alloc(queryQuad.getSubject()), new NodeId(resS, NodeType.SUBJECT), nodeTable);
            }
            if (ok && queryQuad.getPredicate().isVariable()) {
                ok = result.putCompatible(Var.alloc(queryQuad.getPredicate()), new NodeId(resP, NodeType.PREDICATE), nodeTable);
            }
            if (ok && queryQuad.getObject().isVariable()) {
                ok = result.putCompatible(Var.alloc(queryQuad.getObject()), new NodeId(resO, NodeType.OBJECT), nodeTable);
            }
            advance();
            if (ok) return result;
        }
        return null;
    }

    private void prime() {
        if (!primed) {
            primed = true;
            pending = computeNext();
        }
    }

    @Override
    public boolean hasNext() {
        prime();
        return pending != null;
    }

    @Override
    public BindingNodeId next() {
        prime();
        if (pending == null) throw new NoSuchElementException();
        BindingNodeId r = pending;
        pending = computeNext();
        return r;
    }
}
