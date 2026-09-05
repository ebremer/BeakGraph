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
import org.apache.jena.sparql.expr.ExprList;

/**
 * A Full-Scan Iterator for GSPO indices.
 * Traverses S -> P -> O linearly using Start-Bit (Peek-Ahead) logic.
 * Corrected for 1-based select1 bitmaps.
 */
public class BGIteratorSPO_All implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;

    private final BitPackedUnSignedLongBuffer Ss, Bp, Sp, Bo, So;
    // Accelerated rank/select directories used for select1; raw B*/S* still used for get().
    private final HDTBitmapDirectory dirP, dirO;
    // Word-caching probes for the per-row "did a new block start here" checks:
    // idxO/idxP advance monotonically, so one word read serves up to 64 probes.
    private final BitPackedUnSignedLongBuffer.BitReader boBit, bpBit;
    private final long spNum, soNum;
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
    private final NodeTable nodeTable;

    // Row-emission plan (see BGIteratorSO): Var.alloc hoisted out of the row loop.
    // All four stay putCompatible - a variable repeated across positions (or
    // pre-bound in the parent) must agree per row.
    private Var gVar, sVar, pVar, oVar;
    private long gId;

    // Triple-term pattern in the object position: this scan-shaped iterator
    // formerly dropped the constraint entirely (a var-containing triple term is
    // not isConcrete()) and returned every row - the one-classifier rule, CHANGELOG.md "Format v5 design notes". Candidates
    // now unify per row in computeNext; null for every other shape.
    private TripleTermMatcher ttMatcher;

    public BGIteratorSPO_All(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this(dict, reader, bnid, quad, filter, nodeTable, -1, Long.MAX_VALUE);
    }

    /**
     * Range-restricted variant for parallel scanning (see ScanChunks): only
     * subject POSITIONS within [sPosLo, sPosHi] (intersected with the graph's
     * own subject range) are walked. Each subject's whole P/O sub-tree belongs
     * to the chunk owning its position, so chunks neither split nor duplicate rows.
     */
    /** Number of scans constructed (chunks included) - tests pin what does NOT route here. */
    public static final java.util.concurrent.atomic.AtomicLong HITS = new java.util.concurrent.atomic.AtomicLong();

    BGIteratorSPO_All(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable, long sPosLo, long sPosHi) {
        HITS.incrementAndGet();
        this.parentBinding = bnid;
        this.nodeTable = nodeTable;
        BitPackedUnSignedLongBuffer Bs = reader.getBitmapBuffer('S');
        this.Ss = reader.getIDBuffer('S');
        this.Bp = reader.getBitmapBuffer('P');
        this.Sp = reader.getIDBuffer('P');
        this.Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');
        HDTBitmapDirectory dirS = reader.getDirectory('S');
        this.dirP = reader.getDirectory('P');
        this.dirO = reader.getDirectory('O');
        this.boBit = Bo.bitReader();
        this.bpBit = Bp.bitReader();
        this.spNum = Sp.getNumEntries();
        this.soNum = So.getNumEntries();

        RangeBounds bounds = RangeBounds.of(filter, quad, dict);
        minSubId = bounds.minS;
        maxSubId = bounds.maxS;
        minPid = bounds.minP;
        maxPid = bounds.maxP;
        minObjId = bounds.minO;
        maxObjId = bounds.maxO;

        // Resolve the graph the same way the other three iterators behind
        // BGIteratorMaster do: the dispatcher also routes here when the graph
        // VARIABLE is pre-bound in the BindingNodeId, and locate() on the raw
        // variable node returns -1 - silently yielding nothing for a graph
        // that exists.
        if (quad.getGraph().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getGraph())) : NodeId.NONE;
            gi = (bound != NodeId.NONE) ? NodeId.id(bound) : -1;
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        if (gi < 1) return;

        // Honour a subject / object fixed by the pattern - a concrete term named
        // in the triple, or a VARIABLE the parent binding already holds (a join's
        // second pattern, VALUES, a DESCRIBE star). HDF5Reader.read leaves a
        // same-space bound variable as a Var and expects the iterator to consult
        // the binding, as the SO/OS/POS iterators do; this one only looked at
        // isConcrete(), so "?y ?p ?o" after "?x :knows ?y" walked the graph's
        // ENTIRE subject range per input row and rejected every non-matching
        // row in computeNext. A bound id is in the position's own id-space
        // (cross-space bindings were materialized upstream), so it clamps the
        // range exactly like a located concrete term; the per-row putCompatible
        // check stays as the final word.
        long concreteSubId = -1;
        Node sNode = quad.getSubject();
        if (sNode.isConcrete()) {
            concreteSubId = dict.getSubjects().locate(sNode);
            if (concreteSubId < 1) return;
        } else if (sNode.isVariable() && bnid != null) {
            long bound = bnid.get(Var.alloc(sNode));
            if (bound != NodeId.NONE) {
                concreteSubId = NodeId.id(bound);
                if (concreteSubId < 1) return; // DOES_NOT_EXIST: nothing can match
            }
        }
        if (concreteSubId > 0) {
            minSubId = Math.max(minSubId, concreteSubId);
            maxSubId = Math.min(maxSubId, concreteSubId);
        }
        Node oNode = quad.getObject();
        if (oNode.isConcrete()) {
            long oid = dict.getObjects().locate(oNode);
            if (oid < 1) return;
            minObjId = Math.max(minObjId, oid);
            maxObjId = Math.min(maxObjId, oid);
        } else if (oNode.isVariable() && bnid != null && bnid.get(Var.alloc(oNode)) != NodeId.NONE) {
            long oid = NodeId.id(bnid.get(Var.alloc(oNode)));
            if (oid < 1) return; // DOES_NOT_EXIST: nothing can match
            minObjId = Math.max(minObjId, oid);
            maxObjId = Math.min(maxObjId, oid);
        } else if (TripleTermMatcher.isPattern(quad.getObject())) {
            // Var-containing triple term: not concrete, but very much a
            // constraint. Clamp to the triple-term suffix of the object space
            // and unify each surviving row in computeNext.
            ttMatcher = TripleTermMatcher.compile(quad.getObject(), dict);
            if (ttMatcher == null) {
                return; // pattern cannot match anything in this store
            }
            minObjId = Math.max(minObjId, dict.firstTripleTermObjectId());
        }

        // -----------------------------------------------------------------
        // LEVEL 1: Subject Range (Graph Scope)
        // -----------------------------------------------------------------
        // select1 is 1-based. Start of Graph gi is the gi-th '1' in Bs; the end is
        // one before the next block's start.
        long sStart = RangeSelect.blockStart(dirS, Bs, gi);
        if (sStart == -1) return;
        long sEnd = RangeSelect.blockEnd(dirS, Bs, gi, sStart);
        if (sStart > sEnd) return;

        // Parallel-chunk clamp: restrict to this chunk's slice of the graph's range.
        if (sPosLo > sStart) sStart = sPosLo;
        if (sPosHi < sEnd) sEnd = sPosHi;
        if (sStart > sEnd) return;

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
        this.idxP = RangeSelect.blockStart(dirP, Bp, idxS + 1);

        // -----------------------------------------------------------------
        // LEVEL 3: Object Cursor
        // -----------------------------------------------------------------
        // Start of Objects for Predicate `idxP` is the (idxP + 1)-th '1' in Bo.
        this.idxO = RangeSelect.blockStart(dirO, Bo, idxP + 1);

        // --- Safety Checks ---
        // If idxP or idxO are -1 (not found), it means the lists are empty or we overshot.
        // However, select1(1) should always return 0 for non-empty.
        if (idxP == -1 || idxO == -1) return;

        // Ensure we are within bounds
        if (idxP >= spNum || idxO >= soNum) return;

        // Load Initial Values
        this.curSID = Ss.get(idxS);
        this.curPID = Sp.get(idxP);

        if (quad.getGraph().isVariable()) {
            gVar = Var.alloc(quad.getGraph());
            gId = NodeId.pack(NodeType.GRAPH, gi);
        }
        if (quad.getSubject().isVariable()) sVar = Var.alloc(quad.getSubject());
        if (quad.getPredicate().isVariable()) pVar = Var.alloc(quad.getPredicate());
        if (quad.getObject().isVariable()) oVar = Var.alloc(quad.getObject());

        advance();
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

    /** Candidate rows examined so far; lets tests prove an index path was taken. */
    private long visited;

    long rowsVisited() {
        return visited;
    }

    private void advance() {
        hasNext = false;
        while (idxS <= endS) {
            visited++;
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
            if (idxO >= soNum) {
                skipPredicateBlock();
                continue;
            }
            this.curOID = So.get(idxO);
            // Objects ascend within an (S,P) block, so an object range is
            // navigated, not filtered row by row: below the range, seek to the
            // first candidate; above it, nothing later in the block can match.
            if (curOID < minObjId) {
                long nextBlock = RangeSelect.blockStart(dirO, Bo, idxP + 2);
                long blockEnd = (nextBlock == -1 ? soNum : nextBlock) - 1;
                long pos = So.lowerBound(idxO, blockEnd, minObjId);
                if (pos == -1) {
                    skipPredicateBlock();
                    continue;
                }
                idxO = pos;
                this.curOID = So.get(idxO);
            }
            if (curOID > maxObjId) {
                skipPredicateBlock();
                continue;
            }
            if (isMatch) {
                this.resS = curSID;
                this.resP = curPID;
                this.resO = curOID;
                hasNext = true;
            }
            idxO++;
            boolean endOfObjectList = (idxO >= soNum) || boBit.bit(idxO);
            if (endOfObjectList) {
                idxP++;
                boolean endOfPredicateList = (idxP >= spNum) || bpBit.bit(idxP);
                if (endOfPredicateList) {
                    idxS++;
                    if (idxS <= endS) {
                        curSID = Ss.get(idxS);
                    }
                }
                if (idxP < spNum) {
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
        long nextPStart = RangeSelect.blockStart(dirO, Bo, idxP + 2);
        idxO = (nextPStart == -1) ? soNum : nextPStart;

        idxP++;

        boolean endOfPredicateList = (idxP >= spNum) || bpBit.bit(idxP);
        if (endOfPredicateList) {
            idxS++;
            if (idxS <= endS) curSID = Ss.get(idxS);
        }

        if (idxP < spNum) curPID = Sp.get(idxP);
    }

    private void skipSubjectBlock() {
        // Find start of NEXT Subject block
        // Current Subject idxS. Start was select1(idxS+1).
        // Next Subject idxS+1. Start is select1(idxS+2).
        long nextSStartP = RangeSelect.blockStart(dirP, Bp, idxS + 2);

        if (nextSStartP == -1) {
            idxP = spNum;
            idxO = soNum;
        } else {
            idxP = nextSStartP;
            // Now align Object cursor to the new Predicate
            long nextSStartO = RangeSelect.blockStart(dirO, Bo, idxP + 1);
            idxO = (nextSStartO == -1) ? soNum : nextSStartO;
        }

        idxS++;

        if (idxS <= endS) curSID = Ss.get(idxS);
        if (idxP < spNum) curPID = Sp.get(idxP);
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
            if (gVar != null) {
                ok = result.putCompatible(gVar, gId, nodeTable);
            }
            if (ok && sVar != null) {
                ok = result.putCompatible(sVar, NodeId.pack(NodeType.SUBJECT, resS), nodeTable);
            }
            if (ok && pVar != null) {
                ok = result.putCompatible(pVar, NodeId.pack(NodeType.PREDICATE, resP), nodeTable);
            }
            if (ok && oVar != null) {
                ok = result.putCompatible(oVar, NodeId.pack(NodeType.OBJECT, resO), nodeTable);
            }
            if (ok && ttMatcher != null) {
                BindingNodeId unified = ttMatcher.matchAndBind(resO, result, nodeTable);
                if (unified == null) {
                    ok = false;
                } else {
                    result = unified;
                }
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
