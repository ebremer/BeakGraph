package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.Dictionary;
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
 * Iterator for GSPO index where G, S, and P are bound (or fixed), finding O.
 * Optimized with Object-level range filtering and binary search.
 */
public class BGIteratorSO implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final BitPackedUnSignedLongBuffer So;

    private long i;  // current object index
    private long j;  // end object index (inclusive)
    private long gi, si, pi;
    private boolean hasNext = false;

    private long minObjId = 0;
    private long maxObjId = Long.MAX_VALUE;

    // Row-emission plan, computed once: which variables each row binds, with the
    // constant G/S/P NodeIds pre-built (only the object id varies per row).
    private Var gVar, sVar, pVar, oVar;
    private NodeId gId, sId, pId;

    public BGIteratorSO(PositionalDictionaryReader dict, IndexReader reader, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;

        // GSPO Structure mapping
        BitPackedUnSignedLongBuffer Bs = reader.getBitmapBuffer('S');
        BitPackedUnSignedLongBuffer Ss = reader.getIDBuffer('S');
        BitPackedUnSignedLongBuffer Bp = reader.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer Sp = reader.getIDBuffer('P');
        BitPackedUnSignedLongBuffer Bo = reader.getBitmapBuffer('O');
        this.So = reader.getIDBuffer('O');

        HDTBitmapDirectory dirS = reader.getDirectory('S');
        HDTBitmapDirectory dirP = reader.getDirectory('P');
        HDTBitmapDirectory dirO = reader.getDirectory('O');

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
        long sStart = RangeSelect.blockStart(dirS, Bs, gi);
        if (sStart == -1) return;
        long sEnd = RangeSelect.blockEnd(dirS, Bs, gi, sStart);
        if (sStart > sEnd) return;
        long sIndex = Ss.binarySearch(sStart, sEnd, si);
        if (sIndex < 0) return;

        // B. Find Predicate Index under Subject
        long pStart = RangeSelect.blockStart(dirP, Bp, sIndex + 1);
        if (pStart == -1) return;
        long pEnd = RangeSelect.blockEnd(dirP, Bp, sIndex + 1, pStart);
        if (pStart > pEnd) return;
        long pIndex = Sp.binarySearch(pStart, pEnd, pi);
        if (pIndex < 0) return;

        // C. Find Object Range for Predicate
        long rawOStart = RangeSelect.blockStart(dirO, Bo, pIndex + 1);
        if (rawOStart == -1) return;
        long rawOEnd = RangeSelect.blockEnd(dirO, Bo, pIndex + 1, rawOStart);
        if (rawOStart > rawOEnd) return;

        // D. Apply Specific Object Bound or Range Filters
        // Distinguish "object is an unbound variable" from "object is a concrete
        // term (or bound variable)": resolveNode returns -1 for both an unbound
        // variable and a term missing from the dictionary, and a missing term must
        // yield no match - not a full range scan (which fabricated phantom rows,
        // e.g. ASK with a non-existent object answered true).
        Node oNode = quad.getObject();
        boolean oUnbound = oNode.isVariable() && (bnid == null || !bnid.containsKey(Var.alloc(oNode)));

        if (oUnbound) {
            // Case: Object is a variable, apply min/max ID range filters
            this.i = (minObjId <= 0) ? rawOStart : So.lowerBound(rawOStart, rawOEnd, minObjId);
            this.j = (maxObjId == Long.MAX_VALUE) ? rawOEnd : So.upperBound(rawOStart, rawOEnd, maxObjId);

            if (this.i != -1 && this.i <= this.j) {
                this.hasNext = true;
            }
        } else {
            // Case: Object is bound (e.g., G, S, P, O are all known, just checking existence)
            long specificObjId = resolveNode(oNode, dict.getObjects(), bnid);
            if (specificObjId < 1) return; // concrete term absent from this store -> no match
            long foundIdx = So.binarySearch(rawOStart, rawOEnd, specificObjId);
            if (foundIdx >= 0) {
                this.i = foundIdx;
                this.j = foundIdx;
                this.hasNext = true;
            }
        }

        if (hasNext) {
            planRowEmission(quad);
        }
    }

    /**
     * Precomputes which variables each row binds - the pattern and the parent
     * binding are fixed for this iterator's lifetime, so the per-row work
     * reduces to appends of pre-built NodeIds (only the object id varies).
     */
    private void planRowEmission(Quad quad) {
        if (quad.getGraph().isVariable()) {
            Var v = Var.alloc(quad.getGraph());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                gVar = v;
                gId = new NodeId(gi, NodeType.GRAPH);
            }
        }
        if (quad.getSubject().isVariable()) {
            Var v = Var.alloc(quad.getSubject());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                sVar = v;
                sId = new NodeId(si, NodeType.SUBJECT);
            }
        }
        if (quad.getPredicate().isVariable()) {
            Var v = Var.alloc(quad.getPredicate());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                pVar = v;
                pId = new NodeId(pi, NodeType.PREDICATE);
            }
        }
        if (quad.getObject().isVariable()) {
            Var v = Var.alloc(quad.getObject());
            if (parentBinding == null || !parentBinding.containsKey(v)) {
                oVar = v;
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
        // Snap the bound to the edges of the whole value-equal cluster: value-equal
        // but term-distinct literals ("5"^^xsd:int vs "5"^^xsd:integer) occupy
        // adjacent distinct ids, and the raw exact-term insertion point can land
        // inside that cluster, silently dropping qualifying boundary rows.
        long[] c = ValueCluster.of(dict.getObjects(), value);
        switch (op) {
            case ">" -> {
                long target = c[1] + 1;
                if (Long.compareUnsigned(target, minObjId) > 0) minObjId = target;
            }
            case ">=" -> {
                if (Long.compareUnsigned(c[0], minObjId) > 0) minObjId = c[0];
            }
            case "<" -> {
                long target = c[0] - 1;
                if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
            }
            case "<=" -> {
                long target = c[1];
                if (Long.compareUnsigned(target, maxObjId) < 0) maxObjId = target;
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
        if (gVar != null) result.put(gVar, gId);
        if (sVar != null) result.put(sVar, sId);
        if (pVar != null) result.put(pVar, pId);
        if (oVar != null) result.put(oVar, new NodeId(So.get(i), NodeType.OBJECT));
        i++;
        hasNext = (i <= j);
        return result;
    }
}
