package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;

/**
 * Id-level unification of a variable-containing triple-term PATTERN
 * ({@code <<( ?a :b ?c )>>} in a pattern's object position) against stored
 * triple terms, straight off the {@code tripleTerms} component store - no Node
 * materialization for non-matching candidates (PLAN Part IV §IV.5: the
 * TDB2-SolverRX idea, executed below the Node layer).
 *
 * <p>Compiled once per iterator. Concrete components locate ONCE - a missing
 * one means the pattern can match nothing in this store, so {@link #compile}
 * answers null and the iterator answers empty. Variable components bind packed
 * ids in their position's id-space (SUBJECT / PREDICATE / OBJECT). Embedded
 * bindings land in CHAINED {@link BindingNodeId} layers, so the 4-slot layer
 * struct is never widened (PLAN §4.1). Repeated variables - within the
 * pattern, or against the surrounding row and its parent chain - are enforced
 * through {@code putCompatible}, including the cross-space predicate/entity
 * case.
 */
final class TripleTermMatcher {

    private final PositionalDictionaryReader dict;
    // Exactly one of {var, nested matcher, concrete id} describes each
    // component; concrete ids are >= 1 (compile() refuses unresolvables).
    private final Var sVar;
    private final long sId;
    private final Var pVar;
    private final long pId;
    private final Var oVar;
    private final TripleTermMatcher oNested;
    private final long oId;

    private TripleTermMatcher(PositionalDictionaryReader dict,
                              Var sVar, long sId,
                              Var pVar, long pId,
                              Var oVar, TripleTermMatcher oNested, long oId) {
        this.dict = dict;
        this.sVar = sVar;
        this.sId = sId;
        this.pVar = pVar;
        this.pId = pId;
        this.oVar = oVar;
        this.oNested = oNested;
        this.oId = oId;
    }

    /**
     * A triple-term PATTERN: a triple term containing at least one variable at
     * any depth. ({@code isConcrete()} on a Node_Triple delegates to its
     * triple, so this is exactly "has embedded variables".) The single
     * classification predicate every routing site uses - PLAN §4.0 Trap 2's
     * fix is that no iterator answers this question differently.
     */
    static boolean isPattern(Node n) {
        return n.isTripleTerm() && !n.isConcrete();
    }

    /**
     * Compiles {@code ttPattern} against the store, or null when a match is
     * impossible: the store holds no triple terms at all, or a concrete
     * component is absent from its dictionary. Callers answer null with zero
     * rows - never by scanning.
     */
    static TripleTermMatcher compile(Node ttPattern, PositionalDictionaryReader dict) {
        if (dict.firstTripleTermObjectId() == Long.MAX_VALUE) {
            return null; // triple-term-free store: nothing can match
        }
        Triple t = ttPattern.getTriple();
        Node s = t.getSubject();
        Node p = t.getPredicate();
        Node o = t.getObject();

        Var sVar = null;
        long sId = -1;
        if (s.isVariable()) {
            sVar = Var.alloc(s);
        } else {
            sId = dict.getSubjects().locate(s);
            if (sId < 1) return null;
        }

        Var pVar = null;
        long pId = -1;
        if (p.isVariable()) {
            pVar = Var.alloc(p);
        } else {
            pId = dict.getPredicates().locate(p);
            if (pId < 1) return null;
        }

        Var oVar = null;
        TripleTermMatcher oNested = null;
        long oId = -1;
        if (o.isVariable()) {
            oVar = Var.alloc(o);
        } else if (isPattern(o)) {
            oNested = compile(o, dict);
            if (oNested == null) return null;
        } else {
            // Concrete object - including a fully concrete NESTED triple term,
            // which the object dictionary locates as a single id.
            oId = dict.getObjects().locate(o);
            if (oId < 1) return null;
        }
        return new TripleTermMatcher(dict, sVar, sId, pVar, pId, oVar, oNested, oId);
    }

    /**
     * Tests stored object id {@code objectId} against the pattern and, on
     * match, binds the embedded variables into {@code row} - chaining a fresh
     * layer whenever the current one's four slots fill. Returns the layer
     * holding the completed row, or null on mismatch. On null the caller must
     * DISCARD {@code row}: partial embedded bindings may already have landed
     * (every caller builds one row object per candidate, so this costs
     * nothing).
     */
    BindingNodeId matchAndBind(long objectId, BindingNodeId row, NodeTable nodeTable) {
        if (!dict.isTripleTermObjectId(objectId)) {
            return null; // not a triple term (entity or literal id)
        }
        long[] c = dict.tripleTermComponents(objectId);

        if (sVar == null) {
            if (c[0] != sId) return null;
        } else {
            if (row.isFull()) row = new BindingNodeId(row);
            if (!row.putCompatible(sVar, NodeId.pack(NodeType.SUBJECT, c[0]), nodeTable)) return null;
        }

        if (pVar == null) {
            if (c[1] != pId) return null;
        } else {
            if (row.isFull()) row = new BindingNodeId(row);
            if (!row.putCompatible(pVar, NodeId.pack(NodeType.PREDICATE, c[1]), nodeTable)) return null;
        }

        if (oNested != null) {
            return oNested.matchAndBind(c[2], row, nodeTable);
        }
        if (oVar != null) {
            if (row.isFull()) row = new BindingNodeId(row);
            if (!row.putCompatible(oVar, NodeId.pack(NodeType.OBJECT, c[2]), nodeTable)) return null;
            return row;
        }
        return (c[2] == oId) ? row : null;
    }
}
