package com.ebremer.beakgraph.hdf5.jena;

import org.apache.jena.sparql.core.Var;

/**
 * What an iterator's rows have to ask about a variable before binding it,
 * answered once for the iterator instead of once a row.
 *
 * <p>A row is a new {@link BindingNodeId} layer over the iterator's parent
 * binding, and a layer may bind only a variable that is bound nowhere in its
 * chain. {@code put} and {@code putCompatible} find that out by walking the
 * chain, for every variable of every row; but the pattern and the parent are
 * fixed for the iterator's lifetime, so whether a variable is new is the same
 * for every row it emits. A variable that is - held by no layer of the
 * parent, and not the variable of an earlier position of the same row - is
 * bound with {@link BindingNodeId#bind}. One that is not keeps
 * {@code putCompatible}: that is the check that makes {@code ?x <p> ?x} match
 * only the rows whose two positions hold one term.
 *
 * <p>This relies on what the bindings already rely on: a layer handed to an
 * iterator as its parent is not bound into afterwards.
 */
final class RowVars {

    private RowVars() {}

    /**
     * True when {@code v} is new to every row over {@code parent}: a variable,
     * bound in no layer of the parent, and none of the variables the row binds
     * before it ({@code earlier}; nulls are positions that bind nothing).
     */
    static boolean fresh(BindingNodeId parent, Var v, Var... earlier) {
        if (v == null) {
            return false;
        }
        for (Var e : earlier) {
            if (v.equals(e)) {
                return false;
            }
        }
        return parent == null || !parent.containsKey(v);
    }

    /** True when the variables given (nulls are positions that bind nothing) are pairwise distinct. */
    static boolean distinct(Var... vars) {
        for (int i = 0; i < vars.length; i++) {
            for (int k = i + 1; k < vars.length; k++) {
                if (vars[i] != null && vars[i].equals(vars[k])) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * True when the parent already binds the graph's variable {@code g} to the
     * graph {@code gi} itself - a walk over the named graphs binds it before
     * the pattern is read - so that a row has nothing to add and nothing to
     * check for it. Graph ids are entity ids: a binding in the predicates' own
     * id space says nothing about the graph, and is left to the row's check.
     */
    static boolean holds(BindingNodeId parent, Var g, long gi) {
        if (g == null || parent == null) {
            return false;
        }
        long bound = parent.get(g);
        return bound != NodeId.NONE && !NodeId.isSpecial(bound) && !NodeId.isPredicateSpace(bound) && NodeId.id(bound) == gi;
    }
}
