package com.ebremer.beakgraph.hdf5.jena;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprList;

/**
 * Extracts range-pushdown hints ({@code ?var OP constant}) from a FILTER
 * expression list for the index iterators.
 * <p>
 * Only the four ordering comparisons ({@code > >= < <=}) are hints: they narrow
 * the id range an iterator walks. Every other two-argument function - CONTAINS,
 * STRSTARTS, STRENDS, sameTerm, langMatches, {@code =}, {@code !=}, {@code &&} ...
 * - is left entirely to the enclosing OpFilter. Dispatching on the concrete
 * expression class (not on {@code getOpName()}, which is {@code null} for every
 * non-operator function) is what makes that safe. One shared scan keeps the
 * rule identical across the GSPO/GPOS iterators.
 */
final class FilterBounds {

    private FilterBounds() {}

    @FunctionalInterface
    interface Sink {
        /** {@code var OP value}, with OP one of {@code > >= < <=} (already flipped for constant-left forms). */
        void bound(Var var, String op, Node value);
    }

    static void scan(ExprList filter, Sink sink) {
        if (filter == null || filter.isEmpty()) {
            return;
        }
        for (Expr expr : filter.getList()) {
            if (!(expr instanceof ExprFunction2 func)) {
                continue;
            }
            String op = orderingOp(func);
            if (op == null) {
                continue; // not a range comparison: nothing to push down
            }
            Expr left = func.getArg1();
            Expr right = func.getArg2();
            if (left.isVariable() && right.isConstant()) {
                sink.bound(left.asVar(), op, right.getConstant().asNode());
            } else if (left.isConstant() && right.isVariable()) {
                sink.bound(right.asVar(), flip(op), left.getConstant().asNode());
            }
        }
    }

    /** The comparison symbol for an ordering comparison, or {@code null} for any other function. */
    static String orderingOp(ExprFunction2 f) {
        if (f instanceof E_GreaterThan) return ">";
        if (f instanceof E_GreaterThanOrEqual) return ">=";
        if (f instanceof E_LessThan) return "<";
        if (f instanceof E_LessThanOrEqual) return "<=";
        return null;
    }

    /** {@code c OP ?v} rewritten as {@code ?v OP' c}. */
    static String flip(String op) {
        return switch (op) {
            case ">" -> "<";
            case "<" -> ">";
            case ">=" -> "<=";
            case "<=" -> ">=";
            default -> op;
        };
    }
}
