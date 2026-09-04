package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.lib.CdtTerms;
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
 * <p>
 * A hint is only sound when the dictionary's order of the constant's value
 * space is ARQ's comparison order, so that the rows the comparison accepts
 * form one contiguous id run around the constant's {@link ValueCluster}.
 * Constants from the two spaces {@code NodeComparator} orders on its own
 * terms are therefore not hints at all ({@link #orderAgreesWithArq}).
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
                Node value = right.getConstant().asNode();
                if (orderAgreesWithArq(value)) {
                    sink.bound(left.asVar(), op, value);
                }
            } else if (left.isConstant() && right.isVariable()) {
                Node value = left.getConstant().asNode();
                if (orderAgreesWithArq(value)) {
                    sink.bound(right.asVar(), flip(op), value);
                }
            }
        }
    }

    /**
     * Whether ids around {@code value} are ordered the way ARQ compares against
     * it, which is what lets a comparison narrow the id range. False for:
     * <ul>
     *   <li><b>composite literals</b> (cdt:List / cdt:Map): the dictionary orders
     *       them by exact lexical form, ARQ by value, and value-equal forms
     *       ("[1, 2]" and "[1,2]") are not even adjacent - the snapped bound
     *       lands inside the value-equal set and drops boundary rows;</li>
     *   <li><b>language-tagged literals</b>: the dictionary blocks them by exact
     *       tag, then lexical form, then base direction, while ARQ matches tags
     *       case-insensitively and treats base directions on its own terms.
     *       Jena 6 canonicalises tag case at node creation, so "m"@EN and
     *       "m"@en are one term today; the gate still covers rdf:dirLangString
     *       variants and stores written by a non-canonicalising Jena, and a
     *       range comparison against a language-tagged constant is rare.</li>
     * </ul>
     * Every other space is ordered by value with value-equal terms adjacent
     * (see {@code NodeComparator}), so the hint is over-inclusive, never lossy.
     * The enclosing OpFilter evaluates the comparison either way; skipping the
     * hint only costs the narrowing.
     */
    static boolean orderAgreesWithArq(Node value) {
        if (!value.isLiteral()) {
            return true;
        }
        if (CdtTerms.isComposite(value)) {
            return false;
        }
        String lang = value.getLiteralLanguage();
        return lang == null || lang.isEmpty();
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
