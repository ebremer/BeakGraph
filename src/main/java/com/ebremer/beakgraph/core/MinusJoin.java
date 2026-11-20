package com.ebremer.beakgraph.core;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.OpAssign;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpMinus;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpReduced;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpTopN;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.JoinClassifier;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprVisitorBase;
import org.apache.jena.sparql.algebra.walker.Walker;

/**
 * The patterns after a {@code MINUS} joined to the rows before it, rather than evaluated on their own.
 *
 * <p>Jena compiles {@code { A MINUS { B } C }} to {@code (join (minus A B) C)}, and when it cannot run {@code C} with
 * each row's values substituted, it evaluates {@code C} by itself and joins the two. OKN's
 * spoke-genelab/spaceflight-vs-ground-control-60day-deg then read every differential-expression statement of every
 * assay, and sawgraph/samples_longchain_biota a cross product of three unconnected patterns; neither finished in
 * 120 s. This rewrites the join as {@code (minus (join A C) B)}, and a filter Jena put over the {@code MINUS} onto
 * {@code A}, so that {@code C} runs once per row of {@code A}. In-process on OKN's store: samples_longchain_biota in
 * 5.3 s (100 rows), 60day in 9.6 s (10 rows; 108 s the first time in a fresh process).
 *
 * <p>The answers are the same when every variable that {@code B} and {@code C} share, and every variable the filter
 * mentions that {@code C} can bind, is bound in every row of {@code A} ({@link #boundVars}): a row of {@code A}
 * extended by {@code C} is then compatible with a row of {@code B}, and shares a variable with it, exactly when the
 * row of {@code A} alone is and does; and the filter sees the same values. A filter with {@code EXISTS} is left
 * where it is. {@code beakgraph.minus.join=false} turns the rewrite off.
 */
public final class MinusJoin {

    /** Joins rewritten: tests see which way a query went. */
    public static final AtomicLong HITS = new AtomicLong();

    private MinusJoin() {}

    /** {@code op} with each {@code (join (minus A B) C)} that qualifies rewritten. */
    public static Op transform(Op op) {
        if (!Boolean.parseBoolean(System.getProperty("beakgraph.minus.join", "true"))) return op;
        return Transformer.transform(new TransformCopy() {
            @Override
            public Op transform(OpJoin opJoin, Op left, Op right) {
                Op rewritten = rewrite(left, right);
                return (rewritten != null) ? rewritten : super.transform(opJoin, left, right);
            }
        }, op);
    }

    /** {@code (minus (join (filter F A) C) B)} for {@code left} = {@code (filter F (minus A B))}, or null. */
    static Op rewrite(Op left, Op right) {
        ExprList filters = new ExprList();
        Op l = left;
        while (l instanceof OpFilter f) {
            filters.addAll(f.getExprs());
            l = f.getSubOp();
        }
        if (!(l instanceof OpMinus minus) || hasExists(filters)) return null;
        Op a = minus.getLeft(), b = minus.getRight(), c = right;
        Set<Var> bound = boundVars(a);
        Set<Var> inC = OpVars.visibleVars(c);
        for (Var v : OpVars.visibleVars(b)) {
            if (inC.contains(v) && !bound.contains(v)) return null;
        }
        for (Var v : filters.getVarsMentioned()) {
            if (inC.contains(v) && !bound.contains(v)) return null;
        }
        Op fa = filters.isEmpty() ? a : OpFilter.filterBy(filters, a);
        Op joined = JoinClassifier.isLinear(fa, c) ? OpSequence.create(fa, c) : OpJoin.create(fa, c);
        HITS.incrementAndGet();
        return OpMinus.create(joined, b);
    }

    private static boolean hasExists(ExprList exprs) {
        boolean[] found = {false};
        ExprVisitorBase v = new ExprVisitorBase() {
            @Override public void visit(ExprFunctionOp f) { found[0] = true; }
        };
        for (Expr e : exprs) Walker.walk(e, v);
        return found[0];
    }

    /**
     * Variables bound in every row of {@code op}, as far as can be told without running it; never one that some row
     * may leave unbound. Jena's {@code OpVars.fixedVars} counts a {@code BIND} variable, which an error leaves
     * unbound, and a {@code VALUES} variable with {@code UNDEF} rows: neither is counted here. Operators not listed
     * bind nothing for certain.
     */
    public static Set<Var> boundVars(Op op) {
        Set<Var> out = new HashSet<>();
        if (op instanceof OpBGP bgp) {
            for (Triple t : bgp.getPattern()) addVars(out, t.getSubject(), t.getPredicate(), t.getObject());
        } else if (op instanceof OpTriple t) {
            addVars(out, t.getTriple().getSubject(), t.getTriple().getPredicate(), t.getTriple().getObject());
        } else if (op instanceof OpQuadPattern qp) {
            addVars(out, qp.getGraphNode());
            for (Quad q : qp.getPattern()) addVars(out, q.getSubject(), q.getPredicate(), q.getObject());
        } else if (op instanceof OpPath p) {
            addVars(out, p.getTriplePath().getSubject(), p.getTriplePath().getObject());
        } else if (op instanceof OpJoin j) {
            out.addAll(boundVars(j.getLeft()));
            out.addAll(boundVars(j.getRight()));
        } else if (op instanceof OpSequence s) {
            for (Op e : s.getElements()) out.addAll(boundVars(e));
        } else if (op instanceof OpLeftJoin lj) {
            out.addAll(boundVars(lj.getLeft()));
        } else if (op instanceof OpConditional cd) {
            out.addAll(boundVars(cd.getLeft()));
        } else if (op instanceof OpMinus m) {
            out.addAll(boundVars(m.getLeft()));
        } else if (op instanceof OpUnion u) {
            out.addAll(boundVars(u.getLeft()));
            out.retainAll(boundVars(u.getRight()));
        } else if (op instanceof OpGraph g) {
            out.addAll(boundVars(g.getSubOp()));
            addVars(out, g.getNode());
        } else if (op instanceof OpProject p) {
            out.addAll(boundVars(p.getSubOp()));
            out.retainAll(p.getVars());
        } else if (op instanceof OpGroup g) {
            Set<Var> inner = boundVars(g.getSubOp());
            for (Var v : g.getGroupVars().getVars()) {
                if (!g.getGroupVars().hasExpr(v) && inner.contains(v)) out.add(v);
            }
        } else if (op instanceof OpTable t) {
            Set<Var> every = null;
            for (var it = t.getTable().rows(); it.hasNext(); ) {
                Binding row = it.next();
                Set<Var> here = new HashSet<>();
                row.vars().forEachRemaining(here::add);
                if (every == null) every = here; else every.retainAll(here);
                if (every.isEmpty()) break;
            }
            if (every != null) out.addAll(every);
        } else if (op instanceof OpFilter || op instanceof OpExtend || op instanceof OpAssign || op instanceof OpDistinct
                || op instanceof OpReduced || op instanceof OpSlice || op instanceof OpOrder || op instanceof OpTopN) {
            // Rows removed, reordered or given more variables: the ones bound below still are.
            out.addAll(boundVars(((Op1) op).getSubOp()));
        }
        return out;
    }

    private static void addVars(Set<Var> out, Node... nodes) {
        for (Node n : nodes) {
            if (n == null) continue;
            if (n.isVariable()) {
                out.add(Var.alloc(n));
            } else if (n.isTripleTerm()) {
                Triple t = n.getTriple();
                addVars(out, t.getSubject(), t.getPredicate(), t.getObject());
            }
        }
    }
}
