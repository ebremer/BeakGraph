package com.ebremer.beakgraph.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;

/**
 * The triple patterns on both sides of a property path joined into one BGP, when the path cuts a join between them;
 * the path runs after them, and the patterns reached only through the path's far end after it.
 *
 * <p>Jena splits {@code { A . ?x <p>+ ?y . B }} into {@code (sequence (bgp A) (path ...) (bgp B))} and runs the
 * blocks in that order, B once per row of A and the path. OKN's federation/sawgraph-samples-near-PFAS-facilities
 * then took every observation with its result and unit first (A), and checked the PFAS facilities in each
 * observation's cell afterwards (B, joined to A by the cell, not by the path): the patterns that narrow the search
 * ran last, once per observation, and the query did not finish in 300 s.
 *
 * <p>A chain of BGPs and paths ({@code sequence} or {@code join}, nested), with BINDs over its parts, is rewritten
 * when a triple after a path shares a variable other than a path's ends with a triple before the first path. It
 * becomes {@code (extend* (sequence (bgp core) path... (bgp rest)))}: {@code rest} holds the groups of triples
 * (triples sharing variables) that all came after the first path and share a variable with a path's end - the
 * region a {@code +} path climbs to - which stay after the paths, as before; {@code core} everything else. A path
 * whose only link to the triples after it is its own end - a filter like {@code ?x <p>+ <region>} - leaves the chain
 * as it is: it narrows what runs after it. The answers are the same either way: every part is an inner join. A BIND
 * is lifted to the top when its variable is used nowhere else in the chain and the variables of its expression are
 * bound in every row of what it extended ({@link MinusJoin#boundVars}). Only whole chains are rewritten - a part of
 * one, rewritten alone, cannot see the variables the rest binds - and anything else in a chain (a FILTER,
 * OPTIONAL, UNION, a subquery) leaves it as it is. {@code beakgraph.path.merge=false} turns the rewrite off.
 */
public final class PathMerge {

    /** Chains rewritten: tests see which way a query went. */
    public static final AtomicLong HITS = new AtomicLong();

    private PathMerge() {}

    /** {@code op} with each chain that qualifies rewritten. */
    public static Op transform(Op op) {
        if (!Boolean.parseBoolean(System.getProperty("beakgraph.path.merge", "true"))) return op;
        // The chain nodes inside another chain node: rewritten only as part of the whole.
        Set<Op> inner = Collections.newSetFromMap(new IdentityHashMap<>());
        OpWalker.walk(op, new OpVisitorBase() {
            @Override public void visit(OpSequence s) { for (Op e : s.getElements()) if (isChainNode(e)) inner.add(e); }
            @Override public void visit(OpJoin j) { if (isChainNode(j.getLeft())) inner.add(j.getLeft()); if (isChainNode(j.getRight())) inner.add(j.getRight()); }
            @Override public void visit(OpExtend e) { if (isChainNode(e.getSubOp())) inner.add(e.getSubOp()); }
        });
        return Transformer.transform(new TransformCopy() {
            @Override
            public Op transform(OpSequence opSequence, List<Op> elts) {
                Op out = super.transform(opSequence, elts);
                return inner.contains(opSequence) ? out : orSelf(rewrite(out), out);
            }

            @Override
            public Op transform(OpJoin opJoin, Op left, Op right) {
                Op out = super.transform(opJoin, left, right);
                return inner.contains(opJoin) ? out : orSelf(rewrite(out), out);
            }

            @Override
            public Op transform(OpExtend opExtend, Op sub) {
                Op out = super.transform(opExtend, sub);
                return inner.contains(opExtend) ? out : orSelf(rewrite(out), out);
            }
        }, op);
    }

    private static boolean isChainNode(Op op) {
        return op instanceof OpSequence || op instanceof OpJoin || op instanceof OpExtend;
    }

    private static Op orSelf(Op rewritten, Op op) {
        return (rewritten != null) ? rewritten : op;
    }

    /** A chain's parts, in their order. */
    private static final class Chain {
        final List<Triple> triples = new ArrayList<>();
        final List<Boolean> afterPath = new ArrayList<>();      // per triple: does it come after the first path
        final List<OpPath> paths = new ArrayList<>();
        final List<VarExprList> binds = new ArrayList<>();
        final List<Set<Var>> bindInputs = new ArrayList<>();   // what each BIND's subtree binds in every row
    }

    /** {@code op} rewritten, or null when it is not a chain that qualifies. */
    static Op rewrite(Op op) {
        Chain chain = new Chain();
        if (!collect(op, chain) || chain.paths.isEmpty() || !pathCutsAJoin(chain) || !bindsCanRise(chain)) return null;
        List<Triple> core = new ArrayList<>(), rest = new ArrayList<>();
        split(chain, core, rest);
        List<Op> parts = new ArrayList<>();
        if (!core.isEmpty()) parts.add(new OpBGP(BasicPattern.wrap(core)));
        parts.addAll(chain.paths);
        if (!rest.isEmpty()) parts.add(new OpBGP(BasicPattern.wrap(rest)));
        OpSequence seq = OpSequence.create();
        for (Op part : parts) seq.add(part);
        Op out = seq;
        for (VarExprList binds : chain.binds) {
            out = OpExtend.create(out, binds);
        }
        HITS.incrementAndGet();
        return out;
    }

    private static boolean collect(Op op, Chain chain) {
        if (op instanceof OpBGP bgp) {
            for (Triple t : bgp.getPattern()) add(chain, t);
            return true;
        }
        if (op instanceof OpTriple t) {
            add(chain, t.getTriple());
            return true;
        }
        if (op instanceof OpPath p) {
            chain.paths.add(p);
            return true;
        }
        if (op instanceof OpTable t && t.isJoinIdentity()) {
            return true;
        }
        if (op instanceof OpSequence s) {
            for (Op e : s.getElements()) {
                if (!collect(e, chain)) return false;
            }
            return true;
        }
        if (op instanceof OpJoin j) {
            return collect(j.getLeft(), chain) && collect(j.getRight(), chain);
        }
        if (op instanceof OpExtend e) {
            chain.binds.add(e.getVarExprList());
            chain.bindInputs.add(MinusJoin.boundVars(e.getSubOp()));
            return collect(e.getSubOp(), chain);
        }
        return false;
    }

    private static void add(Chain chain, Triple t) {
        chain.triples.add(t);
        chain.afterPath.add(!chain.paths.isEmpty());
    }

    /** True when a triple after a path shares a variable other than a path's ends with a triple before the first path. */
    private static boolean pathCutsAJoin(Chain chain) {
        Set<Var> ends = pathEnds(chain);
        Set<Var> before = new HashSet<>();
        for (int i = 0; i < chain.triples.size(); i++) {
            if (!chain.afterPath.get(i)) before.addAll(varsOf(chain.triples.get(i)));
        }
        for (int i = 0; i < chain.triples.size(); i++) {
            if (!chain.afterPath.get(i)) continue;
            for (Var v : varsOf(chain.triples.get(i))) {
                if (before.contains(v) && !ends.contains(v)) return true;
            }
        }
        return false;
    }

    /** True when every BIND's variable is used nowhere else in the chain and its inputs are bound where it was. */
    private static boolean bindsCanRise(Chain chain) {
        Set<Var> used = new HashSet<>();
        for (Triple t : chain.triples) used.addAll(varsOf(t));
        used.addAll(pathEnds(chain));
        Set<Var> defined = new HashSet<>();
        for (int i = 0; i < chain.binds.size(); i++) {
            for (Var v : chain.binds.get(i).getVars()) {
                if (used.contains(v) || !defined.add(v)) return false;
                var expr = chain.binds.get(i).getExpr(v);
                if (expr == null || !chain.bindInputs.get(i).containsAll(expr.getVarsMentioned())) return false;
            }
        }
        // No BIND's expression reads another BIND's variable.
        for (VarExprList binds : chain.binds) {
            for (Var v : binds.getVars()) {
                for (Var w : binds.getExpr(v).getVarsMentioned()) {
                    if (defined.contains(w)) return false;
                }
            }
        }
        return true;
    }

    /**
     * {@code rest}: the groups of triples sharing variables that all came after the first path and share a variable
     * with a path's end; {@code core}: every other triple.
     */
    private static void split(Chain chain, List<Triple> core, List<Triple> rest) {
        int n = chain.triples.size();
        int[] group = new int[n];
        for (int i = 0; i < n; i++) group[i] = i;
        Map<Var, Integer> owner = new HashMap<>();
        for (int i = 0; i < n; i++) {
            for (Var v : varsOf(chain.triples.get(i))) {
                Integer j = owner.putIfAbsent(v, i);
                if (j != null) union(group, i, j);
            }
        }
        Set<Var> ends = pathEnds(chain);
        Set<Integer> hasBefore = new HashSet<>(), touchesEnd = new HashSet<>();
        for (int i = 0; i < n; i++) {
            int g = find(group, i);
            if (!chain.afterPath.get(i)) hasBefore.add(g);
            for (Var v : varsOf(chain.triples.get(i))) {
                if (ends.contains(v)) touchesEnd.add(g);
            }
        }
        for (int i = 0; i < n; i++) {
            int g = find(group, i);
            boolean later = !hasBefore.contains(g) && touchesEnd.contains(g);
            (later ? rest : core).add(chain.triples.get(i));
        }
    }

    private static Set<Var> pathEnds(Chain chain) {
        Set<Var> ends = new HashSet<>();
        for (OpPath p : chain.paths) addVars(ends, p.getTriplePath().getSubject(), p.getTriplePath().getObject());
        return ends;
    }

    private static Set<Var> varsOf(Triple t) {
        Set<Var> vars = new HashSet<>();
        addVars(vars, t.getSubject(), t.getPredicate(), t.getObject());
        return vars;
    }

    private static int find(int[] group, int i) {
        while (group[i] != i) {
            group[i] = group[group[i]];
            i = group[i];
        }
        return i;
    }

    private static void union(int[] group, int a, int b) {
        group[find(group, a)] = find(group, b);
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
