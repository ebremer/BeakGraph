package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexCounts;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index-backed fast path for whole-graph COUNT aggregates over a single
 * all-variable triple pattern:
 *
 * <pre>  SELECT (COUNT(*) AS ?n)            WHERE { ?s ?p ?o }   - quad count
 *  SELECT (COUNT(?x) AS ?n)           WHERE { ?s ?p ?o }   - x ∈ {s,p,o}: always bound, = quad count
 *  SELECT (COUNT(DISTINCT *) AS ?n)   WHERE { ?s ?p ?o }   - rows are distinct quads, = quad count
 *  SELECT (COUNT(DISTINCT ?s) AS ?n)  WHERE { ?s ?p ?o }   - GSPO subject-level range size
 *  SELECT (COUNT(DISTINCT ?p) AS ?n)  WHERE { ?s ?p ?o }   - GPOS predicate-level range size</pre>
 *
 * each optionally wrapped in {@code GRAPH <concrete>}. The answers come from
 * {@link IndexCounts} - a handful of select1 calls - instead of scanning every
 * row and (for DISTINCT) deduplicating hundreds of millions of ids, which is
 * what made these queries blow the endpoint's wall-clock timeout at PubMed
 * scale.
 *
 * <p>Fires only on the exact algebra shape (group with no GROUP BY keys and a
 * single supported aggregator over a single all-variable triple with pairwise
 * distinct variables, executed with the plain single-root-binding input). A
 * FILTER changes the algebra, a concrete term or repeated variable changes row
 * multiplicity, {@code COUNT(DISTINCT ?o)} has no direct index structure, and
 * the union graph needs cross-graph de-duplication - all of those fall back to
 * normal execution. The emitted row binds the aggregator's internal variable
 * (e.g. {@code ?.0}); the OpExtend/OpProject above rename it exactly as they
 * would for the normal group output.
 */
public final class AggregateCountFastPath {

    private static final Logger logger = LoggerFactory.getLogger(AggregateCountFastPath.class);

    /** Fast-path activations; observability for tests and diagnostics. */
    public static final AtomicLong HITS = new AtomicLong();

    private AggregateCountFastPath() {}

    /**
     * Answers the aggregate from index structure, or returns null (without having
     * consumed {@code input}) when it is not exactly the supported shape. The
     * caller guarantees {@code input} wraps the single-binding root iterator.
     */
    public static QueryIterator tryExecute(OpGroup opGroup, QueryIterPeek input, ExecutionContext execCxt) {
        if (!(execCxt.getActiveGraph() instanceof BeakGraph bg)) return null;
        if (bg.isGraphSetView()) return null; // several graphs: the count is a de-duplicated union, not an index level
        if (!(bg.getReader() instanceof HDF5Reader reader)) return null;

        // --- Algebra shape ---
        if (!opGroup.getGroupVars().isEmpty()) return null; // GROUP BY keys need per-group rows
        List<ExprAggregator> aggs = opGroup.getAggregators();
        if (aggs.size() != 1) return null;
        ExprAggregator ea = aggs.get(0);

        Op inner = opGroup.getSubOp();
        Node target = bg.getNamedGraph();
        if (inner instanceof OpGraph opGraph) {
            if (!opGraph.getNode().isConcrete()) return null;
            target = opGraph.getNode();
            inner = opGraph.getSubOp();
        }
        if (!(inner instanceof OpBGP opBGP)) return null;
        BasicPattern pattern = opBGP.getPattern();
        if (pattern.size() != 1) return null;
        Triple t = pattern.get(0);
        Node s = t.getSubject();
        Node p = t.getPredicate();
        Node o = t.getObject();
        if (!(s.isVariable() && p.isVariable() && o.isVariable())) return null;
        if (s.equals(p) || p.equals(o) || s.equals(o)) return null;

        // --- Which count, and is it index-answerable? ---
        long count = count(ea.getAggregator(), reader, target, s, p, o);
        if (count < 0) return null;

        // --- Input: single root binding that binds none of the pattern's variables ---
        Binding root = input.peek();
        if (root == null) return null;
        if (root.contains(Var.alloc(s)) || root.contains(Var.alloc(p)) || root.contains(Var.alloc(o))) return null;
        input.next(); // consume the single root binding

        HITS.incrementAndGet();
        logger.debug("COUNT aggregate answered from index: graph={}, agg={}, count={}",
                target, ea.getAggregator(), count);

        Binding row = BindingFactory.binding(ea.getVar(), NodeValue.makeInteger(count).asNode());
        return QueryIterPlainWrapper.create(List.of(row).iterator(), execCxt);
    }

    /** The exact count for the aggregator, or -1 when not index-answerable. */
    private static long count(Aggregator agg, HDF5Reader reader, Node graph, Node s, Node p, Node o) {
        // Row count: index rows are de-duplicated quads, so COUNT(*) and
        // COUNT(DISTINCT *) agree; COUNT(?x) counts rows where ?x is bound, and
        // every pattern variable is bound in every row of a single-pattern BGP.
        if (agg instanceof AggCount || agg instanceof AggCountDistinct) {
            return IndexCounts.quads(reader, graph);
        }
        if (agg instanceof AggCountVar) {
            Var v = singleVar(agg);
            if (v == null) return -1;
            if (v.equals(Var.alloc(s)) || v.equals(Var.alloc(p)) || v.equals(Var.alloc(o))) {
                return IndexCounts.quads(reader, graph);
            }
            return -1; // counting a variable the pattern never binds
        }
        if (agg instanceof AggCountVarDistinct) {
            Var v = singleVar(agg);
            if (v == null) return -1;
            if (v.equals(Var.alloc(s))) return IndexCounts.distinctSubjects(reader, graph);
            if (v.equals(Var.alloc(p))) return IndexCounts.distinctPredicates(reader, graph);
            return -1; // DISTINCT ?o has no direct index level
        }
        return -1;
    }

    /** The aggregator's single plain-variable argument, or null (e.g. COUNT(DISTINCT str(?s))). */
    private static Var singleVar(Aggregator agg) {
        ExprList exprs = agg.getExprList();
        if (exprs == null || exprs.size() != 1) return null;
        Expr e = exprs.get(0);
        return e.isVariable() ? e.asVar() : null;
    }
}
