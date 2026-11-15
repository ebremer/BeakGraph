package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexCounts;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
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
 * Index-backed fast path for COUNT aggregates over a single all-variable
 * triple pattern: of one graph, or of every named graph; whole, or grouped by
 * the graph or the predicate.
 *
 * <pre>  SELECT (COUNT(*) AS ?n)            WHERE { ?s ?p ?o }   - quad count
 *  SELECT (COUNT(?x) AS ?n)           WHERE { ?s ?p ?o }   - x ∈ {s,p,o}: always bound, = quad count
 *  SELECT (COUNT(DISTINCT *) AS ?n)   WHERE { ?s ?p ?o }   - rows are distinct quads, = quad count
 *  SELECT (COUNT(DISTINCT ?s) AS ?n)  WHERE { ?s ?p ?o }   - GSPO subject-level range size
 *  SELECT (COUNT(DISTINCT ?p) AS ?n)  WHERE { ?s ?p ?o }   - GPOS predicate-level range size
 *  SELECT ?p (COUNT(*) AS ?n)         WHERE { ?s ?p ?o } GROUP BY ?p   - GPOS: where each predicate's rows begin</pre>
 *
 * each optionally wrapped in {@code GRAPH <concrete>}; and over every named
 * graph of the store's dataset:
 *
 * <pre>  SELECT (COUNT(*) AS ?n)            WHERE { GRAPH ?g { ?s ?p ?o } }               - the graphs' quad counts, summed
 *  SELECT (COUNT(DISTINCT ?g) AS ?n)  WHERE { GRAPH ?g { ?s ?p ?o } }               - the graphs that hold a quad
 *  SELECT (COUNT(DISTINCT ?p) AS ?n)  WHERE { GRAPH ?g { ?s ?p ?o } }               - the graphs' predicate lists, united
 *  SELECT ?g (COUNT(*) AS ?n)         WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g   - one count a graph
 *  SELECT ?p (COUNT(*) AS ?n)         WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?p   - the graphs' predicate counts, summed
 *  SELECT ?g ?p (COUNT(*) AS ?n)      WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ?p</pre>
 *
 * (with {@code COUNT(?x)} and {@code COUNT(DISTINCT *)} for {@code COUNT(*)}
 * as above, and {@code COUNT(DISTINCT ?s)} / {@code ?p} / {@code ?g} under
 * {@code GROUP BY ?g}). The answers come from {@link IndexCounts} - a handful
 * of select1 calls a graph - instead of scanning every row and grouping it,
 * which is what made these queries blow the endpoint's wall-clock timeout at
 * PubMed scale, and what a count of every quad of a store of thousands of
 * graphs spent all but 7% of its time in (PERFORMANCE.md).
 *
 * <p>Fires only on the exact algebra shape: one supported aggregator, group
 * keys that are the predicate variable, the graph variable, both or none,
 * over a single all-variable triple with pairwise distinct variables,
 * executed with the plain single-root-binding input. A FILTER changes the
 * algebra, a concrete term or repeated variable changes row multiplicity
 * (and so does {@code GRAPH ?g { ?g ?p ?o }}), a count of distinct subjects
 * or objects across graphs or within a predicate has no index structure, and
 * the union graph needs cross-graph de-duplication - all of those fall back
 * to normal execution. So does a variable graph in any dataset but the
 * store's own ({@code FROM NAMED} chooses the named graphs), whose named
 * graphs are those {@link GraphVarBG#namedGraphIds} lists. The emitted rows
 * bind the group keys and the aggregator's internal variable (e.g.
 * {@code ?.0}); the OpExtend/OpProject above rename it exactly as they would
 * for the normal group output. A graph or predicate without a row has no
 * group, as in the normal output; with no group keys there is one row, 0 for
 * no rows.
 */
public final class AggregateCountFastPath {

    private static final Logger logger = LoggerFactory.getLogger(AggregateCountFastPath.class);

    /** Fast-path activations; observability for tests and diagnostics. */
    public static final AtomicLong HITS = new AtomicLong();

    /** What the aggregate counts in a group. */
    private enum Counted { ROWS, DISTINCT_SUBJECTS, DISTINCT_PREDICATES, DISTINCT_GRAPHS }

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
        List<ExprAggregator> aggs = opGroup.getAggregators();
        if (aggs.size() != 1) return null;
        ExprAggregator ea = aggs.get(0);

        Op inner = opGroup.getSubOp();
        Node target = bg.getNamedGraph();
        Var g = null; // the variable of GRAPH ?g: the pattern runs over every named graph
        if (inner instanceof OpGraph opGraph) {
            Node graphNode = opGraph.getNode();
            if (graphNode.isVariable()) {
                g = Var.alloc(graphNode);
            } else if (graphNode.isConcrete()) {
                target = graphNode;
            } else {
                return null;
            }
            inner = opGraph.getSubOp();
        }
        if (!(inner instanceof OpBGP opBGP)) return null;
        BasicPattern pattern = opBGP.getPattern();
        if (pattern.size() != 1) return null;
        Triple t = pattern.get(0);
        if (!(t.getSubject().isVariable() && t.getPredicate().isVariable() && t.getObject().isVariable())) return null;
        Var s = Var.alloc(t.getSubject());
        Var p = Var.alloc(t.getPredicate());
        Var o = Var.alloc(t.getObject());
        if (s.equals(p) || p.equals(o) || s.equals(o)) return null;
        if (g != null && (g.equals(s) || g.equals(p) || g.equals(o))) return null; // the graph's name constrains the rows

        // --- Group keys: none, the predicate, the graph, or both ---
        VarExprList keys = opGroup.getGroupVars();
        if (!keys.getExprs().isEmpty()) return null; // GROUP BY (expr AS ?v)
        boolean byGraph = false;
        boolean byPredicate = false;
        for (Var key : keys.getVars()) {
            if (key.equals(p) && !byPredicate) {
                byPredicate = true;
            } else if (key.equals(g) && !byGraph) {
                byGraph = true;
            } else {
                return null; // per-subject or per-object groups need the rows
            }
        }

        Counted counted = counted(ea.getAggregator(), s, p, o, g);
        if (counted == null) return null;

        // --- Input: single root binding that binds none of the pattern's variables ---
        Binding root = input.peek();
        if (root == null) return null;
        if (root.contains(s) || root.contains(p) || root.contains(o) || (g != null && root.contains(g))) return null;

        // --- The rows, or null when this count has no index structure ---
        NodeTable nodeTable = reader.getNodeTable();
        Var out = ea.getVar();
        List<Binding> rows;
        if (g == null) {
            rows = byPredicate ? oneGraphByPredicate(reader, target, counted, p, out, nodeTable)
                               : oneGraph(reader, target, counted, out);
        } else {
            if (!(execCxt.getDataset() instanceof BGDatasetGraph dsg)) return null; // FROM NAMED chose the graphs
            BeakGraph store = dsg.getBeakGraph();
            if (store.isGraphSetView() || store.getReader() != reader) return null;
            if (!(reader.getDictionary() instanceof PositionalDictionaryReader dict) || dict.isEmpty()) return null;
            long[] graphs = GraphVarBG.namedGraphIds(dict, store);
            rows = everyGraph(reader, graphs, counted, byGraph, byPredicate, g, p, out, nodeTable);
        }
        if (rows == null) return null;
        input.next(); // consume the single root binding

        HITS.incrementAndGet();
        logger.debug("COUNT aggregate answered from index: graph={}, agg={}, group keys={}, rows={}",
                (g == null) ? target : g, ea.getAggregator(), keys.getVars(), rows.size());
        return QueryIterPlainWrapper.create(rows.iterator(), execCxt);
    }

    /** What the aggregator counts, or null when it is not one answered here. */
    private static Counted counted(Aggregator agg, Var s, Var p, Var o, Var g) {
        // Row count: index rows are de-duplicated quads, so COUNT(*) and
        // COUNT(DISTINCT *) agree (a variable graph is part of the row);
        // COUNT(?x) counts rows where ?x is bound, and every pattern variable
        // is bound in every row of a single-pattern BGP, as the graph's is.
        if (agg instanceof AggCount || agg instanceof AggCountDistinct) {
            return Counted.ROWS;
        }
        if (agg instanceof AggCountVar) {
            Var v = singleVar(agg);
            return (v != null && (v.equals(s) || v.equals(p) || v.equals(o) || v.equals(g))) ? Counted.ROWS : null;
        }
        if (agg instanceof AggCountVarDistinct) {
            Var v = singleVar(agg);
            if (v == null) return null;
            if (v.equals(s)) return Counted.DISTINCT_SUBJECTS;
            if (v.equals(p)) return Counted.DISTINCT_PREDICATES;
            if (v.equals(g)) return Counted.DISTINCT_GRAPHS;
            return null; // DISTINCT ?o has no direct index level
        }
        return null;
    }

    /** No group keys, one graph: one row, whatever the count. */
    private static List<Binding> oneGraph(HDF5Reader reader, Node graph, Counted counted, Var out) {
        long count = switch (counted) {
            case ROWS -> IndexCounts.quads(reader, graph);
            case DISTINCT_SUBJECTS -> IndexCounts.distinctSubjects(reader, graph);
            case DISTINCT_PREDICATES -> IndexCounts.distinctPredicates(reader, graph);
            case DISTINCT_GRAPHS -> -1;
        };
        return (count < 0) ? null : List.of(row(out, count).build());
    }

    /** {@code GROUP BY ?p} in one graph: a row a predicate of the graph. */
    private static List<Binding> oneGraphByPredicate(HDF5Reader reader, Node graph, Counted counted, Var p, Var out, NodeTable nodeTable) {
        if (counted != Counted.ROWS) return null;
        List<Binding> rows = new ArrayList<>();
        boolean answered = IndexCounts.quadsByPredicate(reader, graph, (pid, quads) -> {
            Node predicate = nodeTable.getNodeForNodeId(NodeId.pack(NodeType.PREDICATE, pid));
            if (predicate != null) {
                rows.add(row(out, quads).add(p, predicate).build());
            }
        });
        return answered ? rows : null;
    }

    /** {@code GRAPH ?g}: the named graphs one after another, their counts summed or reported as the group keys ask. */
    private static List<Binding> everyGraph(HDF5Reader reader, long[] graphs, Counted counted, boolean byGraph, boolean byPredicate,
            Var g, Var p, Var out, NodeTable nodeTable) {
        List<Binding> rows = new ArrayList<>();
        if (byPredicate) {
            // The predicate's rows, of each graph or of all: only the rows themselves have an index structure here.
            if (counted != Counted.ROWS) return null;
            Map<Long, long[]> totals = new LinkedHashMap<>();
            for (long gid : graphs) {
                Node graph = byGraph ? nodeTable.getNodeForNodeId(NodeId.pack(NodeType.GRAPH, gid)) : null;
                boolean answered = IndexCounts.quadsByPredicate(reader, gid, (pid, quads) -> {
                    if (!byGraph) {
                        totals.computeIfAbsent(pid, k -> new long[1])[0] += quads;
                        return;
                    }
                    Node predicate = nodeTable.getNodeForNodeId(NodeId.pack(NodeType.PREDICATE, pid));
                    if (graph != null && predicate != null) {
                        rows.add(row(out, quads).add(g, graph).add(p, predicate).build());
                    }
                });
                if (!answered) return null;
            }
            for (Map.Entry<Long, long[]> e : totals.entrySet()) {
                Node predicate = nodeTable.getNodeForNodeId(NodeId.pack(NodeType.PREDICATE, e.getKey()));
                if (predicate != null) {
                    rows.add(row(out, e.getValue()[0]).add(p, predicate).build());
                }
            }
            return rows;
        }
        if (byGraph) {
            for (long gid : graphs) {
                long quads = IndexCounts.quads(reader, gid);
                if (quads < 0) return null;
                if (quads == 0) continue; // a graph without a row has no group
                long count = switch (counted) {
                    case ROWS -> quads;
                    case DISTINCT_SUBJECTS -> IndexCounts.distinctSubjects(reader, gid);
                    case DISTINCT_PREDICATES -> IndexCounts.distinctPredicates(reader, gid);
                    case DISTINCT_GRAPHS -> 1;
                };
                if (count < 0) return null;
                Node graph = nodeTable.getNodeForNodeId(NodeId.pack(NodeType.GRAPH, gid));
                if (graph != null) {
                    rows.add(row(out, count).add(g, graph).build());
                }
            }
            return rows;
        }
        // No group keys: one row over all the graphs.
        long total = 0;
        switch (counted) {
            case ROWS, DISTINCT_GRAPHS -> {
                for (long gid : graphs) {
                    long quads = IndexCounts.quads(reader, gid);
                    if (quads < 0) return null;
                    total += (counted == Counted.ROWS) ? quads : (quads > 0 ? 1 : 0);
                }
            }
            case DISTINCT_PREDICATES -> {
                // Each graph lists its predicates once; the lists are short, so their union is a set of ids.
                Set<Long> predicates = new HashSet<>();
                for (long gid : graphs) {
                    if (!IndexCounts.quadsByPredicate(reader, gid, (pid, quads) -> predicates.add(pid))) return null;
                }
                total = predicates.size();
            }
            case DISTINCT_SUBJECTS -> {
                return null; // a subject may be in many graphs: counting it once needs the ids themselves
            }
        }
        rows.add(row(out, total).build());
        return rows;
    }

    private static BindingBuilder row(Var out, long count) {
        return Binding.builder().add(out, NodeValue.makeInteger(count).asNode());
    }

    /** The aggregator's single plain-variable argument, or null (e.g. COUNT(DISTINCT str(?s))). */
    private static Var singleVar(Aggregator agg) {
        ExprList exprs = agg.getExprList();
        if (exprs == null || exprs.size() != 1) return null;
        Expr e = exprs.get(0);
        return e.isVariable() ? e.asVar() : null;
    }
}
