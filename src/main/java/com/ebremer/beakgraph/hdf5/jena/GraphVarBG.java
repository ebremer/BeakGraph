package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpAssign;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpDisjunction;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.iterator.QueryIterAbortable;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprList;
import static org.apache.jena.sparql.engine.main.solver.SolverLib.makeAbortable;

/**
 * {@code GRAPH ?g { ... }} on ids: one walk over the dataset's named graphs,
 * the block solved in each by the id-level iterators with {@code ?g} bound to
 * the graph's id.
 *
 * <p>Jena answers this shape by executing the block once for every named
 * graph: it substitutes the binding into the algebra, asks the dataset for a
 * graph view, builds an execution context and a whole iterator pipeline, and
 * assigns {@code ?g} to each row afterwards. A store of tiled data holds
 * thousands of graphs, and for a block that matches in few of them that
 * setup was the query: 88% of a subject lookup across 5,000 graphs, 58% of
 * it building pipelines (PERFORMANCE.md). Here a graph costs a child binding
 * and the iterators that read it.
 *
 * <p>What it answers, and what it leaves to Jena:
 * <ul>
 *   <li><b>The block</b>: a basic graph pattern, or patterns under
 *       {@code FILTER}, {@code OPTIONAL} and {@code UNION} - the algebra's
 *       filter, sequence, conditional and union, and the assignment and
 *       the disjunction ARQ makes of {@code FILTER(?x = <iri>)} and of
 *       {@code FILTER(a || b)} ({@link Plan}). These are
 *       the operators Jena itself runs by handing each row of the left to
 *       the right, which is how they are run here, a graph at a time.
 *       {@code MINUS}, {@code BIND}, a sub-select, a property path, an
 *       optional or a join that cannot be run that way, {@code EXISTS}
 *       inside a filter and a triple term that holds variables are left
 *       alone; so is a filter on {@code geof:sfIntersects} in a store with
 *       the spatial index, which Jena's route answers from that index.</li>
 *   <li><b>The graph variable inside the block</b>: SPARQL joins the graph's
 *       name to the block's solutions <i>afterwards</i>, so a filter inside
 *       the block does not see {@code ?g}. For a plain pattern, binding
 *       {@code ?g} first and solving is that very join, and
 *       {@code GRAPH ?g { ?g ?p ?o }} is walked. For any other block the walk
 *       is taken only when nothing in it mentions {@code ?g}: then binding
 *       it first cannot be seen from inside.</li>
 *   <li><b>The dataset</b>: the store's own ({@link BGDatasetGraph}). A
 *       {@code FROM NAMED} dataset has its own set of named graphs.</li>
 *   <li><b>The graphs</b>: exactly those {@code listGraphNodes()} lists - the
 *       stored graphs but the default graph, in the same order - so the two
 *       ways give the same rows.</li>
 *   <li><b>{@code ?g} bound already</b> (a join, {@code VALUES}): that
 *       binding goes to Jena's operator, whose rules for a single graph
 *       (the synthetic default and union graph names, a graph the dataset
 *       does not hold, a literal) are then the ones applied.</li>
 * </ul>
 *
 * <p>Two kinds of parallelism, both for the root binding only (a binding
 * that carries variables is an outer row, and its block runs once a row):
 * <ul>
 *   <li><b>A large graph met on the way</b> is scanned in parallel, under
 *       the conditions of {@link ScanChunks}, as it was when Jena ran the
 *       pattern graph by graph.</li>
 *   <li><b>Many graphs</b>: once a walk has produced as many rows as
 *       {@code beakgraph.scan.parallel.threshold} says a parallel scan is
 *       worth (65,536), the graphs still to visit are divided among workers,
 *       each walking its share ({@link ParallelScan}) - where a row costs
 *       more than reading it: a join, an optional. A store of thousands of
 *       small graphs never met the per-graph condition, and its joins ran on
 *       one thread however many rows they made. A lookup that finds little
 *       stays on one thread to its end, and a {@code LIMIT} that is met
 *       early starts no worker.
 *       <ul>
 *         <li><b>Not for a single pattern.</b> Its rows cost little to read
 *             and as much to hand from one thread to another, and one thread
 *             consumes them either way: measured, workers made such scans
 *             13 to 17% faster at best and 12% slower at worst, for every
 *             core of the machine.</li>
 *         <li><b>Not through a filter.</b> Filters are evaluated by the
 *             thread that consumes the rows, never by a worker; a block with
 *             a filter that is not its outermost operator is walked on one
 *             thread.</li>
 *       </ul></li>
 * </ul>
 *
 * <p>Neither limit holds where the rows are not handed on at all: under a
 * {@code GROUP BY} or an aggregate, each worker keeps the rows it reads and
 * groups them itself, with filters of its own ({@link GroupOnIds}, which
 * takes the block as {@link Prepared} makes it ready here).
 */
public final class GraphVarBG {

    /** {@code GRAPH ?g} blocks answered here; tests pin which shapes are, and which fall back. */
    public static final AtomicLong HITS = new AtomicLong();

    /** Walks that handed the graphs still to visit to parallel workers; observability for tests and diagnostics. */
    public static final AtomicLong PARALLEL = new AtomicLong();

    /** {@code -Dbeakgraph.graphvar.fastpath=false} leaves every {@code GRAPH ?g} to Jena's operator. */
    private static volatile boolean enabled = Boolean.parseBoolean(System.getProperty("beakgraph.graphvar.fastpath", "true"));

    /**
     * Stands for a term the rows before a pattern will have bound, when the pattern's triples are
     * ordered: {@link BeakGraph#BOUND}, which the reorder costs as bound and never counts as a term.
     */
    private static final Node BOUND = BeakGraph.BOUND;

    private GraphVarBG() {}

    /** Switches this path on or off (tests compare the two ways); returns the previous setting. */
    public static boolean setEnabled(boolean on) {
        boolean previous = enabled;
        enabled = on;
        return previous;
    }

    // ------------------------------------------------------------------ the block, as the walk runs it

    /**
     * A block's algebra as it is evaluated in one graph: every node takes a
     * row and gives the rows that extend it.
     */
    private sealed interface Plan permits Bgp, Filtered, Steps, Optional, Either, Assigned {}

    /** The triples in the order they are read; {@code hints} are the filter's expressions the reads may narrow by. */
    private record Bgp(Triple[] triples, ExprList hints) implements Plan {}

    /** The rows of {@code sub} that satisfy every expression. */
    private record Filtered(ExprList exprs, Plan sub) implements Plan {}

    /** Each step run on every row of the one before it. */
    private record Steps(Plan[] steps) implements Plan {}

    /** Every row of {@code left}, extended by {@code right} where it has rows. */
    private record Optional(Plan left, Plan right) implements Plan {}

    /** The rows of each side in turn: a union, or the disjunction ARQ makes of {@code FILTER(a || b)}. */
    private record Either(Plan[] sides) implements Plan {}

    /**
     * The rows of {@code sub}, each with {@code vars} bound to the terms of
     * {@code ids}: what ARQ makes of {@code FILTER(?x = <iri>)}, having put
     * the IRI in the variable's place in the pattern. A row that holds
     * another term for a variable is no row.
     */
    private record Assigned(Plan sub, Var[] vars, long[] ids) implements Plan {}

    /**
     * Whether the walk evaluates {@code op}: the operators of {@link Plan}
     * over patterns without triple-term patterns, with filters that neither
     * hold a graph pattern of their own nor are answered from the spatial
     * index. Decided before the input is touched.
     */
    private static boolean walkable(Op op, BeakGraph root) {
        if (op instanceof OpBGP bgp) {
            for (Triple t : bgp.getPattern()) {
                if (isPattern(t.getSubject()) || isPattern(t.getPredicate()) || isPattern(t.getObject())) return false;
            }
            return true;
        }
        if (op instanceof OpFilter filter) {
            for (Expr e : filter.getExprs()) {
                if (holdsPattern(e)) return false;
            }
            // Jena's route seeds such a filter's pattern from the spatial index, graph by graph: leave it that.
            if (PatternMatchBG.getSpatialContext(filter.getExprs()) != null && SpatialIndexIterator.isAvailable(root)) return false;
            return walkable(filter.getSubOp(), root);
        }
        if (op instanceof OpSequence sequence) {
            for (Op step : sequence.getElements()) {
                if (!walkable(step, root)) return false;
            }
            return sequence.size() > 0;
        }
        if (op instanceof OpConditional conditional) {
            return walkable(conditional.getLeft(), root) && walkable(conditional.getRight(), root);
        }
        if (op instanceof OpUnion union) {
            return walkable(union.getLeft(), root) && walkable(union.getRight(), root);
        }
        if (op instanceof OpDisjunction disjunction) {
            for (Op side : disjunction.getElements()) {
                if (!walkable(side, root)) return false;
            }
            return disjunction.size() > 0;
        }
        if (op instanceof OpAssign assign) {
            // Constant IRIs and plain strings only: for those "the same value" (what the assignment asks of a
            // variable bound already) is "the same term" (what an id says). A row holds four bindings a layer.
            List<Var> vars = assign.getVarExprList().getVars();
            if (vars.isEmpty() || vars.size() > 4) return false;
            for (Var v : vars) {
                Expr e = assign.getVarExprList().getExpr(v);
                if (e == null || !e.isConstant() || !termIsValue(e.getConstant().asNode())) return false;
            }
            return walkable(assign.getSubOp(), root);
        }
        return false;
    }

    /** An IRI, or a string without a language: a term that is equal in value to no other term. */
    private static boolean termIsValue(Node n) {
        return n.isURI() || (n.isLiteral() && n.getLiteralLanguage().isEmpty()
                && org.apache.jena.datatypes.xsd.XSDDatatype.XSDstring.equals(n.getLiteralDatatype()));
    }

    /** A triple term that holds variables: matched by unification, which the walk leaves to the ordinary path. */
    private static boolean isPattern(Node n) {
        return n.isTripleTerm() && !n.isConcrete();
    }

    /** EXISTS and NOT EXISTS: a graph pattern of their own, which is run against the graph a row is being solved in. */
    static boolean holdsPattern(Expr e) {
        if (e instanceof ExprFunctionOp) return true;
        if (e.isFunction()) {
            for (Expr arg : e.getFunction().getArgs()) {
                if (holdsPattern(arg)) return true;
            }
        }
        return false;
    }

    /**
     * The plan of {@code op}, its patterns ordered as the executor orders
     * them: most selective first against the first input row, and against
     * the variables the plan has bound by then ({@code bound}, which grows as
     * the plan is built). Null where filter placement gives a shape the walk
     * does not run.
     */
    private static Plan compile(Op op, Binding first, Set<Var> bound, ReorderTransformation transform, NodeTable nodeTable) {
        if (op instanceof OpAssign assign) {
            Plan sub = compile(assign.getSubOp(), first, bound, transform, nodeTable);
            if (sub == null) return null;
            List<Var> vars = assign.getVarExprList().getVars();
            long[] ids = new long[vars.size()];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = nodeTable.getNodeIdForNode(assign.getVarExprList().getExpr(vars.get(i)).getConstant().asNode());
                if (NodeId.isDoesNotExist(ids[i])) return null; // a term the store does not hold has no id to bind
            }
            bound.addAll(vars);
            return new Assigned(sub, vars.toArray(Var[]::new), ids);
        }
        if (op instanceof OpBGP bgp) {
            BasicPattern pattern = reorder(bgp.getPattern(), first, bound, transform);
            addVars(pattern, bound);
            return new Bgp(pattern.getList().toArray(Triple[]::new), null);
        }
        if (op instanceof OpFilter filter) {
            if (filter.getSubOp() instanceof OpBGP bgp) {
                // As OpExecutorBG.optimizeExecuteTriples: order the triples, then put each filter after the triples
                // that bind its variables; a filter placed on a pattern is also that pattern's range hints.
                BasicPattern pattern = reorder(bgp.getPattern(), first, bound, transform);
                addVars(pattern, bound);
                return placed(TransformFilterPlacement.transform(filter.getExprs(), pattern));
            }
            Plan sub = compile(filter.getSubOp(), first, bound, transform, nodeTable);
            return (sub == null) ? null : new Filtered(filter.getExprs(), sub);
        }
        if (op instanceof OpSequence sequence) {
            Plan[] steps = new Plan[sequence.size()];
            for (int i = 0; i < steps.length; i++) {
                steps[i] = compile(sequence.get(i), first, bound, transform, nodeTable);
                if (steps[i] == null) return null;
            }
            return new Steps(steps);
        }
        if (op instanceof OpConditional conditional) {
            Plan left = compile(conditional.getLeft(), first, bound, transform, nodeTable);
            // The right side sees the left's variables bound; what it binds itself, a later step cannot count on.
            Plan right = (left == null) ? null : compile(conditional.getRight(), first, new HashSet<>(bound), transform, nodeTable);
            return (right == null) ? null : new Optional(left, right);
        }
        if (op instanceof OpUnion union) {
            return either(List.of(union.getLeft(), union.getRight()), first, bound, transform, nodeTable);
        }
        if (op instanceof OpDisjunction disjunction) {
            return either(disjunction.getElements(), first, bound, transform, nodeTable);
        }
        return null;
    }

    /** The sides of a union or a disjunction, each planned for itself: what one binds, the others and what follows cannot count on. */
    private static Plan either(List<Op> ops, Binding first, Set<Var> bound, ReorderTransformation transform, NodeTable nodeTable) {
        Plan[] sides = new Plan[ops.size()];
        for (int i = 0; i < sides.length; i++) {
            sides[i] = compile(ops.get(i), first, new HashSet<>(bound), transform, nodeTable);
            if (sides[i] == null) return null;
        }
        return new Either(sides);
    }

    /** What filter placement made of a filter over a pattern: patterns, filters over them, and sequences of those. */
    private static Plan placed(Op op) {
        if (op instanceof OpBGP bgp) {
            return new Bgp(bgp.getPattern().getList().toArray(Triple[]::new), null);
        }
        if (op instanceof OpFilter filter) {
            if (filter.getSubOp() instanceof OpBGP bgp) {
                return new Filtered(filter.getExprs(), new Bgp(bgp.getPattern().getList().toArray(Triple[]::new), filter.getExprs()));
            }
            Plan sub = placed(filter.getSubOp());
            return (sub == null) ? null : new Filtered(filter.getExprs(), sub);
        }
        if (op instanceof OpSequence sequence) {
            Plan[] steps = new Plan[sequence.size()];
            for (int i = 0; i < steps.length; i++) {
                steps[i] = placed(sequence.get(i));
                if (steps[i] == null) return null;
            }
            return (steps.length == 0) ? null : new Steps(steps);
        }
        return null;
    }

    private static BasicPattern reorder(BasicPattern pattern, Binding first, Set<Var> bound, ReorderTransformation transform) {
        if (transform == null || pattern.size() < 2) {
            return pattern;
        }
        BasicPattern probe = Substitute.substitute(pattern, first);
        if (!bound.isEmpty()) {
            BasicPattern known = new BasicPattern();
            for (Triple t : probe) {
                known.add(Triple.create(known(t.getSubject(), bound), known(t.getPredicate(), bound), known(t.getObject(), bound)));
            }
            probe = known;
        }
        ReorderProc proc = transform.reorderIndexes(probe);
        BasicPattern reordered = proc.reorder(pattern);
        if (!reordered.getList().equals(pattern.getList())) {
            OpExecutorBG.REORDERS.incrementAndGet();
        }
        return reordered;
    }

    private static Node known(Node n, Set<Var> bound) {
        return (n.isVariable() && bound.contains(Var.alloc(n))) ? BOUND : n;
    }

    private static void addVars(BasicPattern pattern, Set<Var> bound) {
        for (Triple t : pattern) {
            if (t.getSubject().isVariable()) bound.add(Var.alloc(t.getSubject()));
            if (t.getPredicate().isVariable()) bound.add(Var.alloc(t.getPredicate()));
            if (t.getObject().isVariable()) bound.add(Var.alloc(t.getObject()));
        }
    }

    private static boolean hasFilter(Plan plan) {
        return switch (plan) {
            case Bgp b -> false;
            case Filtered f -> true;
            case Steps s -> {
                for (Plan step : s.steps()) {
                    if (hasFilter(step)) yield true;
                }
                yield false;
            }
            case Optional o -> hasFilter(o.left()) || hasFilter(o.right());
            case Either e -> {
                for (Plan side : e.sides()) {
                    if (hasFilter(side)) yield true;
                }
                yield false;
            }
            case Assigned a -> hasFilter(a.sub());
        };
    }

    /**
     * Whether a row of {@code plan} costs more than reading it: a second
     * pattern read for every row of the first, an optional tried for every
     * row. That is the work workers can take off the consuming thread; the
     * rows of a single pattern they can only hand over.
     */
    private static boolean rowsDoWork(Plan plan) {
        return switch (plan) {
            case Bgp b -> b.triples().length >= 2;
            case Filtered f -> rowsDoWork(f.sub());
            case Steps s -> s.steps().length >= 2 || rowsDoWork(s.steps()[0]);
            case Optional o -> true;
            case Either e -> {
                for (Plan side : e.sides()) {
                    if (rowsDoWork(side)) yield true;
                }
                yield false;
            }
            case Assigned a -> rowsDoWork(a.sub());
        };
    }

    /**
     * {@code plan} with filter expressions of its own: for a thread that
     * evaluates them while other threads evaluate theirs. An expression may
     * keep what it has worked out (a function bound on first use, a pattern
     * compiled), and Jena evaluates one on one thread at a time; and the
     * copies read their variables by the rows' ids ({@link RowValue}). The
     * hints of a pattern are only read, and stay the one list.
     */
    private static Plan own(Plan plan, NodeTable nodeTable) {
        return switch (plan) {
            case Bgp b -> b;
            case Filtered f -> new Filtered(RowValue.own(f.exprs(), nodeTable), own(f.sub(), nodeTable));
            case Steps s -> new Steps(own(s.steps(), nodeTable));
            case Optional o -> new Optional(own(o.left(), nodeTable), own(o.right(), nodeTable));
            case Either e -> new Either(own(e.sides(), nodeTable));
            case Assigned a -> new Assigned(own(a.sub(), nodeTable), a.vars(), a.ids());
        };
    }

    private static Plan[] own(Plan[] plans, NodeTable nodeTable) {
        Plan[] copies = new Plan[plans.length];
        for (int i = 0; i < plans.length; i++) {
            copies[i] = own(plans[i], nodeTable);
        }
        return copies;
    }

    /**
     * The pattern a row of {@code plan} is first read from - the one whose
     * read, when it is a scan, can be split by position - or null where the
     * plan starts with sides that are read in turn.
     */
    private static Bgp leftmost(Plan plan) {
        return switch (plan) {
            case Bgp b -> b;
            case Filtered f -> leftmost(f.sub());
            case Steps s -> leftmost(s.steps()[0]);
            case Optional o -> leftmost(o.left());
            case Either e -> null;
            case Assigned a -> leftmost(a.sub());
        };
    }

    // ------------------------------------------------------------------ the entry points

    /**
     * The id-level execution of {@code opGraph}, or null - with {@code input}
     * untouched - when it is not a shape answered here.
     *
     * @param jena Jena's own operator for {@code opGraph} over a given input,
     *             used for the bindings that hold the graph variable already
     */
    static QueryIterator tryExecute(OpGraph opGraph, QueryIterator input, ExecutionContext execCxt, UnaryOperator<QueryIterator> jena) {
        Site site = site(opGraph, execCxt);
        if (site == null) return null;
        if (!input.hasNext()) {
            return input;
        }
        QueryIterPeek peek = QueryIterPeek.create(input, execCxt);
        Prepared prepared = site.prepare(peek.peek());
        if (prepared == null) {
            return jena.apply(peek); // filter placement gave a shape the walk does not run
        }
        HITS.incrementAndGet();
        PatternMatchBG.HITS.incrementAndGet();
        if (prepared.empty) {
            peek.close();
            return QueryIterNullIterator.create(execCxt);
        }
        return new Stage(peek, execCxt, jena, prepared);
    }

    /**
     * {@code GRAPH ?g { ... }} made ready for a caller that takes the rows
     * itself, under the single binding {@code first} that binds nothing: an
     * aggregate over them ({@link GroupOnIds}). Null when the walk would not
     * answer the block.
     */
    static Prepared prepareWalk(OpGraph opGraph, Binding first, ExecutionContext execCxt) {
        Site site = site(opGraph, execCxt);
        return (site == null) ? null : site.prepare(first);
    }

    /**
     * A block that runs in one graph - the active graph, or the graph a
     * {@code GRAPH <iri>} names - made ready as {@link #prepareWalk} makes a
     * block under a graph variable: the same plan, run in that one graph.
     * Null for a block the walk would not answer, for a view of several
     * graphs or of the union graph (whose rows are a union without
     * duplicates), and for the empty pattern.
     */
    static Prepared prepareBlock(Op block, BeakGraph graph, Binding first) {
        if (graph.isGraphSetView()) return null;
        Node name = graph.getNamedGraph();
        if (name == null || Quad.isUnionGraph(name)) return null;
        if (!(graph.getReader() instanceof HDF5Reader reader)) return null;
        if (!(reader.getDictionary() instanceof PositionalDictionaryReader dict) || dict.isEmpty()) return null;
        if (block instanceof OpBGP bgp && bgp.getPattern().isEmpty()) return null;
        if (!walkable(block, graph)) return null;
        Plan plan = compile(block, first, new HashSet<>(), graph.getReorderTransform(), reader.getNodeTable());
        if (plan == null) return null;
        Node term = Quad.isDefaultGraph(name) ? Quad.defaultGraphIRI : name;
        long gid = dict.getGraphs().locate(term);
        return Prepared.of(null, term, plan, null, graph, reader, dict, (gid < 1) ? new long[0] : new long[] {gid});
    }

    /** A {@code GRAPH ?g} block that has passed the checks that need no input row. */
    private record Site(Var graphVar, Op block, BGDatasetGraph dsg, BeakGraph root, HDF5Reader reader, PositionalDictionaryReader dict) {

        /** The block planned against {@code first}, or null where filter placement gives a shape the walk does not run. */
        Prepared prepare(Binding first) {
            // The block runs in each named graph in turn, so its patterns are costed by their exact
            // counts over all of them - the root graph's own transform counts only the stored
            // default graph.
            long[] graphs = namedGraphIds(dict, root);
            if (block instanceof OpBGP bgp) {
                graphs = holdingEveryPredicate(reader, dict, graphs, bgp.getPattern());
            }
            Plan plan = compile(block, first, new HashSet<>(), root.reorderTransformForGraphs(graphs), reader.getNodeTable());
            return (plan == null) ? null : Prepared.of(graphVar, null, plan, dsg, root, reader, dict, graphs);
        }
    }

    /**
     * {@code graphs}, in their order, less those lacking a predicate one of {@code pattern}'s triples
     * names: a BGP has no rows in a graph without one of its predicates, and each graph the walk
     * visits costs an iterator per row.
     */
    static long[] holdingEveryPredicate(HDF5Reader reader, PositionalDictionaryReader dict, long[] graphs, BasicPattern pattern) {
        long[] kept = graphs;
        for (Triple t : pattern) {
            Node p = t.getPredicate();
            if (!p.isURI() || BeakGraph.BOUND.equals(p)) continue;
            long[] holders = reader.graphsWithPredicate(dict.getPredicates().locate(p));
            if (holders == null) return graphs; // no GPOS: every graph may hold it
            kept = java.util.Arrays.stream(kept).filter(g -> java.util.Arrays.binarySearch(holders, g) >= 0).toArray();
        }
        return kept;
    }

    /** Whether {@code opGraph} is a block the walk answers, as far as can be said before the input is touched. */
    private static Site site(OpGraph opGraph, ExecutionContext execCxt) {
        if (!enabled) return null;
        Node graphNode = opGraph.getNode();
        if (!graphNode.isVariable()) return null;
        Var graphVar = Var.alloc(graphNode);
        Op block = opGraph.getSubOp();
        boolean plain = block instanceof OpBGP;
        if (plain && ((OpBGP) block).getPattern().isEmpty()) return null; // GRAPH ?g { }: the list of graphs, Jena's
        if (!(execCxt.getDataset() instanceof BGDatasetGraph dsg)) return null;
        BeakGraph root = dsg.getBeakGraph();
        if (root.isGraphSetView()) return null;
        if (!(root.getReader() instanceof HDF5Reader reader)) return null;
        if (!(reader.getDictionary() instanceof PositionalDictionaryReader dict) || dict.isEmpty()) return null;
        // The graph the block would run against must be a view of the same store.
        if (!(execCxt.getActiveGraph() instanceof BeakGraph active) || active.getReader() != reader) return null;
        if (!walkable(block, root)) return null;
        // A filter inside the block must not see ?g, and the walk binds it first: only where nothing inside could tell.
        if (!plain && OpVars.mentionedVars(block).contains(graphVar)) return null;
        return new Site(graphVar, block, dsg, root, reader, dict);
    }

    /**
     * The ids of the dataset's named graphs, in the order of the stored graph
     * list: what {@link BGDatasetGraph#listGraphNodes()} lists as terms - every
     * stored graph but the default graph (under either of its names), the
     * union graph's name, and the graph the dataset's own default graph is a
     * view of.
     */
    public static long[] namedGraphIds(PositionalDictionaryReader dict, BeakGraph root) {
        Dictionary graphs = dict.getGraphs();
        long defaultGraph = graphs.locate(Quad.defaultGraphIRI);
        long generated = graphs.locate(Quad.defaultGraphNodeGenerated);
        long union = graphs.locate(Quad.unionGraph);
        Node own = root.getNamedGraph();
        long ownId = (own == null) ? -1 : graphs.locate(own);
        return dict.streamGraphIds()
                .filter(id -> id >= 1 && id != defaultGraph && id != generated && id != union && id != ownId)
                .toArray();
    }

    // ------------------------------------------------------------------ a block, and the graphs it runs in

    /**
     * A block made ready to run: its plan, and the graphs it runs in. What
     * the walk ({@link Stage}) is made of, and what an aggregate takes the
     * block's rows from, graph by graph or a part of a large graph at a time
     * ({@link GroupOnIds}).
     */
    static final class Prepared {
        final Var graphVar;             // the variable of GRAPH ?g, bound to each graph in turn; null for a block in one given graph
        final Node graphTerm;           // the one graph's term, where there is no variable
        final Plan plan;                // the block, without its outermost filters
        final List<ExprList> filters;   // those filters, innermost first
        final boolean empty;            // a term of the pattern is not in the store: no graph has a row
        final BGDatasetGraph dsg;       // the dataset the graphs are views of; null for a block in one given graph
        final BeakGraph root;
        final HDF5Reader reader;
        final PositionalDictionaryReader dict;
        final NodeTable nodeTable;
        final long[] graphs;            // the ids of the graphs the block runs in, in the order they are visited

        private Prepared(Var graphVar, Node graphTerm, Plan plan, List<ExprList> filters, boolean empty, BGDatasetGraph dsg, BeakGraph root,
                HDF5Reader reader, PositionalDictionaryReader dict, long[] graphs) {
            this.graphVar = graphVar;
            this.graphTerm = graphTerm;
            this.plan = plan;
            this.filters = filters;
            this.empty = empty;
            this.dsg = dsg;
            this.root = root;
            this.reader = reader;
            this.dict = dict;
            this.nodeTable = reader.getNodeTable();
            this.graphs = graphs;
        }

        static Prepared of(Var graphVar, Node graphTerm, Plan plan, BGDatasetGraph dsg, BeakGraph root, HDF5Reader reader,
                PositionalDictionaryReader dict, long[] graphs) {
            // The outermost filters are applied to the block's rows last: by the thread that consumes a walk's rows, or by
            // each thread that takes rows for an aggregate.
            List<ExprList> filters = new ArrayList<>();
            while (plan instanceof Filtered f) {
                filters.add(f.exprs());
                plan = f.sub();
            }
            // A term the store does not hold matches in no graph: say so once, not once a graph.
            boolean empty = false;
            if (plan instanceof Bgp bgp) {
                for (Triple t : bgp.triples()) {
                    if ((t.getSubject().isConcrete() && dict.getSubjects().locate(t.getSubject()) < 1)
                            || (t.getPredicate().isConcrete() && dict.getPredicates().locate(t.getPredicate()) < 1)
                            || (t.getObject().isConcrete() && dict.getObjects().locate(t.getObject()) < 1)) {
                        empty = true;
                    }
                }
            }
            return new Prepared(graphVar, graphTerm, plan, filters, empty, dsg, root, reader, dict, graphs);
        }

        /**
         * A reader of the block's rows for one thread, every filter applied,
         * one graph or one part of a graph at a time ({@link GraphLoop#graph},
         * {@link GraphLoop#part}).
         *
         * @param own whether the thread is to have filter expressions of its
         *            own: one of several that read at once needs them, and
         *            any thread reads values faster through them
         */
        GraphLoop loop(BindingNodeId base, ExecutionContext execCxt, boolean own) {
            Plan mine = own ? GraphVarBG.own(plan, nodeTable) : plan;
            List<ExprList> outer = filters;
            if (own && !filters.isEmpty()) {
                outer = new ArrayList<>(filters.size());
                for (ExprList exprs : filters) {
                    outer.add(RowValue.own(exprs, nodeTable));
                }
            }
            return new GraphLoop(base, graphs, 0, 0, 0, null, execCxt, mine, outer);
        }

        /**
         * The rows of graph {@code graphs[index]} in parts that can be read at
         * once, where the block's first pattern is a scan of at least
         * {@code threshold} positions there ({@link ScanChunks}); null where
         * it is not. Each part is the first pattern's rows over a range of
         * positions: {@link GraphLoop#part} makes the block's rows of them.
         */
        List<Supplier<Iterator<BindingNodeId>>> split(int index, BindingNodeId base, long threshold) {
            if (threshold <= 0 || empty) return null;
            Bgp first = leftmost(plan);
            if (first == null || first.triples().length == 0) return null;
            Triple t = first.triples()[0];
            if (!t.getSubject().isVariable() || !t.getObject().isVariable()) return null;
            long gid = graphs[index];
            if (graphVar == null) {
                return ScanChunks.chunks(reader, dict, gid, graphTerm, base, t, first.hints(), threshold);
            }
            BindingNodeId b0 = new BindingNodeId(base);
            b0.bind(graphVar, NodeId.pack(NodeType.GRAPH, gid));
            return ScanChunks.chunks(reader, dict, gid, graphVar, b0, t, first.hints(), threshold);
        }

        /**
         * What {@code expr.isSatisfied} is given for the rows of one graph: the
         * execution context with that graph as the active one, as Jena has it
         * when it runs the block there. Made for a graph when a row of it first
         * comes to a filter, and kept while the rows are that graph's. A block
         * in one given graph is handed the context of that graph to begin with.
         */
        final class GraphContext {
            private long gid = -1;
            private ExecutionContext cxt;

            ExecutionContext of(long graph, ExecutionContext execCxt) {
                if (graphVar == null) {
                    return execCxt;
                }
                if (graph != gid) {
                    Node name = nodeTable.getNodeForNodeId(NodeId.pack(NodeType.GRAPH, graph));
                    cxt = (name == null) ? execCxt : ExecutionContext.copyChangeActiveGraph(execCxt, dsg.getGraph(name));
                    gid = graph;
                }
                return cxt;
            }

            boolean accepts(ExprList exprs, BindingNodeId row, long graph, ExecutionContext execCxt) {
                Binding binding = SolverLibBeak.convToBinding(row, root);
                ExecutionContext env = of(graph, execCxt);
                for (Expr e : exprs) {
                    if (!e.isSatisfied(binding, env)) {
                        return false;
                    }
                }
                return true;
            }
        }

        /**
         * The plan's rows for the graphs {@code ids[from..to)}, graph by
         * graph, each opened when the one before it is done. One thread's: the
         * walk's own for its sequential part, a worker's for its share, or -
         * visiting no graph by itself - the reader an aggregate asks for one
         * graph's rows at a time.
         */
        final class GraphLoop implements IteratorCloseable<BindingNodeId> {
            private final BindingNodeId base;
            private final long[] ids;
            private final long scanThreshold;       // from how many positions a single graph is scanned in parallel; 0 for never
            private final List<Abortable> killList;
            private final ExecutionContext execCxt;
            private final Plan plan;                // the block as this thread evaluates it
            private final List<ExprList> outer;     // the outermost filters, where this thread applies them; null where its consumer does
            private final GraphContext context = new GraphContext();
            private final List<ParallelScan> scans = new ArrayList<>(2); // the parallel scans under the current graph's rows
            private final int to;
            private int next;
            private Iterator<BindingNodeId> current;
            private long effort;                    // rows this loop has put to a later pattern or to a filter

            GraphLoop(BindingNodeId base, long[] ids, int from, int to, long scanThreshold, List<Abortable> killList, ExecutionContext execCxt,
                    Plan plan, List<ExprList> outer) {
                this.base = base;
                this.ids = ids;
                this.next = from;
                this.to = to;
                this.scanThreshold = scanThreshold;
                this.killList = killList;
                this.execCxt = execCxt;
                this.plan = plan;
                this.outer = outer;
            }

            /** How far the loop has come: the index of the next graph it would open. */
            int position() {
                return next;
            }

            /**
             * What the rows read so far have cost beyond being read: one for
             * every row handed to a later pattern, to an optional or to a
             * filter. A join that finds little and a filter that passes
             * little have done work that their rows do not show.
             */
            long effort() {
                return effort;
            }

            /**
             * Counts a row put to a later pattern or to a filter, and now and
             * then asks whether the query has been cancelled: a block can
             * read for a long time without giving its consumer a row to
             * notice that at.
             */
            private void spent() {
                if ((++effort & 0x3FF) == 0) {
                    AtomicBoolean cancel = execCxt.getCancelSignal();
                    if (cancel != null && cancel.get()) {
                        throw new QueryCancelledException();
                    }
                }
            }

            /** True while the graph being read has a row left; closes it when it has none. */
            boolean currentHasNext() {
                if (current == null) {
                    return false;
                }
                if (current.hasNext()) {
                    return true;
                }
                closeCurrent();
                return false;
            }

            /** Opens the next graph; false when there is none. */
            boolean openNext() {
                if (next >= to) {
                    return false;
                }
                current = of(ids[next++], scanThreshold > 0, null);
                return true;
            }

            /** Every row of the block in graph {@code ids[index]}. */
            Iterator<BindingNodeId> graph(int index) {
                return of(ids[index], false, null);
            }

            /** The block's rows in graph {@code ids[index]} that come of one part of its first pattern's rows ({@link Prepared#split}). */
            Iterator<BindingNodeId> part(int index, Supplier<Iterator<BindingNodeId>> first) {
                return of(ids[index], false, first);
            }

            private Iterator<BindingNodeId> of(long gid, boolean scan, Supplier<Iterator<BindingNodeId>> first) {
                BindingNodeId child = base;
                if (graphVar != null) {
                    child = new BindingNodeId(base);
                    child.bind(graphVar, NodeId.pack(NodeType.GRAPH, gid)); // the input binding does not hold it: the caller looked
                }
                Iterator<BindingNodeId> out = rows(plan, child, gid, scan, first);
                if (outer != null) {
                    for (ExprList exprs : outer) {
                        out = Iter.filter(out, row -> {
                            spent();
                            return context.accepts(exprs, row, gid, execCxt);
                        });
                    }
                }
                return out;
            }

            void closeCurrent() {
                if (current != null) {
                    Iter.close(current);
                    current = null;
                }
                if (!scans.isEmpty()) {
                    killList.removeAll(scans);
                    scans.clear();
                }
            }

            @Override
            public boolean hasNext() {
                while (!currentHasNext()) {
                    if (!openNext()) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public BindingNodeId next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }

            @Override
            public void close() {
                next = to;
                closeCurrent();
            }

            /**
             * The rows of {@code node} that extend {@code input}, in graph {@code gid}.
             *
             * @param scan  whether {@code input} is the walk's own binding for the graph, so that a first pattern that
             *              is a scan of a large graph may be planned in parallel
             * @param first the rows of the block's first pattern to start from, in place of reading it: one part of
             *              them, for a caller that reads the parts at once; null to read the pattern
             */
            private Iterator<BindingNodeId> rows(Plan node, BindingNodeId input, long gid, boolean scan, Supplier<Iterator<BindingNodeId>> first) {
                switch (node) {
                    case Bgp bgp -> {
                        Triple[] triples = bgp.triples();
                        if (triples.length == 0) {
                            return Iter.singletonIterator(input);
                        }
                        ExprList hints = bgp.hints();
                        Iterator<BindingNodeId> out = (first == null) ? null : first.get();
                        if (out == null && scan && triples[0].getSubject().isVariable() && triples[0].getObject().isVariable()) {
                            ParallelScan parallel = ScanChunks.tryParallel(reader, dict, gid, graphVar, input, triples[0], hints, scanThreshold, execCxt);
                            if (parallel != null) {
                                killList.add(parallel);
                                scans.add(parallel);
                                out = parallel;
                            }
                        }
                        if (out == null) {
                            out = reader.read(gid, input, triples[0], hints, nodeTable);
                        }
                        for (int k = 1; k < triples.length; k++) {
                            Triple t = triples[k];
                            out = Iter.flatMap(out, row -> {
                                spent();
                                return reader.read(gid, row, t, hints, nodeTable);
                            });
                        }
                        return out;
                    }
                    case Filtered filtered -> {
                        ExprList exprs = filtered.exprs();
                        return Iter.filter(rows(filtered.sub(), input, gid, scan, first), row -> {
                            spent();
                            return context.accepts(exprs, row, gid, execCxt);
                        });
                    }
                    case Steps steps -> {
                        Iterator<BindingNodeId> out = rows(steps.steps()[0], input, gid, scan, first);
                        for (int k = 1; k < steps.steps().length; k++) {
                            Plan step = steps.steps()[k];
                            out = Iter.flatMap(out, row -> {
                                spent();
                                return rows(step, row, gid, false, null);
                            });
                        }
                        return out;
                    }
                    case Optional optional -> {
                        Plan right = optional.right();
                        return Iter.flatMap(rows(optional.left(), input, gid, scan, first), row -> {
                            spent();
                            Iterator<BindingNodeId> extended = rows(right, row, gid, false, null);
                            return extended.hasNext() ? extended : Iter.singletonIterator(row);
                        });
                    }
                    case Either either -> {
                        // A side is opened when the one before it is done, not before. (No part of a first pattern
                        // comes here: a plan that begins with sides has none.)
                        return Iter.flatMap(List.of(either.sides()).iterator(), side -> rows(side, input, gid, scan, null));
                    }
                    case Assigned assigned -> {
                        Var[] vars = assigned.vars();
                        long[] ids = assigned.ids();
                        return Iter.removeNulls(Iter.map(rows(assigned.sub(), input, gid, scan, first), row -> {
                            BindingNodeId extended = new BindingNodeId(row);
                            for (int i = 0; i < vars.length; i++) {
                                if (!extended.putCompatible(vars[i], ids[i], nodeTable)) {
                                    return null;
                                }
                            }
                            return extended;
                        }));
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ the walk

    /** One input binding at a time: Jena's operator where it holds the graph variable, the walk over the graphs where it does not. */
    private static final class Stage extends QueryIterRepeatApply {
        private final UnaryOperator<QueryIterator> jena;
        private final Prepared p;
        private final boolean workers;      // whether the plan is for workers: no filter inside it, and rows that do work

        Stage(QueryIterator input, ExecutionContext execCxt, UnaryOperator<QueryIterator> jena, Prepared prepared) {
            super(input, execCxt);
            this.jena = jena;
            this.p = prepared;
            this.workers = !hasFilter(prepared.plan) && rowsDoWork(prepared.plan);
        }

        @Override
        protected QueryIterator nextStage(Binding binding) {
            ExecutionContext execCxt = getExecContext();
            if (binding.contains(p.graphVar)) {
                return jena.apply(QueryIterSingleton.create(binding, execCxt));
            }
            BindingNodeId base = SolverLibBeak.convert(binding, p.root);
            // Parallelism is for the root binding only, as PatternMatchBG.solveFirst has it: a binding that carries
            // variables is an outer row, and its block runs once a row. Nor for a store read through a channel.
            long threshold = (PatternMatchBG.isRootLike(base) && !p.reader.isChannelBacked()) ? ScanChunks.threshold() : 0;
            // Scans are added while the walk runs, and a cancel reads the list from another thread.
            List<Abortable> killList = new CopyOnWriteArrayList<>();
            Iterator<BindingNodeId> rows = new Walk(base, threshold, killList, execCxt);
            rows = makeAbortable(rows, killList, execCxt.getCancelSignal());
            return new QueryIterAbortable(SolverLibBeak.convertToNodes(rows, p.root), killList, null, execCxt);
        }

        /**
         * The walk as its consumer sees it: the graphs one after another on
         * this thread, until it has produced enough rows for workers to be
         * worth starting on the graphs that are left; and the outermost
         * filters applied to whatever comes, here.
         */
        private final class Walk implements IteratorCloseable<BindingNodeId> {
            private final BindingNodeId base;
            private final long threshold;
            private final List<Abortable> killList;
            private final ExecutionContext execCxt;
            private final Prepared.GraphLoop loop;
            private final Prepared.GraphContext context = p.new GraphContext();
            private ParallelScan parallel;  // the graphs left, once they are the workers'
            private long produced;
            private boolean closed;
            private BindingNodeId pending;

            Walk(BindingNodeId base, long threshold, List<Abortable> killList, ExecutionContext execCxt) {
                this.base = base;
                this.threshold = threshold;
                this.killList = killList;
                this.execCxt = execCxt;
                this.loop = p.new GraphLoop(base, p.graphs, 0, p.graphs.length, threshold, killList, execCxt, p.plan, null);
            }

            /** The next row of the plan, before the outermost filters; null at the end. */
            private BindingNodeId raw() {
                for (;;) {
                    if (parallel != null) {
                        return parallel.hasNext() ? parallel.next() : null;
                    }
                    if (loop.currentHasNext()) {
                        produced++;
                        return loop.next();
                    }
                    // Between two graphs: the place to hand the rest to workers.
                    if (closed) {
                        return null;
                    }
                    int at = loop.position();
                    if (workers && threshold > 0 && produced >= threshold && p.graphs.length - at >= 2) {
                        loop.close();
                        parallel = shareOut(at);
                        killList.add(parallel);
                        continue;
                    }
                    if (!loop.openNext()) {
                        return null;
                    }
                }
            }

            /** The graphs from {@code from} on, in as many contiguous shares as there are workers to take them. */
            private ParallelScan shareOut(int from) {
                int left = p.graphs.length - from;
                int shares = (int) Math.min(4L * Runtime.getRuntime().availableProcessors(), left);
                int each = (left + shares - 1) / shares;
                List<Supplier<Iterator<BindingNodeId>>> chunks = new ArrayList<>(shares);
                for (int lo = from; lo < p.graphs.length; lo += each) {
                    int start = lo;
                    int end = Math.min(p.graphs.length, lo + each);
                    // A worker plans no scan of its own, and reads the list it is given through no filter: nothing of it is shared.
                    chunks.add(() -> p.new GraphLoop(base, p.graphs, start, end, 0, killList, execCxt, p.plan, null));
                }
                PARALLEL.incrementAndGet();
                return new ParallelScan(chunks, execCxt.getCancelSignal());
            }

            private void advance() {
                while (pending == null) {
                    BindingNodeId row = raw();
                    if (row == null) {
                        return;
                    }
                    if (p.filters.isEmpty() || passes(row)) {
                        pending = row;
                    }
                }
            }

            private boolean passes(BindingNodeId row) {
                long gid = NodeId.id(row.get(p.graphVar));
                for (ExprList exprs : p.filters) {
                    if (!context.accepts(exprs, row, gid, execCxt)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                advance();
                return pending != null;
            }

            @Override
            public BindingNodeId next() {
                advance();
                if (pending == null) {
                    throw new NoSuchElementException();
                }
                BindingNodeId row = pending;
                pending = null;
                return row;
            }

            @Override
            public void close() {
                closed = true;
                loop.close();
                if (parallel != null) {
                    parallel.close();
                    killList.remove(parallel);
                }
            }
        }
    }
}
