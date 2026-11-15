package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.jena.GraphVarBG.Prepared;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorDelayedInitialization;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.expr.aggregate.AggAvg;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;
import org.apache.jena.sparql.expr.aggregate.AggMax;
import org.apache.jena.sparql.expr.aggregate.AggMin;
import org.apache.jena.sparql.expr.aggregate.AggSum;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;
import org.apache.jena.sparql.function.FunctionEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code GROUP BY} and the aggregates on ids, in the threads that read the
 * rows.
 *
 * <p>Jena groups by taking every row of the pattern as a binding of terms on
 * the one thread that asked: it makes a key binding of the row, looks the
 * group up by it, and hands the row to each aggregate. However many threads
 * read the rows, that thread sets the pace - which is why handing the rows of
 * a single pattern to workers gained nothing (PERFORMANCE.md) - and every row
 * costs its terms. Here a row stays the ids it was read as:
 * <ul>
 *   <li><b>The group</b> is found by the ids of the key variables, in a table
 *       of longs.</li>
 *   <li><b>The counts</b> - {@code COUNT(*)}, {@code COUNT(?v)},
 *       {@code COUNT(DISTINCT ?v)} - are taken from the ids: a row, a
 *       variable that has one, a set of them. No term is read.</li>
 *   <li><b>{@code SUM}, {@code AVG}, {@code MIN}, {@code MAX}</b> (and
 *       {@code COUNT} of an expression) are given the row by Jena's own
 *       accumulators, so a value is added, compared and found in error by
 *       exactly the code that does so in Jena's group.</li>
 *   <li><b>Workers</b> each keep groups of their own for the rows they read,
 *       and the groups are put together when the workers are done: counts
 *       added, sets united, and the accumulators' results combined by the
 *       operation that made them - Jena's addition for a sum, its ordering
 *       for a minimum or a maximum, the sum over the count for an average.
 *       The thread that asked reads rows itself only until reading has
 *       cost enough for workers to be worth starting
 *       ({@code beakgraph.group.handoff}); a query that reads little never
 *       starts one, and {@code beakgraph.scan.parallel.threshold=0} starts
 *       none for any.</li>
 * </ul>
 *
 * <p>What it takes: an {@code OpGroup} over the single root binding, with
 * plain variables for keys and the aggregates above, whose pattern is a block
 * {@link GraphVarBG} runs - patterns under filters, optionals and unions -
 * under {@code GRAPH ?g}, under {@code GRAPH <iri>} or in the active graph,
 * or a union of such. The rows are read as the walk reads them
 * ({@link Prepared}): the named graphs divided among workers, and a graph
 * whose first pattern is a large scan divided by position - a predicate's
 * scan by its rows where its objects are too few ({@link ScanChunks}). Each
 * thread has copies of the filters and of the aggregates' expressions, since
 * an expression may keep state and Jena evaluates one on one thread at a
 * time; the copies read a variable's value by the row's id
 * ({@link RowValue}), which keeps the threads out of the one cache Jena
 * makes values through. Everything else is Jena's group over
 * whatever executes the pattern, as before: a key that is an expression,
 * {@code SAMPLE}, {@code GROUP_CONCAT}, {@code DISTINCT} inside any aggregate
 * but a count of a variable, a pattern the walk does not run, the union graph
 * or a {@code FROM} list (whose rows are a union without duplicates), and
 * the counts {@link AggregateCountFastPath} reads off the index, which come
 * first.
 *
 * <p>The answers are Jena's, with one thing to know: a floating-point sum
 * depends on the order of its additions, SPARQL leaves that order open, and
 * workers add in another order than one thread does. A {@code SUM} or
 * {@code AVG} over {@code xsd:double} or {@code xsd:float} values can
 * therefore differ in its last digits from the sum one thread makes - as it
 * already could wherever a parallel scan delivered the rows. Integer and
 * decimal sums are exact in any order. The work is divided by counting, not
 * by timing, and what each worker found is combined in the order of the
 * work, not in the order the workers finish, so a query gives the same
 * answer each time it is run - but for a floating-point sum in a query with
 * more groups than a worker keeps to itself
 * ({@code beakgraph.group.flush}), which workers hand on as they go.
 */
public final class GroupOnIds {

    private static final Logger logger = LoggerFactory.getLogger(GroupOnIds.class);

    /** Groups answered here; tests pin which shapes are, and which fall back. */
    public static final AtomicLong HITS = new AtomicLong();

    /** Times rows were handed to workers to be grouped where they are read; observability for tests and diagnostics. */
    public static final AtomicLong PARALLEL = new AtomicLong();

    /** {@code -Dbeakgraph.group.fastpath=false} leaves every group to Jena's operator. */
    private static volatile boolean enabled = Boolean.parseBoolean(System.getProperty("beakgraph.group.fastpath", "true"));

    private static final long TICK_MS = 25;
    private static final long GOLDEN = 0x9E3779B97F4A7C15L;

    private GroupOnIds() {}

    /** Switches this path on or off (tests compare the two ways); returns the previous setting. */
    public static boolean setEnabled(boolean on) {
        boolean previous = enabled;
        enabled = on;
        return previous;
    }

    // ------------------------------------------------------------------ what is asked

    private enum Kind {
        /** {@code COUNT(*)}: the rows of the group. */
        ROWS,
        /** {@code COUNT(?v)}: the rows in which the variable has an id. */
        BOUND,
        /** {@code COUNT(DISTINCT ?v)}: the ids the variable has. */
        DISTINCT,
        /** {@code COUNT(expr)}: Jena's accumulator; counts are added. */
        COUNTED,
        /** {@code SUM(expr)}: Jena's accumulator; sums are added. */
        SUM,
        /** {@code AVG(expr)}: Jena's accumulator of the sum; sums are added, and divided by the rows at the end. */
        AVG,
        /** {@code MIN(expr)}: Jena's accumulator; the least of the results by Jena's ordering. */
        MIN,
        /** {@code MAX(expr)}: Jena's accumulator; the greatest. */
        MAX
    }

    /**
     * One aggregate of the query.
     *
     * @param out   the variable its result is bound to
     * @param var   the variable counted, for {@link Kind#BOUND} and {@link Kind#DISTINCT}
     * @param rows  the aggregator that takes the rows, for the kinds Jena accumulates
     * @param asked the query's own aggregator, which says what a query without rows answers
     * @param slot  where a group keeps this aggregate's count or state
     */
    private record Spec(Kind kind, Var out, Var var, Aggregator rows, Aggregator asked, int slot) {

        /**
         * This aggregate with expressions of its own, for one thread: no other
         * evaluates them, and they read their variables by the rows' ids
         * ({@link RowValue}).
         */
        Spec own(NodeTable nodeTable) {
            if (rows == null) return this;
            ExprList exprs = rows.getExprList();
            return (exprs == null || exprs.isEmpty()) ? this : new Spec(kind, out, var, rows.copy(RowValue.own(exprs, nodeTable)), asked, slot);
        }
    }

    /** The query's aggregates, each given its place in a group; null when one of them is not answered here. */
    private static Spec[] specs(List<ExprAggregator> aggregators) {
        Spec[] specs = new Spec[aggregators.size()];
        int counts = 0;
        int states = 0;
        for (int i = 0; i < specs.length; i++) {
            ExprAggregator ea = aggregators.get(i);
            Aggregator agg = ea.getAggregator();
            Var out = ea.getVar();
            // By the class itself: a subclass, or the DISTINCT form of an aggregate, is another aggregate.
            Class<?> type = agg.getClass();
            if (type == AggCount.class) {
                specs[i] = new Spec(Kind.ROWS, out, null, null, agg, -1);
                continue;
            }
            Expr expr = singleExpr(agg);
            if (expr == null || GraphVarBG.holdsPattern(expr)) return null; // EXISTS runs a pattern of its own for every row
            if (type == AggCountVar.class) {
                specs[i] = expr.isVariable() ? new Spec(Kind.BOUND, out, expr.asVar(), null, agg, counts++)
                                             : new Spec(Kind.COUNTED, out, null, agg, agg, states++);
            } else if (type == AggCountVarDistinct.class) {
                if (!expr.isVariable()) return null; // the distinct values of an expression are terms, not ids
                specs[i] = new Spec(Kind.DISTINCT, out, expr.asVar(), null, agg, states++);
            } else if (type == AggSum.class) {
                specs[i] = new Spec(Kind.SUM, out, null, agg, agg, states++);
            } else if (type == AggAvg.class) {
                specs[i] = new Spec(Kind.AVG, out, null, new AggSum(expr), agg, states++);
            } else if (type == AggMin.class) {
                specs[i] = new Spec(Kind.MIN, out, null, agg, agg, states++);
            } else if (type == AggMax.class) {
                specs[i] = new Spec(Kind.MAX, out, null, agg, agg, states++);
            } else {
                return null;
            }
        }
        return specs;
    }

    /** The aggregator's one argument, or null. */
    private static Expr singleExpr(Aggregator agg) {
        ExprList exprs = agg.getExprList();
        return (exprs == null || exprs.size() != 1) ? null : exprs.get(0);
    }

    /** A block and the context its filters are evaluated in. */
    private record Source(Prepared block, ExecutionContext env) {}

    /**
     * The blocks under the group: the sides of a union one after another, a
     * block under a graph variable, under a graph's name, or in the active
     * graph. False when any of it is not a block the walk runs.
     */
    private static boolean collect(Op op, BeakGraph active, Binding first, ExecutionContext execCxt, List<Source> out) {
        if (op instanceof OpUnion union) {
            return collect(union.getLeft(), active, first, execCxt, out) && collect(union.getRight(), active, first, execCxt, out);
        }
        if (op instanceof OpGraph opGraph) {
            Node g = opGraph.getNode();
            if (g.isVariable()) {
                Prepared block = GraphVarBG.prepareWalk(opGraph, first, execCxt);
                return block != null && out.add(new Source(block, execCxt));
            }
            // A graph by its name. The synthetic names have rules of their own, which are Jena's; so has a dataset
            // that is not the store's (FROM NAMED).
            if (!g.isURI() || Quad.isDefaultGraph(g) || Quad.isUnionGraph(g)) return false;
            if (!(execCxt.getDataset() instanceof BGDatasetGraph dsg) || dsg.getBeakGraph().getReader() != active.getReader()) return false;
            if (!(dsg.getGraph(g) instanceof BeakGraph view)) return false;
            Prepared block = GraphVarBG.prepareBlock(opGraph.getSubOp(), view, first);
            return block != null && out.add(new Source(block, ExecutionContext.copyChangeActiveGraph(execCxt, view)));
        }
        Prepared block = GraphVarBG.prepareBlock(op, active, first);
        return block != null && out.add(new Source(block, execCxt));
    }

    // ------------------------------------------------------------------ the entry point

    /**
     * The group computed on ids, or null - with {@code input} untouched -
     * when it is not a shape answered here. The caller guarantees
     * {@code input} wraps the single-binding root iterator.
     */
    static QueryIterator tryExecute(OpGroup opGroup, QueryIterPeek input, ExecutionContext execCxt) {
        if (!enabled) return null;
        if (!(execCxt.getActiveGraph() instanceof BeakGraph active)) return null;
        VarExprList keys = opGroup.getGroupVars();
        if (!keys.getExprs().isEmpty()) return null; // GROUP BY (expr AS ?v)
        Spec[] specs = specs(opGroup.getAggregators());
        if (specs == null) return null;
        Binding root = input.peek();
        if (root == null || !root.isEmpty()) return null; // a binding that carries variables is an outer row
        List<Source> sources = new ArrayList<>();
        if (!collect(opGroup.getSubOp(), active, root, execCxt, sources)) return null;
        HDF5Reader reader = sources.get(0).block().reader;
        for (Source source : sources) {
            if (source.block().reader != reader) return null;
        }
        input.next(); // consume the single root binding
        HITS.incrementAndGet();
        logger.debug("Group on ids: keys={}, aggregates={}, blocks={}", keys.getVars(), opGroup.getAggregators(), sources.size());
        for (Source source : sources) {
            PatternMatchBG.HITS.incrementAndGet();
            if (source.block().graphVar != null) {
                GraphVarBG.HITS.incrementAndGet();
            }
        }
        Run run = new Run(keys.getVars().toArray(Var[]::new), specs, sources, root, execCxt);
        return new Result(run, input, execCxt);
    }

    /** The group's rows, worked out when the first of them is asked for; as Jena's QueryIterGroup is. */
    private static final class Result extends QueryIterPlainWrapper {
        private final Run run;
        private final QueryIterator input;

        Result(Run run, QueryIterator input, ExecutionContext execCxt) {
            super(new IteratorDelayedInitialization<Binding>() {
                @Override
                protected Iterator<Binding> initializeIterator() {
                    return run.rows().iterator();
                }
            }, execCxt);
            this.run = run;
            this.input = input;
        }

        @Override
        protected void requestCancel() {
            run.stop.set(true);
            super.requestCancel();
        }

        @Override
        protected void closeIterator() {
            run.stop.set(true);
            input.close();
            super.closeIterator();
        }
    }

    // ------------------------------------------------------------------ a group, and the groups of one thread

    /** A set of ids: longs that are never 0. */
    private static final class LongSet {
        private long[] slots = new long[16];
        private int size;

        void add(long id) {
            int mask = slots.length - 1;
            int i = (int) ((id * GOLDEN) >>> 32) & mask;
            for (;;) {
                long held = slots[i];
                if (held == id) return;
                if (held == 0) break;
                i = (i + 1) & mask;
            }
            slots[i] = id;
            if (++size * 2 > slots.length) {
                long[] old = slots;
                slots = new long[old.length * 2];
                size = 0;
                for (long x : old) {
                    if (x != 0) add(x);
                }
            }
        }

        void addAll(LongSet other) {
            for (long x : other.slots) {
                if (x != 0) add(x);
            }
        }

        int size() {
            return size;
        }
    }

    /**
     * What one of Jena's accumulators has taken in one group, and what has
     * come of the accumulators that took the group's other rows elsewhere.
     */
    private static final class Value {
        private Accumulator rows;   // takes this thread's rows; null when there have been none since the last result was drawn
        private NodeValue result;   // of the rows taken before, here or on other threads; null for none yet
        private boolean error;      // some accumulator found an error: the aggregate has no value

        void take(Spec spec, Binding binding, FunctionEnv env) {
            if (rows == null) {
                rows = spec.rows().createAccumulator();
            }
            rows.accumulate(binding, env);
        }

        /** Draws the accumulator's result into {@link #result}. */
        private void settle(Kind kind) {
            if (rows != null) {
                NodeValue value = rows.getValue();
                rows = null;
                if (value == null) {
                    error = true; // an accumulator that has taken a row has no value only when it found an error
                } else {
                    combine(kind, value);
                }
            }
        }

        private void combine(Kind kind, NodeValue value) {
            if (result == null) {
                result = value;
                return;
            }
            result = switch (kind) {
                case COUNTED, SUM, AVG -> XSDFuncOp.numAdd(value, result);
                case MIN -> (NodeValue.compareAlways(result, value) > 0) ? value : result;
                case MAX -> (NodeValue.compareAlways(result, value) < 0) ? value : result;
                default -> throw new IllegalStateException(kind.toString());
            };
        }

        void merge(Kind kind, Value other) {
            settle(kind);
            other.settle(kind);
            error |= other.error;
            if (other.result != null) {
                combine(kind, other.result);
            }
        }

        /** The aggregate's value for a group of {@code rows} rows, or null where Jena's accumulator would have none. */
        NodeValue value(Kind kind, long rows) {
            settle(kind);
            if (error || result == null) {
                return null;
            }
            // As AggAvg: the sum over the count of the numbers, and with no error every row was one.
            return (kind == Kind.AVG) ? XSDFuncOp.numDivide(result, NodeValue.makeInteger(rows)) : result;
        }
    }

    private static final class Group {
        long rows;
        long[] counts;      // one a COUNT(?v)
        Object[] states;    // one a COUNT(DISTINCT ?v) - a LongSet - or an aggregate Jena accumulates - a Value

        Group(int counts, int states) {
            this.counts = (counts == 0) ? null : new long[counts];
            this.states = (states == 0) ? null : new Object[states];
        }

        /** Takes what {@code other} holds; {@code other} is not used again. */
        void merge(Group other, Spec[] specs) {
            rows += other.rows;
            for (Spec spec : specs) {
                switch (spec.kind()) {
                    case ROWS -> { }
                    case BOUND -> counts[spec.slot()] += other.counts[spec.slot()];
                    case DISTINCT -> {
                        LongSet theirs = (LongSet) other.states[spec.slot()];
                        LongSet mine = (LongSet) states[spec.slot()];
                        if (mine == null) {
                            states[spec.slot()] = theirs;
                        } else if (theirs != null) {
                            mine.addAll(theirs);
                        }
                    }
                    default -> {
                        Value theirs = (Value) other.states[spec.slot()];
                        Value mine = (Value) states[spec.slot()];
                        if (mine == null) {
                            states[spec.slot()] = theirs;
                        } else if (theirs != null) {
                            mine.merge(spec.kind(), theirs);
                        }
                    }
                }
            }
        }
    }

    /** Groups by their keys: {@code k} longs a group, in a table that is probed from where the keys hash to. */
    private static final class Table {
        private final int k;
        private long[] keys;
        private Group[] groups;
        private int size;

        Table(int k) {
            this.k = k;
            this.keys = new long[16 * k];
            this.groups = new Group[16];
        }

        private int hash(long[] key, int from) {
            long h = 0;
            for (int i = 0; i < k; i++) {
                h = (h ^ key[from + i]) * GOLDEN;
                h ^= h >>> 29;
            }
            return (int) (h >>> 32);
        }

        /** The slot of the group with these keys, or - as {@code -(slot) - 1} - the free slot where it belongs. */
        private int find(long[] key, int from) {
            int mask = groups.length - 1;
            int i = hash(key, from) & mask;
            for (;;) {
                if (groups[i] == null) {
                    return -i - 1;
                }
                boolean same = true;
                for (int j = 0; j < k && same; j++) {
                    same = keys[i * k + j] == key[from + j];
                }
                if (same) {
                    return i;
                }
                i = (i + 1) & mask;
            }
        }

        Group get(long[] key, int from) {
            int i = find(key, from);
            return (i < 0) ? null : groups[i];
        }

        /** Adds a group whose keys {@link #get} did not find. */
        void put(long[] key, int from, Group group) {
            if ((size + 1) * 2L > groups.length) {
                grow();
            }
            int i = -find(key, from) - 1;
            System.arraycopy(key, from, keys, i * k, k);
            groups[i] = group;
            size++;
        }

        private void grow() {
            long[] oldKeys = keys;
            Group[] oldGroups = groups;
            if ((long) oldGroups.length * 2 * k > Integer.MAX_VALUE - 8) {
                throw new OutOfMemoryError("more groups than a table of them holds: " + size);
            }
            keys = new long[oldKeys.length * 2];
            groups = new Group[oldGroups.length * 2];
            for (int i = 0; i < oldGroups.length; i++) {
                if (oldGroups[i] != null) {
                    int at = -find(oldKeys, i * k) - 1;
                    System.arraycopy(oldKeys, i * k, keys, at * k, k);
                    groups[at] = oldGroups[i];
                }
            }
        }
    }

    // ------------------------------------------------------------------ one execution

    /** A piece of the rows that a worker reads by itself, into groups of its own. */
    private interface Unit {
        void read(Prepared.GraphLoop reader, Run.Sink sink);
    }

    private static final class Run {
        private final Var[] keys;
        private final Spec[] specs;
        private final int counts;
        private final int states;
        private final boolean valued;       // whether some aggregate takes more of a row than that it is one
        private final List<Source> sources;
        private final Binding root;
        private final ExecutionContext execCxt;
        private final AtomicBoolean cancelSignal;
        private final BeakGraph graph;      // whose reader the ids are ranks of
        private final NodeTable nodeTable;
        private final PositionalDictionaryReader dict;
        private final AtomicLongArray predicateKeys;   // the key of each predicate id, once worked out; null where there are too many to table
        private final long threshold;
        private final long handOff;
        private final int flushAt;
        final AtomicBoolean stop = new AtomicBoolean();
        private final Sink all;
        private long taken;                 // rows grouped so far, by this thread and by the workers it has waited for
        private long effort;                // what reading has cost so far: see read(Source)
        private long walked;                // the part of it that was graphs read whole by this thread,
        private long visited;               // and how many graphs those were

        Run(Var[] keys, Spec[] specs, List<Source> sources, Binding root, ExecutionContext execCxt) {
            this.keys = keys;
            this.specs = specs;
            int counts = 0;
            int states = 0;
            for (Spec spec : specs) {
                if (spec.kind() == Kind.BOUND) counts++;
                else if (spec.kind() != Kind.ROWS) states++;
            }
            this.counts = counts;
            this.states = states;
            this.valued = counts + states > 0;
            this.sources = sources;
            this.root = root;
            this.execCxt = execCxt;
            this.cancelSignal = execCxt.getCancelSignal();
            Prepared first = sources.get(0).block();
            this.graph = first.root;
            this.nodeTable = first.nodeTable;
            this.dict = first.dict;
            long predicates = dict.getPredicates().getNumberOfNodes();
            this.predicateKeys = (predicates < (1 << 24)) ? new AtomicLongArray((int) predicates + 1) : null;
            // As the walk: parallel work is not for a store read through a channel, which is one lock and one cache.
            this.threshold = first.reader.isChannelBacked() ? 0 : ScanChunks.threshold();
            this.handOff = Long.getLong("beakgraph.group.handoff", 4096L);
            this.flushAt = Math.max(1, Integer.getInteger("beakgraph.group.flush", 8192));
            this.all = new Sink(own());
        }

        /** The aggregates with expressions of their own, for one thread. */
        private Spec[] own() {
            Spec[] own = new Spec[specs.length];
            for (int i = 0; i < own.length; i++) {
                own[i] = specs[i].own(nodeTable);
            }
            return own;
        }

        // -------------------------------------------------------------- ids as keys

        /**
         * The id as a key: equal for equal terms, whatever position the id
         * was read in. Graphs, subjects and objects number the same terms
         * alike; a predicate has a number of its own, and is given the
         * entity's where the store also holds it as one, so that a variable
         * bound to a predicate in one branch of a union and to a subject in
         * another groups as the terms do.
         */
        private long key(long id) {
            if (id == NodeId.NONE) {
                return NodeId.NONE;
            }
            if (NodeId.isPredicateSpace(id)) {
                return predicateKey(NodeId.id(id));
            }
            if (NodeId.isSpecial(id)) {
                // Reached only if a row could hold a term the store does not: no row read here can.
                throw new IllegalStateException("a row holds an id that is no term's: " + NodeId.toString(id));
            }
            return NodeId.pack(NodeType.OBJECT, NodeId.id(id));
        }

        private long predicateKey(long pid) {
            if (predicateKeys != null && pid < predicateKeys.length()) {
                long known = predicateKeys.get((int) pid);
                if (known != 0) {
                    return known;
                }
            }
            long own = NodeId.pack(NodeType.PREDICATE, pid);
            Node term = nodeTable.getNodeForNodeId(own);
            long entity = (term == null) ? -1 : dict.getSubjects().locate(term);
            long key = (entity >= 1) ? NodeId.pack(NodeType.OBJECT, entity) : own;
            if (predicateKeys != null && pid < predicateKeys.length()) {
                predicateKeys.set((int) pid, key);
            }
            return key;
        }

        // -------------------------------------------------------------- the groups of one thread

        /** The groups one thread has made of the rows it read. */
        private final class Sink {
            private final Spec[] specs;             // this thread's: expressions no other thread evaluates
            private final long[] scratch = new long[keys.length];
            private Table table = (keys.length == 0) ? null : new Table(keys.length);
            private Group single;                   // the one group of a query without keys
            long taken;

            Sink(Spec[] specs) {
                this.specs = specs;
            }

            int groups() {
                return (table == null) ? 0 : table.size;
            }

            void take(BindingNodeId row) {
                taken++;
                Group group;
                if (table == null) {
                    group = single;
                    if (group == null) {
                        group = single = new Group(counts, states);
                    }
                } else {
                    for (int i = 0; i < keys.length; i++) {
                        scratch[i] = key(row.get(keys[i]));
                    }
                    group = table.get(scratch, 0);
                    if (group == null) {
                        group = new Group(counts, states);
                        table.put(scratch, 0, group);
                    }
                }
                group.rows++;
                if (!valued) {
                    return;
                }
                Binding binding = null;
                for (Spec spec : specs) {
                    switch (spec.kind()) {
                        case ROWS -> { }
                        case BOUND -> {
                            if (row.get(spec.var()) != NodeId.NONE) {
                                group.counts[spec.slot()]++;
                            }
                        }
                        case DISTINCT -> {
                            long id = row.get(spec.var());
                            if (id != NodeId.NONE) {
                                LongSet seen = (LongSet) group.states[spec.slot()];
                                if (seen == null) {
                                    group.states[spec.slot()] = seen = new LongSet();
                                }
                                seen.add(key(id));
                            }
                        }
                        default -> {
                            if (binding == null) {
                                binding = SolverLibBeak.convToBinding(row, graph);
                            }
                            Value value = (Value) group.states[spec.slot()];
                            if (value == null) {
                                group.states[spec.slot()] = value = new Value();
                            }
                            value.take(spec, binding, execCxt);
                        }
                    }
                }
            }

            /** Takes the groups of {@code other}, which is emptied or dropped afterwards. */
            void absorb(Sink other) {
                if (table == null) {
                    if (other.single != null) {
                        if (single == null) {
                            single = other.single;
                        } else {
                            single.merge(other.single, specs);
                        }
                    }
                    return;
                }
                Table theirs = other.table;
                for (int i = 0; i < theirs.groups.length; i++) {
                    Group group = theirs.groups[i];
                    if (group != null) {
                        Group mine = table.get(theirs.keys, i * theirs.k);
                        if (mine == null) {
                            table.put(theirs.keys, i * theirs.k, group);
                        } else {
                            mine.merge(group, specs);
                        }
                    }
                }
            }

            /** Forgets the groups (they have been absorbed elsewhere); the count of rows taken stays. */
            void clear() {
                single = null;
                if (table != null) {
                    table = new Table(keys.length);
                }
            }
        }

        // -------------------------------------------------------------- reading

        private boolean stopped() {
            return stop.get() || (cancelSignal != null && cancelSignal.get());
        }

        /** The rows into the groups; a worker gives up quietly when the query is stopped, and the thread that asked says so. */
        private void drain(Iterator<BindingNodeId> rows, Sink sink, boolean worker) {
            try {
                int n = 0;
                while (rows.hasNext()) {
                    sink.take(rows.next());
                    if ((++n & 63) == 0) {
                        if (stopped()) {
                            if (worker) return;
                            throw new QueryCancelledException();
                        }
                        // A worker that has made many groups hands them on, so that workers together hold little
                        // more than the groups there are.
                        if (worker && sink.groups() >= flushAt) {
                            synchronized (all) {
                                all.absorb(sink);
                            }
                            sink.clear();
                        }
                    }
                }
            } finally {
                Iter.close(rows);
            }
        }

        /** The answer: the rows of Jena's QueryIterGroup, group for group. */
        List<Binding> rows() {
            for (Source source : sources) {
                read(source);
            }
            if (stopped()) {
                throw new QueryCancelledException();
            }
            List<Binding> rows = new ArrayList<>();
            if (taken == 0) {
                // No row: with keys there is no group; without, one row of what each aggregate says of nothing.
                if (keys.length > 0) {
                    return rows;
                }
                BindingBuilder builder = Binding.builder();
                for (Spec spec : specs) {
                    Node value = spec.asked().getValueEmpty();
                    if (value != null) {
                        builder.add(spec.out(), value);
                    }
                }
                rows.add(specs.length == 0 ? BindingFactory.binding() : builder.build());
                return rows;
            }
            if (all.table == null) {
                rows.add(row(null, 0, all.single));
            } else {
                Table table = all.table;
                for (int i = 0; i < table.groups.length; i++) {
                    if (table.groups[i] != null) {
                        rows.add(row(table.keys, i * table.k, table.groups[i]));
                    }
                }
            }
            return rows;
        }

        private Binding row(long[] keyIds, int from, Group group) {
            BindingBuilder builder = Binding.builder();
            for (int i = 0; i < keys.length; i++) {
                long id = keyIds[from + i];
                Node term = (id == NodeId.NONE) ? null : nodeTable.getNodeForNodeId(id);
                if (term != null) {
                    builder.add(keys[i], term);
                }
            }
            for (Spec spec : specs) {
                NodeValue value = switch (spec.kind()) {
                    case ROWS -> NodeValue.makeInteger(group.rows);
                    case BOUND -> NodeValue.makeInteger(group.counts[spec.slot()]);
                    case DISTINCT -> {
                        LongSet seen = (LongSet) group.states[spec.slot()];
                        yield NodeValue.makeInteger(seen == null ? 0 : seen.size());
                    }
                    default -> {
                        Value held = (Value) group.states[spec.slot()];
                        yield (held == null) ? null : held.value(spec.kind(), group.rows);
                    }
                };
                if (value != null) {
                    builder.add(spec.out(), value.asNode());
                }
            }
            return builder.build();
        }

        /**
         * One block's rows: graph after graph on this thread, until reading
         * has cost enough for workers to be worth starting on the graphs that
         * are left; and a graph whose first pattern is a large scan read in
         * parts, by workers, whenever it is met.
         *
         * <p>The cost is counted, not timed, so that the same query divides
         * its work the same way each time: a row grouped is one, a row handed
         * to a later pattern, an optional or a filter is one, and a graph
         * visited is four (an index is asked for it even where it has no
         * row). The walk that hands its rows to a consumer waits for 65,536
         * of them; here no row is handed on, and workers are started once
         * reading has cost {@code beakgraph.group.handoff} (4096) - as many
         * of them as the graphs left would each have that much to do, going
         * by what a graph has cost so far, and none if that is not two. A
         * count of what one subject has in each of 5,000 graphs is three
         * workers' work, not a hundred's: started for every core, it took
         * half as long again as on one thread.
         */
        private void read(Source source) {
            Prepared block = source.block();
            if (block.empty) {
                return;
            }
            BindingNodeId base = SolverLibBeak.convert(root, block.root);
            Prepared.GraphLoop mine = null;
            int n = block.graphs.length;
            for (int i = 0; i < n; i++) {
                if (stopped()) {
                    throw new QueryCancelledException();
                }
                if (threshold > 0) {
                    if (effort >= handOff && n - i >= 2) {
                        long each = (visited == 0) ? handOff : Math.max(1, walked / visited);
                        long workers = Math.min(4L * Runtime.getRuntime().availableProcessors(), each * (n - i) / Math.max(1, handOff));
                        if (workers >= 2) {
                            share(source, base, i, (int) workers);
                            return;
                        }
                    }
                    List<Supplier<Iterator<BindingNodeId>>> parts = block.split(i, base, threshold);
                    if (parts != null && parts.size() >= 2) {
                        List<Unit> units = new ArrayList<>(parts.size());
                        addParts(units, i, parts);
                        PARALLEL.incrementAndGet();
                        work(source, base, units);
                        continue;
                    }
                }
                if (mine == null) {
                    mine = block.loop(base, source.env(), true);
                }
                long before = all.taken;
                long cost = mine.effort();
                drain(mine.graph(i), all, false);
                taken += all.taken - before;
                long spent = (all.taken - before) + (mine.effort() - cost) + 4;
                effort += spent;
                walked += spent;
                visited++;
            }
        }

        private void addParts(List<Unit> units, int index, List<Supplier<Iterator<BindingNodeId>>> parts) {
            for (Supplier<Iterator<BindingNodeId>> part : parts) {
                units.add((reader, sink) -> drain(reader.part(index, part), sink, true));
            }
        }

        /** A graph a worker found to be one to read in parts, and the parts. */
        private record Large(int index, List<Supplier<Iterator<BindingNodeId>>> parts) {}

        /**
         * The graphs from {@code from} on, in {@code workers} shares. A worker
         * that meets a graph whose first pattern is a large scan does not
         * read it but says so, and those graphs are read in parts, by
         * workers again, when the shares are done: whether a graph is large
         * costs an index a few questions, which five thousand graphs are
         * better asked by many threads than by this one before any starts.
         */
        private void share(Source source, BindingNodeId base, int from, int workers) {
            Prepared block = source.block();
            int n = block.graphs.length;
            int shares = Math.min(workers, n - from);
            int each = (n - from + shares - 1) / shares;
            List<Unit> units = new ArrayList<>(shares);
            ConcurrentLinkedQueue<Large> large = new ConcurrentLinkedQueue<>();
            for (int lo = from; lo < n; lo += each) {
                int start = lo;
                int end = Math.min(n, lo + each);
                units.add((reader, sink) -> {
                    for (int index = start; index < end; index++) {
                        if (stopped()) return;
                        List<Supplier<Iterator<BindingNodeId>>> parts = block.split(index, base, threshold);
                        if (parts != null && parts.size() >= 2) {
                            large.add(new Large(index, parts));
                        } else {
                            drain(reader.graph(index), sink, true);
                        }
                    }
                });
            }
            PARALLEL.incrementAndGet();
            work(source, base, units);
            if (!large.isEmpty()) {
                // In the order of the graphs, whatever order the workers met them in: the same parts each time.
                List<Large> found = new ArrayList<>(large);
                found.sort(Comparator.comparingInt(Large::index));
                List<Unit> parts = new ArrayList<>();
                for (Large graph : found) {
                    addParts(parts, graph.index(), graph.parts());
                }
                work(source, base, parts);
            }
        }

        /**
         * Runs the units on workers and waits for them: each with a reader
         * and groups of its own, put together here in the order of the units
         * when all are done. A worker's failure stops the others and is
         * thrown here; so is a cancel.
         */
        private void work(Source source, BindingNodeId base, List<Unit> units) {
            int n = units.size();
            Sink[] sinks = new Sink[n];
            CountDownLatch done = new CountDownLatch(n);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int u = 0; u < n; u++) {
                int index = u;
                Unit unit = units.get(u);
                ParallelScan.pool().execute(() -> {
                    ParallelScan.ACTIVE_WORKERS.incrementAndGet();
                    try {
                        if (stopped()) return;
                        Sink sink = new Sink(own());
                        sinks[index] = sink;
                        unit.read(source.block().loop(base, source.env(), true), sink);
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                        stop.set(true);
                    } finally {
                        ParallelScan.ACTIVE_WORKERS.decrementAndGet();
                        done.countDown();
                    }
                });
            }
            boolean interrupted = false;
            for (;;) {
                try {
                    if (done.await(TICK_MS, TimeUnit.MILLISECONDS)) break;
                } catch (InterruptedException e) {
                    interrupted = true;
                    stop.set(true); // the workers see it within a few rows, and are waited for: they read a store that may be closed after
                }
                if (cancelSignal != null && cancelSignal.get()) {
                    stop.set(true);
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            Throwable t = failure.get();
            if (t != null) {
                if (t instanceof RuntimeException r) throw r;
                if (t instanceof Error e) throw e;
                throw new RuntimeException(t);
            }
            if (stopped()) {
                throw new QueryCancelledException();
            }
            for (Sink sink : sinks) {
                if (sink != null) {
                    all.absorb(sink);
                    taken += sink.taken;
                    effort += sink.taken;
                }
            }
        }
    }
}
