package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
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
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import static org.apache.jena.sparql.engine.main.solver.SolverLib.makeAbortable;

/**
 * {@code GRAPH ?g { BGP }} on ids: one walk over the dataset's named graphs,
 * the pattern solved in each by the id-level iterators with {@code ?g} bound
 * to the graph's id.
 *
 * <p>Jena answers this shape by executing the pattern once for every named
 * graph: it substitutes the binding into the algebra, asks the dataset for a
 * graph view, builds an execution context and a whole iterator pipeline, and
 * assigns {@code ?g} to each row afterwards. A store of tiled data holds
 * thousands of graphs, and for a pattern that matches in few of them that
 * setup was the query: 88% of a subject lookup across 5,000 graphs, 58% of
 * it building pipelines (PERFORMANCE.md). Here a graph costs a child binding
 * and the iterator that reads it.
 *
 * <p>What it answers, and what it leaves to Jena:
 * <ul>
 *   <li><b>The shape</b>: a variable graph over a basic graph pattern of at
 *       least one triple, in the store's own dataset
 *       ({@link BGDatasetGraph}). A {@code FILTER} or anything else inside
 *       the {@code GRAPH} block is another algebra shape; a
 *       {@code FROM NAMED} dataset has its own set of named graphs; a triple
 *       term holding variables is matched by unification. All three run as
 *       before.</li>
 *   <li><b>The graphs</b>: exactly those {@code listGraphNodes()} lists - the
 *       stored graphs but the default graph, in the same order - so the two
 *       ways give the same rows.</li>
 *   <li><b>{@code ?g} bound already</b> (a join, {@code VALUES}): that
 *       binding goes to Jena's operator, whose rules for a single graph
 *       (the synthetic default and union graph names, a graph the dataset
 *       does not hold, a literal) are then the ones applied.</li>
 *   <li><b>{@code ?g} in the pattern too</b> ({@code GRAPH ?g { ?g ?p ?o }}):
 *       binding it first and solving is the join SPARQL defines, since a
 *       pattern's solutions are joined with the graph's name.</li>
 * </ul>
 *
 * <p>A large graph met on the way is still scanned in parallel, under the
 * conditions of {@link ScanChunks}, as it was when Jena ran the pattern
 * graph by graph.
 */
public final class GraphVarBG {

    /** {@code GRAPH ?g} patterns answered here; tests pin which shapes are, and which fall back. */
    public static final AtomicLong HITS = new AtomicLong();

    /** {@code -Dbeakgraph.graphvar.fastpath=false} leaves every {@code GRAPH ?g} to Jena's operator. */
    private static volatile boolean enabled = Boolean.parseBoolean(System.getProperty("beakgraph.graphvar.fastpath", "true"));

    private GraphVarBG() {}

    /** Switches this path on or off (tests compare the two ways); returns the previous setting. */
    public static boolean setEnabled(boolean on) {
        boolean previous = enabled;
        enabled = on;
        return previous;
    }

    /**
     * The id-level execution of {@code opGraph}, or null - with {@code input}
     * untouched - when it is not a shape answered here.
     *
     * @param jena Jena's own operator for {@code opGraph} over a given input,
     *             used for the bindings that hold the graph variable already
     */
    static QueryIterator tryExecute(OpGraph opGraph, QueryIterator input, ExecutionContext execCxt, UnaryOperator<QueryIterator> jena) {
        if (!enabled) return null;
        Node graphNode = opGraph.getNode();
        if (!graphNode.isVariable()) return null;
        if (!(opGraph.getSubOp() instanceof OpBGP opBGP)) return null;
        BasicPattern pattern = opBGP.getPattern();
        if (pattern.isEmpty()) return null;
        for (Triple t : pattern) {
            if (isPattern(t.getSubject()) || isPattern(t.getPredicate()) || isPattern(t.getObject())) return null;
        }
        if (!(execCxt.getDataset() instanceof BGDatasetGraph dsg)) return null;
        BeakGraph root = dsg.getBeakGraph();
        if (root.isGraphSetView()) return null;
        if (!(root.getReader() instanceof HDF5Reader reader)) return null;
        if (!(reader.getDictionary() instanceof PositionalDictionaryReader dict) || dict.isEmpty()) return null;
        // The graph the pattern would run against must be a view of the same store.
        if (!(execCxt.getActiveGraph() instanceof BeakGraph active) || active.getReader() != reader) return null;

        if (!input.hasNext()) {
            return input;
        }
        HITS.incrementAndGet();
        PatternMatchBG.HITS.incrementAndGet();
        // A term the store does not hold matches in no graph: say so once, not once a graph.
        for (Triple t : pattern) {
            if ((t.getSubject().isConcrete() && dict.getSubjects().locate(t.getSubject()) < 1)
                    || (t.getPredicate().isConcrete() && dict.getPredicates().locate(t.getPredicate()) < 1)
                    || (t.getObject().isConcrete() && dict.getObjects().locate(t.getObject()) < 1)) {
                input.close();
                return QueryIterNullIterator.create(execCxt);
            }
        }
        if (pattern.size() >= 2) {
            ReorderTransformation transform = root.getReorderTransform();
            if (transform != null) {
                QueryIterPeek peek = QueryIterPeek.create(input, execCxt);
                input = peek;
                pattern = OpExecutorBG.reorder(pattern, peek, transform);
            }
        }
        return new Stage(input, execCxt, jena, Var.alloc(graphNode), pattern.getList().toArray(Triple[]::new),
                root, reader, dict, namedGraphIds(dict, root));
    }

    /** A triple term that holds variables: matched by unification, which the graph walk leaves to the ordinary path. */
    private static boolean isPattern(Node n) {
        return n.isTripleTerm() && !n.isConcrete();
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

    /** One input binding at a time: Jena's operator where it holds the graph variable, the walk over the graphs where it does not. */
    private static final class Stage extends QueryIterRepeatApply {
        private final UnaryOperator<QueryIterator> jena;
        private final Var graphVar;
        private final Triple[] triples;
        private final BeakGraph root;
        private final HDF5Reader reader;
        private final PositionalDictionaryReader dict;
        private final long[] graphs;
        /** Whether the first triple is of a shape a parallel scan answers at all: its subject and object both variables. */
        private final boolean scanShaped;

        Stage(QueryIterator input, ExecutionContext execCxt, UnaryOperator<QueryIterator> jena, Var graphVar, Triple[] triples,
                BeakGraph root, HDF5Reader reader, PositionalDictionaryReader dict, long[] graphs) {
            super(input, execCxt);
            this.jena = jena;
            this.graphVar = graphVar;
            this.triples = triples;
            this.root = root;
            this.reader = reader;
            this.dict = dict;
            this.graphs = graphs;
            this.scanShaped = triples[0].getSubject().isVariable() && triples[0].getObject().isVariable();
        }

        @Override
        protected QueryIterator nextStage(Binding binding) {
            ExecutionContext execCxt = getExecContext();
            if (binding.contains(graphVar)) {
                return jena.apply(QueryIterSingleton.create(binding, execCxt));
            }
            BindingNodeId base = SolverLibBeak.convert(binding, root);
            // A scan is planned in parallel for the root binding only, as PatternMatchBG.solveFirst plans it: a
            // binding that carries variables is an outer row, and its pattern runs once a row.
            long threshold = (scanShaped && PatternMatchBG.isRootLike(base)) ? ScanChunks.threshold() : 0;
            // Scans are added while the walk runs and a cancel reads the list from another thread.
            List<Abortable> killList = new CopyOnWriteArrayList<>();
            Iterator<BindingNodeId> rows = new Walk(base, threshold, killList, execCxt);
            rows = makeAbortable(rows, killList, execCxt.getCancelSignal());
            return new QueryIterAbortable(SolverLibBeak.convertToNodes(rows, root), killList, null, execCxt);
        }

        /** The pattern's rows graph by graph, each graph opened when the one before it is done. */
        private final class Walk implements IteratorCloseable<BindingNodeId> {
            private final BindingNodeId base;
            private final long threshold;
            private final List<Abortable> killList;
            private final ExecutionContext execCxt;
            private final NodeTable nodeTable = reader.getNodeTable();
            private int next;
            private Iterator<BindingNodeId> current;
            private ParallelScan scan;      // the parallel scan under the current graph's rows, if it has one
            private boolean closed;

            Walk(BindingNodeId base, long threshold, List<Abortable> killList, ExecutionContext execCxt) {
                this.base = base;
                this.threshold = threshold;
                this.killList = killList;
                this.execCxt = execCxt;
            }

            private Iterator<BindingNodeId> open(long gid) {
                BindingNodeId child = new BindingNodeId(base);
                child.put(graphVar, NodeId.pack(NodeType.GRAPH, gid));
                Iterator<BindingNodeId> rows = null;
                if (threshold > 0) {
                    scan = ScanChunks.tryParallel(reader, dict, gid, graphVar, child, triples[0], threshold, execCxt);
                    if (scan != null) {
                        killList.add(scan);
                        rows = scan;
                    }
                }
                if (rows == null) {
                    rows = reader.read(gid, child, triples[0], null, nodeTable);
                }
                for (int k = 1; k < triples.length; k++) {
                    Triple t = triples[k];
                    rows = Iter.flatMap(rows, row -> reader.read(gid, row, t, null, nodeTable));
                }
                return rows;
            }

            private void done() {
                Iter.close(current);
                current = null;
                if (scan != null) {
                    killList.remove(scan);
                    scan = null;
                }
            }

            @Override
            public boolean hasNext() {
                for (;;) {
                    if (current != null) {
                        if (current.hasNext()) {
                            return true;
                        }
                        done();
                    }
                    if (closed || next >= graphs.length) {
                        return false;
                    }
                    current = open(graphs[next++]);
                }
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
                closed = true;
                if (current != null) {
                    done();
                }
            }
        }
    }
}
