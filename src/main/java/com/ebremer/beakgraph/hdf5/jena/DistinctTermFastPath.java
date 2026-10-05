package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index-backed fast path for {@code SELECT DISTINCT ?x WHERE { ?s ?p ?o }} where
 * {@code ?x} is the predicate or the subject variable.
 *
 * <p>Both indexes store per-graph term levels that ARE these queries' answers:
 * <ul>
 *   <li><b>?p</b> - the GPOS predicate level under a graph lists exactly the
 *       predicates occurring in that graph (sorted, each once): a few dozen index
 *       entries instead of scanning every quad. Small by nature, so the union
 *       graph is served too (per-graph lists de-duplicated in a set).</li>
 *   <li><b>?s</b> - the GSPO subject level under a graph lists exactly the
 *       graph's subjects, sorted and duplicate-free. It is STREAMED (no dedup
 *       state at all): at PubMed scale that is 447M rows the scan-based
 *       DISTINCT would otherwise have to pull through a hash set after reading
 *       billions of quads. The union graph would need cross-graph
 *       de-duplication (a k-way sorted merge - future work), so it falls back.</li>
 * </ul>
 *
 * <p>{@code ?o} has no single index level (objects repeat per predicate group)
 * and falls back. An earlier DISTINCT optimization was removed as unsound - it
 * streamed file-global id lists (over-reporting terms from other graphs,
 * including the VoID metadata graph), dropped FILTERs, and ignored the incoming
 * iterator. This one is sound by construction because it fires only on the
 * exact algebra shape whose answer the index directly stores:
 *
 * <pre>  (distinct (project (?x) (bgp (?s ?p ?o))))            - active graph
 *  (distinct (project (?x) (graph &lt;g&gt; (bgp (?s ?p ?o)))))  - concrete graph</pre>
 *
 * with all three pattern positions distinct unbound variables, exactly one
 * projected variable, no filter (a FILTER changes the algebra shape, so it
 * cannot fire here), and the plain top-level execution input (a single root
 * binding that binds none of the pattern's variables). Everything else falls
 * back to normal execution.
 */
public final class DistinctTermFastPath {

    private static final Logger logger = LoggerFactory.getLogger(DistinctTermFastPath.class);

    /** Fast-path activations; observability for tests and diagnostics. */
    public static final AtomicLong HITS = new AtomicLong();

    private DistinctTermFastPath() {}

    /**
     * Answers the query from index structure, or returns null (without having
     * consumed {@code input}) when it is not exactly a supported shape.
     * <p>
     * The caller guarantees {@code input} wraps the single-binding root iterator
     * (it checks {@code instanceof QueryIterRoot} before wrapping into the peek);
     * consuming that one binding here is therefore safe on the success path.
     */
    public static QueryIterator tryExecute(OpDistinct opDistinct, QueryIterPeek input, ExecutionContext execCxt) {
        if (!(execCxt.getActiveGraph() instanceof BeakGraph bg)) return null;
        if (bg.isGraphSetView()) return null; // several graphs: no single index level answers it
        if (!(bg.getReader() instanceof HDF5Reader reader)) return null;
        if (!(reader.getDictionary() instanceof PositionalDictionaryReader dict)) return null;

        // --- Algebra shape ---
        if (!(opDistinct.getSubOp() instanceof OpProject project)) return null;
        List<Var> projected = project.getVars();
        if (projected.size() != 1) return null;
        Var v = projected.get(0);

        Op inner = project.getSubOp();
        Node target = bg.getNamedGraph();
        if (inner instanceof OpGraph opGraph) {
            if (!opGraph.getNode().isConcrete()) return null; // GRAPH ?g - different answer shape
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
        if (s.equals(p) || p.equals(o) || s.equals(o)) return null; // repeated vars constrain rows

        boolean predicatePosition = Var.alloc(p).equals(v);
        boolean subjectPosition = Var.alloc(s).equals(v);
        if (!predicatePosition && !subjectPosition) return null; // ?o: no single index level

        IndexReader index = reader.getIndexReader(predicatePosition ? Index.GPOS : Index.GSPO);
        if (index == null) return null; // index absent (foreign/old file): scan instead

        // Union DISTINCT ?s needs cross-graph de-duplication of potentially
        // hundreds of millions of ids - fall back until a sorted-merge exists.
        if (subjectPosition && Quad.isUnionGraph(target)) return null;

        // --- Input ---
        Binding root = input.peek();
        if (root == null) return null; // exhausted input: the normal path answers empty immediately
        // A pre-bound pattern variable (initial bindings) constrains the answer - scan instead.
        if (root.contains(Var.alloc(s)) || root.contains(Var.alloc(p)) || root.contains(Var.alloc(o))) return null;
        input.next(); // consume the single root binding (unrelated vars are dropped by the project)

        HITS.incrementAndGet();
        NodeTable nodeTable = reader.getNodeTable();

        if (predicatePosition) {
            Set<Long> pids = new LinkedHashSet<>();
            if (Quad.isUnionGraph(target)) {
                // Union-of-named-graphs semantics: every stored graph except the default.
                long defaultGi = dict.getGraphs().locate(Quad.defaultGraphIRI);
                dict.streamGraphIds()
                        .filter(gid -> gid != defaultGi)
                        .forEach(gid -> collectGraphPredicates(index, gid, pids));
            } else {
                collectGraphPredicates(index, resolveGraphId(dict, target), pids);
            }
            logger.debug("DISTINCT ?p answered from GPOS index: graph={}, {} predicates", target, pids.size());
            Iterator<Binding> out = Iter.removeNulls(Iter.map(pids.iterator(), pid -> {
                Node pn = nodeTable.getNodeForNodeId(NodeId.pack(NodeType.PREDICATE, pid));
                return (pn == null) ? null : BindingFactory.binding(v, pn);
            }));
            return QueryIterPlainWrapper.create(out, execCxt);
        }

        // Subject position: stream the graph's GSPO subject level - already sorted
        // and duplicate-free, so no dedup state regardless of subject count.
        long[] range = RangeSelect.firstLevelRange(index, 'S', resolveGraphId(dict, target));
        logger.debug("DISTINCT ?s answered from GSPO index: graph={}, {} subjects",
                target, (range == null) ? 0 : (range[1] - range[0] + 1));
        if (range == null) {
            return QueryIterPlainWrapper.create(Collections.emptyIterator(), execCxt);
        }
        Iterator<Binding> out = new SubjectBindings(index.getIDBuffer('S'), range[0], range[1], nodeTable, v);
        return QueryIterPlainWrapper.create(out, execCxt);
    }

    /** The graph's first-level id (entity id), or -1 for "no rows here". */
    private static long resolveGraphId(PositionalDictionaryReader dict, Node target) {
        Node g = Quad.isDefaultGraph(target) ? Quad.defaultGraphIRI : target;
        return dict.getGraphs().locate(g);
    }

    /**
     * Adds the ids of every predicate present in graph {@code gi} - the GPOS
     * P-level range belonging to that graph. Sorted and duplicate-free within one
     * graph by index construction; id 0 is the padding row of an empty graph block.
     */
    private static void collectGraphPredicates(IndexReader gpos, long gi, Set<Long> out) {
        long[] range = RangeSelect.firstLevelRange(gpos, 'P', gi);
        if (range == null) return;
        BitPackedUnSignedLongBuffer sp = gpos.getIDBuffer('P');
        for (long i = range[0]; i <= range[1]; i++) {
            long pid = sp.get(i);
            if (pid >= 1) {
                out.add(pid);
            }
        }
    }


    /**
     * Lazily streams one binding per subject id in [start..end] of the GSPO
     * subject level. Look-ahead so unresolvable ids are skipped rather than
     * emitted as null (mirrors the predicate path's removeNulls).
     */
    private static final class SubjectBindings implements Iterator<Binding> {
        private final BitPackedUnSignedLongBuffer ss;
        private final NodeTable nodeTable;
        private final Var var;
        private final long end;
        private long i;
        private Binding pending;

        SubjectBindings(BitPackedUnSignedLongBuffer ss, long start, long end, NodeTable nodeTable, Var var) {
            this.ss = ss;
            this.end = end;
            this.i = start;
            this.nodeTable = nodeTable;
            this.var = var;
            advance();
        }

        private void advance() {
            pending = null;
            while (pending == null && i <= end) {
                long sid = ss.get(i++);
                if (sid < 1) continue; // defensive: padding rows never appear in non-empty blocks
                Node n = nodeTable.getNodeForNodeId(NodeId.pack(NodeType.SUBJECT, sid));
                if (n != null) {
                    pending = BindingFactory.binding(var, n);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return pending != null;
        }

        @Override
        public Binding next() {
            if (pending == null) throw new NoSuchElementException();
            Binding r = pending;
            advance();
            return r;
        }
    }
}
