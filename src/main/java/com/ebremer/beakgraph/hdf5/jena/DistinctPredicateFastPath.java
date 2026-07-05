package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
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
 * Index-backed fast path for {@code SELECT DISTINCT ?p WHERE { ?s ?p ?o }}.
 *
 * <p>The GPOS index's second level lists, per graph, exactly the predicates that
 * occur in that graph (sorted, each once) - so this query shape is answerable by
 * reading one small index range instead of scanning every quad and deduplicating
 * (on a PubMed-scale store: 53 index entries vs billions of rows, which no
 * request timeout survives).
 *
 * <p>An earlier DISTINCT optimization was removed as unsound: it streamed the
 * file-global columnar id lists (over-reporting terms from other named graphs,
 * including the VoID metadata graph), dropped FILTERs, and discarded the
 * incoming iterator. This one is sound by construction because it only fires on
 * the exact algebra shape whose answer the index directly stores:
 *
 * <pre>  (distinct (project (?p) (bgp (?s ?p ?o))))            - active graph
 *  (distinct (project (?p) (graph &lt;g&gt; (bgp (?s ?p ?o)))))  - concrete graph / union</pre>
 *
 * with all three pattern positions distinct unbound variables, only the
 * predicate variable projected, no filter (a FILTER changes the algebra shape,
 * so it cannot fire here), and the plain top-level execution input (a single
 * root binding that binds none of the pattern's variables). Everything else
 * falls back to normal execution. Per-graph enumeration means the default graph
 * never over-reports predicates that exist only in named graphs, and
 * {@code urn:x-arq:unionGraph} unions the named graphs' lists (default graph
 * excluded), matching scan semantics.
 */
public final class DistinctPredicateFastPath {

    private static final Logger logger = LoggerFactory.getLogger(DistinctPredicateFastPath.class);

    /** Fast-path activations; observability for tests and diagnostics. */
    public static final AtomicLong HITS = new AtomicLong();

    private DistinctPredicateFastPath() {}

    /**
     * Answers the query from the GPOS index, or returns null (without having
     * consumed {@code input}) when it is not exactly the supported shape.
     * <p>
     * The caller guarantees {@code input} wraps the single-binding root iterator
     * (it checks {@code instanceof QueryIterRoot} before wrapping into the peek);
     * consuming that one binding here is therefore safe on the success path.
     */
    public static QueryIterator tryExecute(OpDistinct opDistinct, QueryIterPeek input, ExecutionContext execCxt) {
        if (!(execCxt.getActiveGraph() instanceof BeakGraph bg)) return null;
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
        if (!Var.alloc(p).equals(v)) return null; // only the predicate position is index-answerable

        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        if (gpos == null) return null; // index absent (foreign/old file): scan instead

        // --- Input ---
        Binding root = input.peek();
        if (root == null) return null; // exhausted input: the normal path answers empty immediately
        // A pre-bound pattern variable (initial bindings) constrains the answer - scan instead.
        if (root.contains(Var.alloc(s)) || root.contains(v) || root.contains(Var.alloc(o))) return null;
        input.next(); // consume the single root binding (unrelated vars are dropped by the project)

        // --- Enumerate predicate ids from the index ---
        Set<Long> pids = new LinkedHashSet<>();
        if (Quad.isUnionGraph(target)) {
            // Union-of-named-graphs semantics: every stored graph except the default.
            long defaultGi = dict.getGraphs().locate(Quad.defaultGraphIRI);
            dict.streamGraphIds()
                    .filter(gid -> gid != defaultGi)
                    .forEach(gid -> collectGraphPredicates(gpos, gid, pids));
        } else {
            Node g = Quad.isDefaultGraph(target) ? Quad.defaultGraphIRI : target;
            collectGraphPredicates(gpos, dict.getGraphs().locate(g), pids);
        }

        HITS.incrementAndGet();
        logger.debug("DISTINCT ?p answered from GPOS index: graph={}, {} predicates", target, pids.size());

        NodeTable nodeTable = reader.getNodeTable();
        Iterator<Binding> out = Iter.removeNulls(Iter.map(pids.iterator(), pid -> {
            Node pn = nodeTable.getNodeForNodeId(new NodeId(pid, NodeType.PREDICATE));
            return (pn == null) ? null : BindingFactory.binding(v, pn);
        }));
        return QueryIterPlainWrapper.create(out, execCxt);
    }

    /**
     * Adds the ids of every predicate present in graph {@code gi} - the GPOS
     * P-level range belonging to that graph. Sorted and duplicate-free within one
     * graph by index construction; id 0 is the padding row of an empty graph block.
     */
    private static void collectGraphPredicates(IndexReader gpos, long gi, Set<Long> out) {
        if (gi < 1) return;
        BitPackedUnSignedLongBuffer bp = gpos.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer sp = gpos.getIDBuffer('P');
        HDTBitmapDirectory dirP = gpos.getDirectory('P');
        long start = select1(dirP, bp, gi);
        if (start == -1) return;
        long next = select1(dirP, bp, gi + 1);
        long end = (next == -1) ? sp.getNumEntries() - 1 : next - 1;
        for (long i = start; i <= end; i++) {
            long pid = sp.get(i);
            if (pid >= 1) {
                out.add(pid);
            }
        }
    }

    private static long select1(HDTBitmapDirectory dir, BitPackedUnSignedLongBuffer fallback, long rank) {
        if (rank < 1) return -1;
        return (dir != null) ? dir.select1(rank) : fallback.select1(rank);
    }
}
