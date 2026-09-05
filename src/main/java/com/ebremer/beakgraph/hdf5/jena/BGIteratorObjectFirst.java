package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;

/**
 * {@code ?s ?p <o>} in a known graph (BG-335). The store has GSPO and GPOS
 * only - no index starts with the object - so the previous answer was a walk
 * over the whole graph with an object clamp ({@link BGIteratorSPO_All}):
 * O(rows in the graph) for one object lookup, per input binding. Instead, the
 * graph's predicate list is read from the GPOS first level (the same per-graph
 * list the DISTINCT ?p fast path streams) and each predicate becomes one
 * {@link BGIteratorOS} probe with the predicate pre-bound in a child binding:
 * (#predicates in the graph) bitmap-directory lookups, no scan. Rows carry the
 * predicate id through the child binding; every other position is handled by
 * the OS iterator as usual, range FILTER hints on the subject included.
 * Property paths that seed from the object ({@code ?s :p+ <o>} steps,
 * {@code ^!:p}) reach this through {@code Graph.find(ANY, ANY, o)}.
 */
final class BGIteratorObjectFirst implements Iterator<BindingNodeId> {

    /** Number of object-first dispatches - tests pin the routing. */
    public static final AtomicLong HITS = new AtomicLong();

    private final Iterator<BindingNodeId> chain;

    /**
     * The predicate must be a variable (it is pre-bound per probe) and must not
     * share its name with the subject variable: a subject reading that
     * cross-space id would be wrong. That shape keeps the SPO_All route.
     */
    static boolean applicable(Quad quad) {
        Node p = quad.getPredicate();
        Node s = quad.getSubject();
        return p.isVariable() && !(s.isVariable() && s.getName().equals(p.getName()));
    }

    BGIteratorObjectFirst(PositionalDictionaryReader dict, IndexReader gpos, BindingNodeId bnid, Quad quad,
                          ExprList filter, NodeTable nodeTable) {
        HITS.incrementAndGet();
        long gi;
        if (quad.getGraph().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getGraph())) : NodeId.NONE;
            gi = (bound != NodeId.NONE) ? NodeId.id(bound) : -1;
        } else {
            gi = dict.getGraphs().locate(quad.getGraph());
        }
        long oi;
        if (quad.getObject().isVariable()) {
            long bound = (bnid != null) ? bnid.get(Var.alloc(quad.getObject())) : NodeId.NONE;
            oi = (bound != NodeId.NONE) ? NodeId.id(bound) : -1;
        } else {
            oi = dict.getObjects().locate(quad.getObject());
        }
        if (gi < 1 || oi < 1) {
            chain = Collections.emptyIterator();
            return;
        }
        // The graph's block at the GPOS predicate level; a block whose first id
        // is 0 is the writer's padding row for an empty graph.
        BitPackedUnSignedLongBuffer bp = gpos.getBitmapBuffer('P');
        BitPackedUnSignedLongBuffer sp = gpos.getIDBuffer('P');
        HDTBitmapDirectory dirP = gpos.getDirectory('P');
        long pStart = RangeSelect.blockStart(dirP, bp, gi);
        if (pStart == -1) {
            chain = Collections.emptyIterator();
            return;
        }
        long pEnd = RangeSelect.blockEnd(dirP, bp, gi, pStart);
        if (pStart > pEnd || sp.get(pStart) == 0) {
            chain = Collections.emptyIterator();
            return;
        }
        Var pVar = Var.alloc(quad.getPredicate());
        chain = Iter.flatMap(LongStream.rangeClosed(pStart, pEnd).iterator(), pos -> {
            long pid = sp.get(pos);
            if (pid < 1) {
                return Collections.<BindingNodeId>emptyIterator();
            }
            BindingNodeId child = new BindingNodeId(bnid);
            child.put(pVar, NodeId.pack(NodeType.PREDICATE, pid));
            return new BGIteratorOS(dict, gpos, child, quad, filter, nodeTable);
        });
    }

    @Override
    public boolean hasNext() {
        return chain.hasNext();
    }

    @Override
    public BindingNodeId next() {
        return chain.next();
    }
}
