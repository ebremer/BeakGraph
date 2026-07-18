package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * A Var -&gt; packed-NodeId binding layer, chained to an optional parent layer,
 * carrying the original Jena {@link Binding} for eventual conversion back.
 *
 * <p>One of these is allocated PER RESULT ROW by every iterator, so the layout
 * is a single object with four inline (Var, long) slots - a quad pattern binds
 * at most four variables per layer - no backing arrays, no per-value NodeId
 * objects (ids are packed longs, {@link NodeId}). Lookups scan the own slots
 * and then the parent chain; absence is {@link NodeId#NONE}, not null. A layer
 * never re-binds a variable already bound in itself or its chain ({@link #put}
 * keeps the first binding; {@link #putCompatible} reports the conflict), so
 * iteration over the chain never yields duplicate variables.
 */
public class BindingNodeId implements Iterable<Var> {

    // This is the parent binding - which may be several steps up the chain.
    // This just carried around for later use when we go BindingNodeId back to Binding.
    private final Binding parentBinding;
    private final BindingNodeId parent;

    private Var v0, v1, v2, v3;
    private long i0, i1, i2, i3;
    private int n = 0;

    private BindingNodeId(BindingNodeId parent, Binding parentBinding) {
        this.parent = parent;
        this.parentBinding = parentBinding;
    }

    // Make from an existing BindingNodeId
    public BindingNodeId(BindingNodeId other) {
        this(other, other != null ? other.getParentBinding() : null);
    }

    // Make from an existing Binding
    public BindingNodeId(Binding binding) {
        this(null, binding);
    }

    public BindingNodeId() {
        this(null, (Binding) null);
    }

    public Binding getParentBinding()    { return parentBinding; }

    /** The packed id bound to {@code v}, or {@link NodeId#NONE}. */
    public long get(Var v) {
        for (BindingNodeId layer = this; layer != null; layer = layer.parent) {
            int m = layer.n;
            if (m > 0 && layer.v0.equals(v)) return layer.i0;
            if (m > 1 && layer.v1.equals(v)) return layer.i1;
            if (m > 2 && layer.v2.equals(v)) return layer.i2;
            if (m > 3 && layer.v3.equals(v)) return layer.i3;
        }
        return NodeId.NONE;
    }

    public boolean containsKey(Var v) {
        return get(v) != NodeId.NONE;
    }

    /**
     * True when this layer's four inline slots are used. Callers that need to
     * bind more variables (triple-term unification binds embedded variables on
     * top of a row's own) CHAIN a new layer - the struct itself is never
     * widened, so ordinary rows keep the allocation- and branch-free shape.
     */
    boolean isFull() {
        return n == 4;
    }

    private void append(Var v, long id) {
        switch (n) {
            case 0 -> { v0 = v; i0 = id; }
            case 1 -> { v1 = v; i1 = id; }
            case 2 -> { v2 = v; i2 = id; }
            case 3 -> { v3 = v; i3 = id; }
            default -> throw new IllegalStateException(
                    "BindingNodeId layer holds at most 4 bindings (a quad pattern's positions); chain a new layer instead");
        }
        n++;
    }

    /**
     * Binds {@code v} to {@code id}; when {@code v} is already bound, the existing
     * binding is kept. Only safe when the caller guarantees the re-put carries the
     * same term (e.g. re-binding a value derived from this binding's own entry).
     * Row-building code that can see a variable repeated within one triple pattern
     * must use {@link #putCompatible} and reject the row on a conflict instead -
     * silently keeping the first value made {@code ?s ?p ?s} match every triple.
     */
    public void put(Var v, long id) {
        if ( v == null || id == NodeId.NONE )
            throw new IllegalArgumentException("("+v+","+NodeId.toString(id)+")");
        // Includes conversion where we are copying from parent.
        if (!containsKey(v)) {
            append(v, id);
        }
    }

    /**
     * Binds {@code v} to {@code id}, or reports a conflict: returns true when the
     * binding was added or the existing binding denotes the same term, false when
     * {@code v} is already bound to a different term (the caller must reject the
     * row). {@code nodeTable} is needed only to compare ids across the predicate /
     * entity id-spaces; pass null when that cross-space case cannot occur.
     */
    public boolean putCompatible(Var v, long id, NodeTable nodeTable) {
        if ( v == null || id == NodeId.NONE )
            throw new IllegalArgumentException("("+v+","+NodeId.toString(id)+")");
        long existing = get(v);
        if (existing == NodeId.NONE) {
            append(v, id);
            return true;
        }
        return sameTerm(existing, id, nodeTable);
    }

    /**
     * Whether two packed NodeIds denote the same RDF term. GRAPH, SUBJECT and
     * OBJECT ids share the universal entity id-space (object literals are offset
     * beyond the entity range), so within it equal ids mean equal terms.
     * PREDICATE ids live in an isolated dictionary, so a predicate/entity pair is
     * resolved to Nodes and compared as terms.
     */
    private static boolean sameTerm(long a, long b, NodeTable nodeTable) {
        if (a == b) return true;
        if (NodeId.isSpecial(a) || NodeId.isSpecial(b)) return false; // a == b already said no
        boolean aPred = NodeId.isPredicateSpace(a);
        boolean bPred = NodeId.isPredicateSpace(b);
        if (aPred == bPred) {
            // Same id-space (both predicate, or both universal entity/object space).
            return NodeId.id(a) == NodeId.id(b);
        }
        if (nodeTable == null) return false;
        Node na = nodeTable.getNodeForNodeId(a);
        Node nb = nodeTable.getNodeForNodeId(b);
        return na != null && na.equals(nb);
    }

    private Var slot(int i) {
        return switch (i) {
            case 0 -> v0;
            case 1 -> v1;
            case 2 -> v2;
            default -> v3;
        };
    }

    /**
     * All bound variables: this layer's, then the parent chain's. Duplicate-free
     * because a layer never re-binds a variable already bound below it.
     */
    @Override
    public Iterator<Var> iterator() {
        return new Iterator<>() {
            private BindingNodeId layer = BindingNodeId.this;
            private int i = 0;

            private void advance() {
                while (layer != null && i >= layer.n) {
                    layer = layer.parent;
                    i = 0;
                }
            }

            @Override
            public boolean hasNext() {
                advance();
                return layer != null;
            }

            @Override
            public Var next() {
                advance();
                if (layer == null) throw new NoSuchElementException();
                return layer.slot(i++);
            }
        };
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for ( Var v : this )
        {
            if ( ! first )
                sb.append(" ");
            first = false;
            long x = get(v);
            if ( ! NodeId.isDoesNotExist(x)) {
                sb.append(v);
                sb.append(" = ");
                sb.append(NodeId.toString(x));
            }
        }
        if ( getParentBinding() != null ) {
            sb.append(" ->> ");
            sb.append(getParentBinding());
        }
        return sb.toString();
    }
}
