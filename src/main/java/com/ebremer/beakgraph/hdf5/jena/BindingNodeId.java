package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * A Var -&gt; NodeId binding layer, chained to an optional parent layer, carrying
 * the original Jena {@link Binding} for eventual conversion back.
 *
 * <p>Array-backed rather than map-backed: one of these is allocated PER RESULT
 * ROW by every iterator, and a triple/quad pattern binds at most four variables,
 * so the former HashMap-per-row (16-bucket table + node per entry) was nearly
 * all garbage. Lookups linear-scan the own slots (&le; 4) and then the parent
 * chain - cheaper than hashing at these sizes. A layer never re-binds a
 * variable already bound in itself or its chain ({@link #put} keeps the first
 * binding; {@link #putCompatible} reports the conflict), so iteration over the
 * chain never yields duplicate variables.
 */
public class BindingNodeId implements Iterable<Var> {
    private static final Var[] NO_VARS = new Var[0];
    private static final NodeId[] NO_IDS = new NodeId[0];

    // This is the parent binding - which may be several steps up the chain.
    // This just carried around for later use when we go BindingNodeId back to Binding.
    private final Binding parentBinding;
    private final BindingNodeId parent;

    private Var[] vars = NO_VARS;
    private NodeId[] ids = NO_IDS;
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

    public NodeId get(Var v) {
        for (BindingNodeId layer = this; layer != null; layer = layer.parent) {
            Var[] lv = layer.vars;
            for (int i = 0, m = layer.n; i < m; i++) {
                if (lv[i].equals(v)) {
                    return layer.ids[i];
                }
            }
        }
        return null;
    }

    public boolean containsKey(Var v) {
        return get(v) != null;
    }

    private void append(Var v, NodeId id) {
        if (n == vars.length) {
            int cap = (n == 0) ? 4 : n * 2;
            vars = Arrays.copyOf(vars, cap);
            ids = Arrays.copyOf(ids, cap);
        }
        vars[n] = v;
        ids[n] = id;
        n++;
    }

    /**
     * Binds {@code v} to {@code n}; when {@code v} is already bound, the existing
     * binding is kept. Only safe when the caller guarantees the re-put carries the
     * same term (e.g. re-binding a value derived from this binding's own entry).
     * Row-building code that can see a variable repeated within one triple pattern
     * must use {@link #putCompatible} and reject the row on a conflict instead -
     * silently keeping the first value made {@code ?s ?p ?s} match every triple.
     */
    public void put(Var v, NodeId n) {
        if ( v == null || n == null )
            throw new IllegalArgumentException("("+v+","+n+")");
        // Includes conversion where we are copying from parent.
        if (!containsKey(v)) {
            append(v, n);
        }
    }

    /**
     * Binds {@code v} to {@code n}, or reports a conflict: returns true when the
     * binding was added or the existing binding denotes the same term, false when
     * {@code v} is already bound to a different term (the caller must reject the
     * row). {@code nodeTable} is needed only to compare ids across the predicate /
     * entity id-spaces; pass null when that cross-space case cannot occur.
     */
    public boolean putCompatible(Var v, NodeId n, NodeTable nodeTable) {
        if ( v == null || n == null )
            throw new IllegalArgumentException("("+v+","+n+")");
        NodeId existing = get(v);
        if (existing == null) {
            append(v, n);
            return true;
        }
        return sameTerm(existing, n, nodeTable);
    }

    /**
     * Whether two NodeIds denote the same RDF term. GRAPH, SUBJECT and OBJECT ids
     * share the universal entity id-space (object literals are offset beyond the
     * entity range), so within it equal ids mean equal terms. PREDICATE ids live in
     * an isolated dictionary, so a predicate/entity pair is resolved to Nodes and
     * compared as terms.
     */
    private static boolean sameTerm(NodeId a, NodeId b, NodeTable nodeTable) {
        if (a.equals(b)) return true;
        NodeType ta = a.getType();
        NodeType tb = b.getType();
        if (ta == NodeType.SPECIAL || tb == NodeType.SPECIAL) return false; // a.equals(b) already said no
        boolean aPred = (ta == NodeType.PREDICATE);
        boolean bPred = (tb == NodeType.PREDICATE);
        if (aPred == bPred) {
            // Same id-space (both predicate, or both universal entity/object space).
            return a.getId() == b.getId();
        }
        if (nodeTable == null) return false;
        Node na = nodeTable.getNodeForNodeId(a);
        Node nb = nodeTable.getNodeForNodeId(b);
        return na != null && na.equals(nb);
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
                return layer.vars[i++];
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
            NodeId x = get(v);
            if ( ! NodeId.isDoesNotExist(x)) {
                sb.append(v);
                sb.append(" = ");
                sb.append(x);
            }
        }
        if ( getParentBinding() != null ) {
            sb.append(" ->> ");
            sb.append(getParentBinding());
        }
        return sb.toString();
    }
}
