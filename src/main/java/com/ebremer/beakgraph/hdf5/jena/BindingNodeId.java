package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.jena.atlas.lib.Map2;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

public class BindingNodeId extends Map2<Var, NodeId> {
    public static BindingNodeId root = new BindingNodeId(null, null, null) {
        @Override
        public String toString() {
            return "<root>";
        }
    };

    // This is the parent binding - which may be several steps up the chain.
    // This just carried around for later use when we go BindingNodeId back to Binding.
    private final Binding parentBinding;

    // Possible optimization: there are at most 3 possible values so HashMap is overkill.
    // Use a chain of small objects.

    private BindingNodeId(Map<Var, NodeId> map1, Map2<Var, NodeId> map2, Binding parentBinding) {
        super(map1, map2);
        this.parentBinding = parentBinding;
    }

    // Make from an existing BindingNodeId
    public BindingNodeId(BindingNodeId other) {
        this(new HashMap<>(), other, other != null ? other.getParentBinding() : null);
    }

    // Make from an existing Binding
    public BindingNodeId(Binding binding) {
        this(new HashMap<>(), null, binding);
    }

    public BindingNodeId() {
        this(new HashMap<>(), null, null);
    }

    public Binding getParentBinding()    { return parentBinding; }

    /**
     * Binds {@code v} to {@code n}; when {@code v} is already bound, the existing
     * binding is kept. Only safe when the caller guarantees the re-put carries the
     * same term (e.g. re-binding a value derived from this binding's own entry).
     * Row-building code that can see a variable repeated within one triple pattern
     * must use {@link #putCompatible} and reject the row on a conflict instead -
     * silently keeping the first value made {@code ?s ?p ?s} match every triple.
     */
    @Override
    public void put(Var v, NodeId n) {
        if ( v == null || n == null )
            throw new IllegalArgumentException("("+v+","+n+")");
        // Includes conversion where we are copying from parent.
        if (!super.containsKey(v)) {
            super.put(v, n);
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
            super.put(v, n);
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

    public void putAll(BindingNodeId other) {
        Iterator<Var> vIter = other.iterator();
        for (; vIter.hasNext() ; ) {
            Var v = vIter.next();
            if ( v == null )
                throw new IllegalArgumentException("Null key");
            NodeId n = other.get(v);
            if ( n == null )
                throw new IllegalArgumentException("("+v+","+n+")");
            super.put(v, n);
        }
    }
    
    /**
     * Returns a Set of entries representing all bindings (local + parent).
     * Added to support iteration in BGIterator classes.
     * @return 
     */
    public Set<Map.Entry<Var, NodeId>> entrySet() {
        Map<Var, NodeId> allEntries = new HashMap<>();
        for (Var v : this) {
            allEntries.put(v, get(v));
        }
        return allEntries.entrySet();
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
