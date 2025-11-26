package com.ebremer.beakgraph.hdf5.jena;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.jena.atlas.lib.Map2;
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

    @Override
    public void put(Var v, NodeId n) {
        if ( v == null || n == null )
            throw new IllegalArgumentException("("+v+","+n+")");
        // Includes conversion where we are copying from parent.
        if (!super.containsKey(v)) {
            super.put(v, n);
        }
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
