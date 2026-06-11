package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import java.util.Iterator;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBase;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class BindingBG extends BindingBase {
    private final BeakGraph bGraph;
    private final BindingNodeId idBinding;
    private final Binding parent;
    private static final Logger logger = LoggerFactory.getLogger(BindingBG.class);

    public BindingBG(BindingNodeId idBinding, BeakGraph bGraph) {
        super(idBinding.getParentBinding());
        this.idBinding = idBinding;
        this.parent = idBinding.getParentBinding();
        this.bGraph = bGraph;
    }

    public BindingNodeId getBindingId() {
        return idBinding ;
    }

    /**
     * Whether this level of the binding exposes {@code var}. BindingBase's
     * contract is that a child never re-binds a parent var - size() is
     * size1() + parent.size() and vars() concatenates both levels - but the
     * id-map deliberately COPIES every parent var (SolverLibBeak.convert) so
     * the solvers can substitute at the id level. Exposing those copies here
     * as well double-counted every input var: size() lied, vars() yielded
     * duplicates, and DISTINCT could not merge a row of this shape with an
     * equal-valued row of normal shape. Parent vars therefore resolve through
     * the parent, which carries the identical term the ids were derived from -
     * including terms not in this store at all (the "does not exist" ids).
     */
    private boolean ownVar(Var var) {
        return idBinding.containsKey(var) && (parent == null || !parent.contains(var));
    }

    @Override
    protected Node get1(Var var) {
        if (ownVar(var)) {
            NodeId id = idBinding.get(var);
            // A var bound to "does not exist" has no node here; return null so
            // BindingBase falls back to the parent binding.
            if (NodeId.isDoesNotExist(id)) {
                return null;
            }
            return bGraph.getReader().getNodeTable().getNodeForNodeId(id);
        }
        return null;
    }

    @Override
    protected Iterator<Var> vars1() {
        return Iter.filter(idBinding.iterator(), this::ownVar);
    }

    @Override
    protected int size1() {
        int n = 0;
        Iterator<Var> it = idBinding.iterator();
        while (it.hasNext()) {
            if (ownVar(it.next())) {
                n++;
            }
        }
        return n;
    }

    @Override
    protected boolean isEmpty1() {
        return size1() == 0;
    }

    @Override
    protected boolean contains1(Var var) {
        return ownVar(var);
    }

    /**
     * Detaching must materialize the NodeId-backed bindings into plain nodes:
     * Jena detaches bindings it copies or spills beyond the execution that
     * created them, and a detached binding must not keep resolving lazily
     * against this graph's reader (which may be closed by then). Both detach
     * paths are overridden - the default "original parent" path would return
     * {@code this}, still reader-backed.
     */
    @Override
    protected Binding detachWithNewParent(Binding newParent) {
        return materialize(newParent);
    }

    @Override
    protected Binding detachWithOriginalParent() {
        return materialize(idBinding.getParentBinding());
    }

    private Binding materialize(Binding newParent) {
        BindingBuilder builder = Binding.builder(newParent);
        Iterator<Var> it = idBinding.iterator();
        while (it.hasNext()) {
            Var v = it.next();
            if (builder.contains(v)) continue;
            Node n = get1(v);
            if (n != null) {
                builder.add(v, n);
            }
        }
        return builder.build();
    }
}
