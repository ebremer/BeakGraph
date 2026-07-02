package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.utils.UTIL;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.irix.IRIx;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ResultSetStream;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.graph.NodeTransformLib;

/**
 * Resolves document-relative IRIs stored in a BeakGraph HDF5 file against the
 * URL the file is served from, per RFC 3986.
 * <p>
 * BeakGraph stores IRIs that were relative in the source RDF document (the empty
 * reference {@code <>}, or a sibling such as {@code <image.png>}) in their
 * relative form. The absolute identity of such a resource depends on where the
 * file is retrieved from, so it is computed here at query time rather than baked
 * in at build time. This class converts between the stored "storage form" and
 * the absolute "result form" expected by SPARQL clients:
 * <ul>
 *   <li>{@link #absoluteToStorage(Predicate)} - query input: an absolute IRI
 *       under the base is rewritten to the relative form when the dictionary
 *       holds that relative form (and not the absolute one).</li>
 *   <li>{@link #storageToAbsolute()} - query output: a relative IRI is resolved
 *       against the base into an absolute IRI.</li>
 * </ul>
 */
public class RelativeIRIResolver {

    private final IRIx base;

    /**
     * @param baseURI the URL the HDF5 file is served from (the document base);
     *                may be null, in which case the resolver is a no-op.
     */
    public RelativeIRIResolver(String baseURI) {
        IRIx b = null;
        if (baseURI != null) {
            try {
                b = IRIx.create(baseURI);
            } catch (Exception ex) {
                b = null;
            }
        }
        this.base = b;
    }

    /** True when a usable base is present and transforms will have an effect. */
    public boolean isActive() {
        return base != null;
    }

    /**
     * Input transform: an absolute IRI under the document base is rewritten to
     * its relative form so that queries naming a resource by its served URL
     * still match a relative-stored term. Every other node (variables,
     * literals, blank nodes, unrelated IRIs) passes through.
     * <p>
     * The rewrite fires only when {@code storedTerm} says the relative form IS
     * in the store and the absolute form is NOT. The dictionary holds a term in
     * exactly one of the two forms (relative only when the source document used
     * a relative reference), so rewriting unconditionally turned every query
     * naming an absolute-stored same-host IRI into a guaranteed miss - Jena's
     * relativize also emits {@code /absolute/path} and {@code ../up} forms the
     * writer never produces. When both forms are stored, the exact term the
     * query named (the absolute one) wins.
     *
     * @param storedTerm whether a node exists in the store's dictionary
     */
    public NodeTransform absoluteToStorage(Predicate<Node> storedTerm) {
        return node -> {
            if (base != null && node != null && node.isURI()
                    && !UTIL.isRelativeIRI(node.getURI())) {
                try {
                    IRIx rel = base.relativize(IRIx.create(node.getURI()));
                    if (rel != null && rel.isRelative()) {
                        Node relNode = NodeFactory.createURI(rel.str());
                        if (storedTerm.test(relNode) && !storedTerm.test(node)) {
                            return relNode;
                        }
                    }
                } catch (RuntimeException ignore) {
                    // leave the node unchanged on any IRI parsing failure
                }
            }
            return node;
        };
    }

    /**
     * Output transform: a relative IRI (storage form) is resolved against the
     * document base into an absolute IRI. Absolute IRIs pass through unchanged.
     */
    public NodeTransform storageToAbsolute() {
        return node -> {
            if (base != null && node != null && node.isURI()
                    && UTIL.isRelativeIRI(node.getURI())) {
                try {
                    return NodeFactory.createURI(base.resolve(node.getURI()).str());
                } catch (RuntimeException ignore) {
                    // leave the node unchanged on any IRI parsing failure
                }
            }
            return node;
        };
    }

    /**
     * Wrap a SELECT result so every relative IRI is resolved on the way out.
     * The wrapped result is lazy - it must be consumed before the underlying
     * {@code QueryExecution} is closed.
     */
    public ResultSet resolve(ResultSet rs) {
        if (base == null) {
            return rs;
        }
        final NodeTransform t = storageToAbsolute();
        final RowSet rows = RowSet.adapt(rs);
        final List<Var> vars = rows.getResultVars();
        Iterator<Binding> it = new Iterator<>() {
            @Override public boolean hasNext() { return rows.hasNext(); }
            @Override public Binding next() {
                Binding in = rows.next();
                BindingBuilder bb = Binding.builder();
                in.forEach((v, n) -> bb.add(v, t.apply(n)));
                return bb.build();
            }
        };
        return ResultSetStream.create(vars, it);
    }

    /**
     * Copy a CONSTRUCT / DESCRIBE model, resolving every relative IRI against
     * the document base.
     */
    public Model resolve(Model m) {
        if (base == null) {
            return m;
        }
        final NodeTransform t = storageToAbsolute();
        Model out = ModelFactory.createDefaultModel();
        out.setNsPrefixes(m.getNsPrefixMap());
        Iterator<Triple> it = m.getGraph().find();
        while (it.hasNext()) {
            out.getGraph().add(NodeTransformLib.transform(t, it.next()));
        }
        return out;
    }
}
