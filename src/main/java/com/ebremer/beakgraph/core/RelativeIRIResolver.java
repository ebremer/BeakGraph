package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.core.lib.RelativeIris;
import com.ebremer.beakgraph.utils.UTIL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.jena.graph.Node;
import org.apache.jena.datatypes.TypeMapper;
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
 * URL the file is served from, per RFC 3986. Lives in {@code core} so the
 * programmatic API ({@link BeakGraph#getBase()}, the query engine and the
 * dataset graph) can resolve as the servlets do; the former
 * {@code core.fuseki} class is a deprecated alias (BG-396).
 * <p>
 * BeakGraph stores IRIs that were relative in the source RDF document (the empty
 * reference {@code <>}, a sibling such as {@code <image.png>}, a parent such as
 * {@code <../thumbs/t.png>} or a path-absolute {@code </LICENSE>}) in their
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
     * The rewrite fires only when {@code storedTerm} says a relative form IS
     * in the store and the absolute form is NOT. The dictionary holds a term
     * in one form only (relative when the source document used a relative
     * reference), so rewriting unconditionally turned every query naming an
     * absolute-stored same-host IRI into a guaranteed miss. When both forms
     * are stored, the exact term the query named (the absolute one) wins.
     * <p>
     * One served IRI can correspond to several stored forms depending on how
     * the source spelled the reference: {@code ../x} (any number of levels,
     * {@link RelativeIris#relativize}), Jena's own relativization, and the
     * path-absolute {@code /a/x} the writer stores for a source's
     * {@code </a/x>}. Each candidate is tried against the dictionary in turn.
     *
     * @param storedTerm whether a node exists in the store's dictionary
     */
    public NodeTransform absoluteToStorage(Predicate<Node> storedTerm) {
        return tripleTermRecursive(node -> {
            if (base != null && node != null && node.isURI()
                    && !UTIL.isRelativeIRI(node.getURI())) {
                try {
                    if (storedTerm.test(node)) {
                        return node;
                    }
                    for (String candidate : storageCandidates(node.getURI())) {
                        Node relNode = NodeFactory.createURI(candidate);
                        if (storedTerm.test(relNode)) {
                            return relNode;
                        }
                    }
                } catch (RuntimeException ignore) {
                    // leave the node unchanged on any IRI parsing failure
                }
            }
            // A literal typed with an absolute datatype IRI under the base may be
            // stored with the relative datatype the source spelled (BG-394).
            if (base != null && node != null && node.isLiteral()) {
                String dt = node.getLiteralDatatypeURI();
                if (dt != null && !UTIL.isRelativeIRI(dt)) {
                    try {
                        if (storedTerm.test(node)) {
                            return node;
                        }
                        for (String candidate : storageCandidates(dt)) {
                            Node relNode = withDatatype(node, candidate);
                            if (storedTerm.test(relNode)) {
                                return relNode;
                            }
                        }
                    } catch (RuntimeException ignore) {
                        // leave the node unchanged on any IRI parsing failure
                    }
                }
            }
            return node;
        });
    }

    private static Node withDatatype(Node literal, String datatypeUri) {
        return NodeFactory.createLiteralDT(literal.getLiteralLexicalForm(),
                TypeMapper.getInstance().getSafeTypeByName(datatypeUri));
    }

    /** Distinct relative forms the writer may have stored for {@code absolute}, most likely first. */
    private List<String> storageCandidates(String absolute) {
        List<String> out = new ArrayList<>(3);
        String textual = RelativeIris.relativize(base.str(), absolute);
        if (textual != null) {
            out.add(textual);
        }
        IRIx rel = base.relativize(IRIx.create(absolute));
        if (rel != null && rel.isRelative() && !out.contains(rel.str())) {
            out.add(rel.str());
        }
        String pathAbsolute = RelativeIris.pathAbsoluteForm(base.str(), absolute);
        if (pathAbsolute != null && !out.contains(pathAbsolute)) {
            out.add(pathAbsolute);
        }
        return out;
    }

    /**
     * Applies {@code perNode} through RDF 1.2 triple terms too: IRIs INSIDE a
     * term need the same storage/serving-form rewrite as top-level ones. Both
     * per-node rewrites here are idempotent, so this stays correct even where
     * Jena's own transform plumbing also recurses into triple terms.
     */
    private static NodeTransform tripleTermRecursive(NodeTransform perNode) {
        return node -> (node != null && node.isTripleTerm())
                ? com.ebremer.beakgraph.core.lib.TripleTerms.map(node, perNode::apply)
                : perNode.apply(node);
    }

    /**
     * Output transform: a relative IRI (storage form) is resolved against the
     * document base into an absolute IRI - as a term, and as the datatype of a
     * literal ({@code "7"^^<scoreType>} is served as
     * {@code "7"^^<base-resolved scoreType>}). Absolute IRIs pass through
     * unchanged.
     */
    public NodeTransform storageToAbsolute() {
        return tripleTermRecursive(node -> {
            if (base != null && node != null && node.isURI()
                    && UTIL.isRelativeIRI(node.getURI())) {
                try {
                    return NodeFactory.createURI(base.resolve(node.getURI()).str());
                } catch (RuntimeException ignore) {
                    // leave the node unchanged on any IRI parsing failure
                }
            }
            if (base != null && node != null && node.isLiteral()) {
                String dt = node.getLiteralDatatypeURI();
                if (dt != null && UTIL.isRelativeIRI(dt)) {
                    try {
                        return withDatatype(node, base.resolve(dt).str());
                    } catch (RuntimeException ignore) {
                        // leave the node unchanged on any IRI parsing failure
                    }
                }
            }
            return node;
        });
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
        final RowSet rows = RowSet.adapt(rs);
        final List<Var> vars = rows.getResultVars();
        Iterator<Binding> it = new Iterator<>() {
            @Override public boolean hasNext() { return rows.hasNext(); }
            @Override public Binding next() { return resolve(rows.next()); }
        };
        return ResultSetStream.create(vars, it);
    }

    /**
     * One result row with every relative IRI resolved on the way out (the
     * VALUES of the binding; Jena's {@code NodeTransformLib.transform(Binding,
     * NodeTransform)} maps only the variables). What the query engine applies
     * to every row of an execution over a graph with a base (BG-396).
     */
    public Binding resolve(Binding in) {
        if (base == null) {
            return in;
        }
        final NodeTransform t = storageToAbsolute();
        BindingBuilder bb = Binding.builder();
        in.forEach((v, n) -> bb.add(v, t.apply(n)));
        return bb.build();
    }

    /**
     * Resolve every relative IRI of a CONSTRUCT / DESCRIBE triple stream on the
     * way out - lazily, one triple at a time (the streaming counterpart of
     * {@link #resolve(Model)}, for responses that must not be materialized).
     */
    public Iterator<Triple> resolve(Iterator<Triple> triples) {
        if (base == null) {
            return triples;
        }
        final NodeTransform t = storageToAbsolute();
        return org.apache.jena.atlas.iterator.Iter.map(triples, triple -> NodeTransformLib.transform(t, triple));
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
