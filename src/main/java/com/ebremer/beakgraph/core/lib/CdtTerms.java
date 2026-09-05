package com.ebremer.beakgraph.core.lib;

import java.util.List;
import java.util.Map;
import org.apache.jena.cdt.CDTKey;
import org.apache.jena.cdt.CDTValue;
import org.apache.jena.cdt.CompositeDatatypeBase;
import org.apache.jena.graph.Node;

/**
 * Term-kind helpers for SPARQL-CDT composite literals (cdt:List / cdt:Map).
 *
 * <p>Composite literals ride the term-exact strings path with no storage
 * changes, but two of their properties need special handling elsewhere:
 * they have no canonical form (so term identity IS lexical identity - see
 * {@link NodeComparator}), and their lexical forms may embed blank node
 * labels whose co-reference BeakGraph's rank-derived relabeling would
 * silently sever (so ingest rejects those - see the writer guards).
 */
public final class CdtTerms {

    private CdtTerms() {}

    /** True when {@code n} is a literal whose datatype is cdt:List or cdt:Map. */
    public static boolean isComposite(Node n) {
        return n.isLiteral() && n.getLiteralDatatype() instanceof CompositeDatatypeBase;
    }

    /**
     * True when a well-formed composite literal's value contains a blank node
     * at any nesting depth. The SPARQL-CDT spec (section 5.2) requires a blank
     * node label inside a composite literal to co-refer with the same label
     * outside it, but BeakGraph regenerates blank-node labels from dictionary
     * rank - the label in the literal's text would keep naming a blank node
     * that no longer exists in the graph. Callers reject such literals at
     * ingest rather than storing that silent severing.
     *
     * <p>An ILL-FORMED composite literal returns {@code false}: it has no
     * parseable value, so there is no co-reference to break. (Unreachable from
     * parsed sources - RIOT's CDT-aware default profile rejects ill-formed
     * composite lexical forms at parse - but possible from programmatically
     * built nodes.) Note the check parses the composite value (cost O(size),
     * composite literals only).
     */
    public static boolean containsBlankNode(Node n) {
        if (!isComposite(n)) {
            return false;
        }
        Object value;
        try {
            value = n.getLiteralValue();
        } catch (RuntimeException ex) {
            return false; // ill-formed: opaque, nothing co-refers
        }
        return valueContainsBlankNode(value);
    }

    /**
     * Whether a composite literal's value holds a document-relative IRI
     * (recursing into nested lists / maps and map keys). The lexical form
     * is stored verbatim, so a relative reference inside it is neither
     * relativized at ingest nor resolved at serve time: the reader re-parses
     * the value against Jena's system base (the server's working directory)
     * while the same reference outside the literal is served under the store's
     * URL - one resource, two IRIs, and a {@code file:} path leaked to every
     * client. Callers reject such literals at ingest (BG-395), as they do
     * blank nodes. At ingest the parser has resolved the reference against
     * the sentinel base, so the test is "under the sentinel host, or still
     * without a scheme"; an ill-formed composite returns false (opaque).
     */
    public static boolean containsRelativeIri(Node n) {
        if (!isComposite(n)) {
            return false;
        }
        Object value;
        try {
            value = n.getLiteralValue();
        } catch (RuntimeException ex) {
            return false;
        }
        return valueContainsRelativeIri(value);
    }

    /**
     * Rejects a literal the store cannot represent faithfully: a composite
     * literal with a blank node or a document-relative IRI inside. THE guard
     * every engine's literal registration calls (the RAM builder, the ultra
     * ingest, the disk pipeline), so the policy cannot drift between them.
     */
    public static void requireStorable(Node o) {
        if (containsBlankNode(o)) {
            throw new IllegalStateException(
                    "Unsupported object literal (blank node inside cdt: composite literal cannot be stored; its co-reference with the graph would silently break): " + o);
        }
        if (containsRelativeIri(o)) {
            throw new IllegalStateException(
                    "Unsupported object literal (relative IRI inside cdt: composite literal cannot be stored; it is neither relativized at ingest nor resolved when served, so it would name a different resource than the same reference outside the literal): " + o);
        }
    }

    private static boolean isRelativeIri(String uri) {
        return uri.startsWith(RelativeIris.SENTINEL_PREFIX) || com.ebremer.beakgraph.utils.UTIL.isRelativeIRI(uri);
    }

    private static boolean valueContainsRelativeIri(Object value) {
        if (value instanceof List<?> list) {
            for (Object e : list) {
                if (e instanceof CDTValue v && cdtValueContainsRelativeIri(v)) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (e.getKey() instanceof CDTKey k && k.asNode() != null && k.asNode().isURI()
                        && isRelativeIri(k.asNode().getURI())) {
                    return true;
                }
                if (e.getValue() instanceof CDTValue v && cdtValueContainsRelativeIri(v)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private static boolean cdtValueContainsRelativeIri(CDTValue v) {
        if (v.isNull() || !v.isNode()) {
            return false;
        }
        Node node = v.asNode();
        if (node.isURI()) {
            return isRelativeIri(node.getURI());
        }
        return containsRelativeIri(node); // recurse into nested cdt literals
    }

    private static boolean valueContainsBlankNode(Object value) {
        if (value instanceof List<?> list) {
            for (Object e : list) {
                if (e instanceof CDTValue v && cdtValueContainsBlankNode(v)) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                // The grammar forbids blank nodes as map KEYS; checked anyway so a
                // parser relaxation cannot reopen the co-reference hole.
                if (e.getKey() instanceof CDTKey k && k.asNode() != null && k.asNode().isBlank()) {
                    return true;
                }
                if (e.getValue() instanceof CDTValue v && cdtValueContainsBlankNode(v)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private static boolean cdtValueContainsBlankNode(CDTValue v) {
        if (v.isNull() || !v.isNode()) {
            // null elements carry no node; parsed CDT values otherwise always
            // wrap nodes (nested lists/maps surface as cdt-typed literal nodes).
            return false;
        }
        Node node = v.asNode();
        if (node.isBlank()) {
            return true;
        }
        return containsBlankNode(node); // recurse into nested cdt literals
    }
}
