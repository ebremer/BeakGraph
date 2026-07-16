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
