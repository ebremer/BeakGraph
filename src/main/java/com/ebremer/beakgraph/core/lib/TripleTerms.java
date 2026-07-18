package com.ebremer.beakgraph.core.lib;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;

/**
 * Shared recursion helpers for RDF 1.2 triple terms ({@code <<( s p o )>>}).
 *
 * <p>Triple terms nest through the object position only (RDF 1.2 Concepts), and
 * the abstract syntax forbids cycles, so every recursion here terminates. All
 * ingest hierarchies and per-quad transforms route through these two shapes so
 * the engines cannot drift on how deeply they walk a term (the mirror-topology
 * hazard PLAN Part I §1.6 documents).
 */
public final class TripleTerms {

    private TripleTerms() {}

    /** Position of a component during a {@link #walk}. */
    public enum Position { SUBJECT, PREDICATE, OBJECT }

    /** Receives each component of a triple term, all nesting depths. */
    public interface ComponentVisitor {
        /**
         * A non-triple-term component: an IRI or blank node in any position, or
         * a literal (object position only).
         */
        void component(Position position, Node node);

        /**
         * A NESTED triple term found in an object position. Its own components
         * are delivered by the ongoing walk; this callback is for registering
         * the nested term itself (e.g. as a dictionary entry in its own right).
         */
        default void nestedTripleTerm(Node tripleTerm) {}
    }

    /**
     * Depth-first walk over every component of {@code tripleTerm}, recursing
     * into nested triple-term objects. The term itself is NOT delivered - only
     * its components (callers already hold the outer term).
     */
    public static void walk(Node tripleTerm, ComponentVisitor visitor) {
        Triple t = tripleTerm.getTriple();
        visitor.component(Position.SUBJECT, t.getSubject());
        visitor.component(Position.PREDICATE, t.getPredicate());
        Node o = t.getObject();
        if (o.isTripleTerm()) {
            visitor.nestedTripleTerm(o);
            walk(o, visitor);
        } else {
            visitor.component(Position.OBJECT, o);
        }
    }

    /** Convenience walk that only cares about leaf components. */
    public static void forEachComponent(Node tripleTerm, Consumer<Node> leaf) {
        walk(tripleTerm, (position, node) -> leaf.accept(node));
    }

    /**
     * Rebuilds {@code tripleTerm} by applying {@code f} to every non-triple-term
     * component, recursing into nested triple-term objects. Returns the SAME
     * node instance when no component changed, so callers can use identity to
     * skip quad reconstruction (matching the relativize() convention).
     *
     * <p>{@code f} is never handed a triple term - nesting is the recursion's
     * job - so per-kind transforms (blank-node alignment, IRI relativization,
     * numeric canonicalization) plug in unchanged.
     */
    public static Node map(Node tripleTerm, UnaryOperator<Node> f) {
        Triple t = tripleTerm.getTriple();
        Node s = t.getSubject();
        Node p = t.getPredicate();
        Node o = t.getObject();
        Node s2 = f.apply(s);
        Node p2 = f.apply(p);
        Node o2 = o.isTripleTerm() ? map(o, f) : f.apply(o);
        if (s2 == s && p2 == p && o2 == o) {
            return tripleTerm;
        }
        return NodeFactory.createTripleTerm(s2, p2, o2);
    }
}
