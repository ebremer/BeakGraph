package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ebremer.beakgraph.core.Dictionary;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;

/**
 * BG-68: the cluster walk probes ids that are provably valid, so a failing
 * {@code extract} is a real read failure (an HTTP range read, a corrupt
 * block). It used to be swallowed: the walk stopped early and the range bound
 * snapped INSIDE the value-equal cluster - exactly the silent boundary loss
 * ValueCluster exists to prevent. The failure must surface instead.
 */
class ValueClusterFailureTest {

    private static Node lit(String lex, XSDDatatype dt) {
        return NodeFactory.createLiteralDT(lex, dt);
    }

    /** A value-ordered dictionary whose reads fail: the ids are valid, the storage is not. */
    private static final class FailingDictionary implements Dictionary {
        final List<Node> nodes = List.of(
                lit("4", XSDDatatype.XSDint), lit("5", XSDDatatype.XSDint),
                lit("5", XSDDatatype.XSDinteger), lit("6", XSDDatatype.XSDint));
        final UncheckedIOException failure = new UncheckedIOException(new IOException("range read failed"));

        @Override public long locate(Node element) { return search(element) >= 0 ? search(element) : -1; }
        @Override public long search(Node element) {
            int i = nodes.indexOf(element);
            return i >= 0 ? i + 1 : -(3) - 1; // an absent probe lands between the fives and the six
        }
        @Override public Node extract(long id) { throw failure; }
        @Override public long getNumberOfNodes() { return nodes.size(); }
        @Override public Stream<Node> streamNodes() { return nodes.stream(); }
    }

    @Test
    void aFailingExtractSurfacesFromTheNumericBounds() {
        FailingDictionary dict = new FailingDictionary();
        UncheckedIOException ex = assertThrows(UncheckedIOException.class,
                () -> ValueCluster.of(dict, lit("5", XSDDatatype.XSDint)));
        assertSame(dict.failure, ex, "the read failure itself, not a narrowed range");
    }

    @Test
    void aFailingExtractSurfacesFromTheClusterWalk() {
        FailingDictionary dict = new FailingDictionary();
        UncheckedIOException ex = assertThrows(UncheckedIOException.class,
                () -> ValueCluster.cluster(dict, lit("5", XSDDatatype.XSDint)));
        assertSame(dict.failure, ex);
    }
}
