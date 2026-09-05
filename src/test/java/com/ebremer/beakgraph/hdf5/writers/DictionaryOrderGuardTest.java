package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.DictionarySection;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;

/**
 * BG-104: dictionary ids are comparator ranks, so two entries the comparator
 * cannot separate would share one id - and the index's duplicate check
 * (now on ids) would silently merge their rows. The dictionary build is the
 * one place every engine passes through, so it fails there, loudly.
 */
class DictionaryOrderGuardTest {

    private static DictionaryWriter build(List<Node> sorted) throws Exception {
        Stats stats = new Stats();
        stats.numIRI = sorted.size();
        return new MultiTypeDictionaryWriter.Builder()
                .setName("entities")
                .setSortedNodes(new ArrayList<>(sorted))
                .setStats(stats)
                .section(DictionarySection.ENTITIES)
                .build();
    }

    @Test
    void twoEntriesTheComparatorCannotSeparateFailTheBuild() {
        Node a = NodeFactory.createURI("http://ex.org/a");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> build(List.of(a, a)));
        assertTrue(ex.getMessage().contains("answers 0") && ex.getMessage().contains("share one id"), ex.getMessage());
    }

    @Test
    void anUnsortedCallerSuppliedListFailsTheBuild() {
        Node a = NodeFactory.createURI("http://ex.org/a");
        Node b = NodeFactory.createURI("http://ex.org/b");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> build(List.of(b, a)));
        assertTrue(ex.getMessage().contains("not in NodeComparator order"), ex.getMessage());
    }

    @Test
    void aStrictlyAscendingListBuildsAndRanksAsBefore() throws Exception {
        Node a = NodeFactory.createURI("http://ex.org/a");
        Node b = NodeFactory.createURI("http://ex.org/b");
        DictionaryWriter w = build(List.of(a, b));
        try {
            assertEquals(1, w.locate(a));
            assertEquals(2, w.locate(b));
            assertEquals(2, w.getNumberOfNodes());
        } finally {
            ((AutoCloseable) w).close();
        }
    }
}
