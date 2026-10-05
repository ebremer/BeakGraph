package com.ebremer.beakgraph.hdf5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.writers.MultiTypeDictionaryWriter;
import java.util.Set;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;

/**
 * The section enum that replaced the writers' second {@code Types} enum
 * (BG-90): the routed kinds per section, the group names it fixes, and the
 * builder taking its configuration from it.
 */
class DictionarySectionTest {

    @Test
    void sectionsRouteTheDocumentedKinds() {
        assertTrue(DictionarySection.ENTITIES.routes().containsAll(Set.of(DataType.IRI, DataType.RELATIVE_IRI, DataType.BNODE)));
        assertFalse(DictionarySection.ENTITIES.routes().contains(DataType.STRING));
        assertFalse(DictionarySection.PREDICATES.routes().contains(DataType.BNODE));
        assertTrue(DictionarySection.LITERALS.routes().containsAll(Set.of(DataType.INTEGER, DataType.LONG, DataType.FLOAT,
                DataType.DOUBLE, DataType.STRING, DataType.TRIPLE_TERM)));
        assertFalse(DictionarySection.LITERALS.routes().contains(DataType.IRI));
        assertFalse(DictionarySection.ENTITIES.routes().contains(DataType.TRIPLE_TERM), "triple terms are object-only");
        assertEquals(Params.ENTITIES, DictionarySection.ENTITIES.groupName());
        assertEquals(Params.PREDICATES, DictionarySection.PREDICATES.groupName());
        assertEquals(Params.LITERALS, DictionarySection.LITERALS.groupName());
        assertThrows(UnsupportedOperationException.class, () -> DictionarySection.LITERALS.routes().add(DataType.IRI));
    }

    @Test
    void theBuilderTakesItsRoutesAndNameFromTheSection() throws Exception {
        Stats stats = new Stats();
        stats.numIRI = 1;
        MultiTypeDictionaryWriter.Builder b = new MultiTypeDictionaryWriter.Builder()
                .setNodes(Set.of(NodeFactory.createURI("http://ex.org/a")))
                .setStats(stats)
                .section(DictionarySection.ENTITIES);
        assertEquals(DictionarySection.ENTITIES.routes(), b.getEnabledTypes());
        assertEquals(1, b.build().getNumberOfNodes());
    }
}
