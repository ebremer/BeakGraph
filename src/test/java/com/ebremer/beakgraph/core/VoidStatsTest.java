package com.ebremer.beakgraph.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The VoID description carries one void:propertyPartition per (graph,
 * predicate) pair, so a predicate used in several graphs appears in several
 * partitions. VoidStats must SUM them: a plain put() kept whichever partition
 * HashMap iteration visited last, feeding the reorder cost model a per-graph
 * count against the dataset-wide total.
 */
class VoidStatsTest {

    @TempDir
    Path dir;

    @Test
    void predicateCountsSumAcrossGraphPartitions() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            ex:s1 ex:p ex:o1 .
            ex:s2 ex:p ex:o2 .
            ex:g1 {
                ex:s3 ex:p ex:o3 .
                ex:s4 ex:p ex:o4 .
                ex:s5 ex:p ex:o5 .
            }
            """;
        File src = dir.resolve("stats.trig").toFile();
        File h5 = dir.resolve("stats.trig.h5").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        try (HDF5Reader reader = new HDF5Reader(h5)) {
            VoidStats stats = VoidStats.load(reader);
            assertEquals(5, stats.predicateCount(NodeFactory.createURI("http://ex.org/p")),
                    "ex:p appears in two graph partitions (2 + 3) and must sum to 5");
        }
    }

    /**
     * The distinct subject / object counts are the sizes of the columnar role
     * lists, not of the dictionary sections: the entity section pools graph
     * names and object-only entities, so a bound-object pattern was always
     * costed as more selective than a bound-subject one (BG-12).
     */
    @Test
    void distinctCountsAreTheRoleListsNotTheSectionSizes() throws Exception {
        String ttl = """
            @prefix ex: <http://ex.org/> .
            ex:s1 ex:p ex:o1 .
            ex:s1 ex:q "x" .
            """;
        File src = dir.resolve("roles.ttl").toFile();
        File h5 = dir.resolve("roles.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            VoidStats stats = VoidStats.load(reader);
            // The VoID metadata graph is part of the store, so its own subjects and
            // objects count too; what matters is that the counts are the role lists.
            assertEquals(reader.getDictionary().subjectCount(), stats.distinctSubjects());
            assertEquals(reader.getDictionary().objectCount(), stats.distinctObjects());
            assertTrue(stats.distinctSubjects() < reader.getDictionary().getSubjects().getNumberOfNodes(),
                    "ex:o1 is an entity but never a subject: the section size overstates");
            assertTrue(stats.distinctObjects() < reader.getDictionary().getObjects().getNumberOfNodes(),
                    "ex:s1 is an entity but never an object: the section size overstates");
        }
    }
}
