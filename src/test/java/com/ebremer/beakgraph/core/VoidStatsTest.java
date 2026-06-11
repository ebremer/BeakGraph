package com.ebremer.beakgraph.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        HDF5Writer.Builder().setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        try (HDF5Reader reader = new HDF5Reader(h5)) {
            VoidStats stats = VoidStats.load(reader);
            assertEquals(5, stats.predicateCount(NodeFactory.createURI("http://ex.org/p")),
                    "ex:p appears in two graph partitions (2 + 3) and must sum to 5");
        }
    }
}
