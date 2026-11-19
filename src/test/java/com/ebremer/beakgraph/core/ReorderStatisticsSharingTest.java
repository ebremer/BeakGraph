package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * BG-1: the VoID statistics are dataset-wide and loaded once, on the dataset's graph. Each view's
 * reorder transform is its own - it costs patterns by their exact counts in the view's graphs -
 * but every one of them reads the root's statistics, so GRAPH ?g does not reload them per graph.
 */
class ReorderStatisticsSharingTest {

    @TempDir
    Path dir;

    @Test
    void everyViewCostsByTheRootsStatistics() throws Exception {
        Path src = dir.resolve("s.trig");
        Files.writeString(src, """
            @prefix ex: <http://ex.org/> .
            ex:a ex:p ex:b .
            ex:g1 { ex:a ex:p ex:c . ex:a ex:q ex:d . }
            ex:g2 { ex:e ex:p ex:f . }
            """);
        File h5 = dir.resolve("s.h5").toFile();
        HDF5Writer.Builder().setVoidMode(VoidMode.EXACT).setSource(src.toFile()).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph root = new BeakGraph(new HDF5Reader(h5))) {
            DatasetGraph dsg = root.getDataset().asDatasetGraph();
            BGReorderTransform rootTransform = assertInstanceOf(BGReorderTransform.class, root.getReorderTransform());
            for (BeakGraph view : new BeakGraph[] {
                    (BeakGraph) dsg.getGraph(NodeFactory.createURI("http://ex.org/g1")),
                    (BeakGraph) dsg.getGraph(NodeFactory.createURI("http://ex.org/g2")),
                    (BeakGraph) dsg.getGraph(Quad.unionGraph)}) {
                BGReorderTransform t = assertInstanceOf(BGReorderTransform.class, view.getReorderTransform());
                assertNotSame(rootTransform, t);
                assertSame(rootTransform.stats(), t.stats(), "a view must read the root's statistics, not reload them");
            }
            BGReorderTransform walk = assertInstanceOf(BGReorderTransform.class, root.reorderTransformForGraphs(new long[] {1, 2}));
            assertSame(rootTransform.stats(), walk.stats());
        }
    }
}
