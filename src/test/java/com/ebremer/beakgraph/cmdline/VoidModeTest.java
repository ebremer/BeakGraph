package com.ebremer.beakgraph.cmdline;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * VoID generation policy: OFF by default, exact with -void, HyperLogLog with
 * -voidsketch, and refusal when both are requested.
 */
class VoidModeTest {

    @TempDir
    Path dir;

    private File build(String name, VoidMode mode) throws Exception {
        File src = dir.resolve(name + ".ttl").toFile();
        Files.write(src.toPath(),
                ("<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
               + "<http://ex.org/a> <http://ex.org/q> \"x\" .\n").getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve(name + ".h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setVoidMode(mode).build().write();
        return h5;
    }

    private boolean hasVoidGraph(File h5) throws Exception {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                    .query(QueryFactory.create(
                            "ASK { GRAPH <" + Params.VOIDSTRING + "> { ?s ?p ?o } }")).build()) {
                return qe.execAsk();
            }
        }
    }

    @Test
    void noVoidGraphByDefault() throws Exception {
        assertFalse(hasVoidGraph(build("off", VoidMode.NONE)),
                "without -void/-voidsketch the statistics graph must not exist");
    }

    @Test
    void exactAndSketchModesWriteTheVoidGraph() throws Exception {
        assertTrue(hasVoidGraph(build("exact", VoidMode.EXACT)), "-void must write the statistics graph");
        assertTrue(hasVoidGraph(build("sketch", VoidMode.SKETCH)), "-voidsketch must write the statistics graph");
    }

    @Test
    void cliOptionsParseAndMapToModes() {
        Parameters p = new Parameters();
        com.beust.jcommander.JCommander.newBuilder().addObject(p).build()
                .parse("-src", "x", "-voidsketch");
        assertTrue(p.voidSketch);
        assertFalse(p.voidExact);
        assertFalse(new Parameters().voidExact, "VoID must be OFF by default");
        assertFalse(new Parameters().voidSketch, "VoID must be OFF by default");
    }

    @Test
    void voidAndVoidSketchTogetherAreRefused() {
        Parameters p = new Parameters();
        p.src = dir.toFile();
        p.voidExact = true;
        p.voidSketch = true;
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new BeakGraphCLI(p),
                "-void plus -voidsketch must refuse to run");
        assertTrue(ex.getMessage().contains("mutually exclusive"));
    }

    @Test
    void voidGraphIsTheOnlyDifference() throws Exception {
        // Same data, void off vs on: the data graphs must be identical.
        File off = build("cmp-off", VoidMode.NONE);
        File on = build("cmp-on", VoidMode.EXACT);
        try (BeakGraph a = new BeakGraph(new HDF5Reader(off));
             BeakGraph b = new BeakGraph(new HDF5Reader(on))) {
            assertTrue(a.getDataset().getDefaultModel().isIsomorphicWith(b.getDataset().getDefaultModel()));
            assertFalse(a.getDataset().asDatasetGraph().listGraphNodes().hasNext(),
                    "no named graphs at all without void");
            assertEquals(Params.VOIDSTRING,
                    b.getDataset().asDatasetGraph().listGraphNodes().next().getURI(),
                    "with -void the only named graph is the statistics graph");
        }
    }
}
