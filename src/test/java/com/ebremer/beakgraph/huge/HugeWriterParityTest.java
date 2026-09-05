package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end parity: the disk-based writer must produce a store that reads
 * back (through the UNMODIFIED jHDF-based readers) semantically identical to
 * the RAM writer's - same graphs, isomorphic per-graph content (blank node
 * labels legitimately differ), same query answers through both indexes, and
 * structurally identical HDF5 metadata (same dataset tree, same numEntries /
 * width / FCD attributes everywhere). The mixed fixture carries every term
 * kind the disk pipeline handles specially: base-direction literals, nested
 * triple terms and composite (cdt:) literals (BG-126).
 */
class HugeWriterParityTest {

    @TempDir
    static Path dir;

    @BeforeAll
    static void requireNative() {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static Path writeBoth(String name, String content, boolean spatial, boolean features) throws Exception {
        File src = dir.resolve(name).toFile();
        Files.write(src.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return writeBoth(name, src, spatial, features);
    }

    /** Writes src with both writers; returns the directory holding {name}.ram.h5 / {name}.huge.h5. */
    private static Path writeBoth(String name, File src, boolean spatial, boolean features) throws Exception {
        File ram = dir.resolve(name + ".ram.h5").toFile();
        File huge = dir.resolve(name + ".huge.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(ram)
                .setSpatial(spatial).setFeatures(features).build().write();
        HugeHDF5Writer.Builder().setSource(src).setDestination(huge)
                .setSpatial(spatial).setFeatures(features)
                // deliberately tiny batches: force spill runs and multi-level merges
                .setTermSpillBatch(512).setIdSpillBatch(1024).setMergeFanIn(4)
                .build().write();
        return dir;
    }

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    @Test
    void mixedTypesAndNamedGraphs() throws Exception {
        String trig = StoreParity.mixedTrig();
        writeBoth("mixed.trig", trig, false, false);
        Path ram = dir.resolve("mixed.trig.ram.h5");
        Path huge = dir.resolve("mixed.trig.huge.h5");
        StoreParity.assertStoresEquivalent(ram, huge,
                "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
                "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
                "SELECT ?s ?o WHERE { GRAPH <http://ex.org/g1> { ?s <http://ex.org/p0> ?o } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o }",
                "SELECT ?g ?s WHERE { GRAPH ?g { ?s <http://ex.org/p0> <http://ex.org/o0> } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/dl> ?o }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/tt> ?o }",
                "SELECT ?x WHERE { <http://ex.org/s0> <http://ex.org/tt2> <<( <http://ex.org/a> <http://ex.org/b> <<( <http://ex.org/x> <http://ex.org/y> ?x )>> )>> }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/list> ?o }");
        StoreParity.assertSameStructure(ram, huge);
    }

    @Test
    void spatialAndFeaturesParity() throws Exception {
        String trig = StoreParity.SPATIAL_TRIG;
        writeBoth("spatial.trig", trig, true, true);
        Path ram = dir.resolve("spatial.trig.ram.h5");
        Path huge = dir.resolve("spatial.trig.huge.h5");
        StoreParity.assertStoresEquivalent(ram, huge,
                "SELECT ?s ?p ?o WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }",
                "SELECT ?g WHERE { GRAPH ?g { <http://ex.org/geo1> ?p ?o } }");
        StoreParity.assertSameStructure(ram, huge);
    }

    @Test
    void stressManySpillRunsMatchesRamWriter() throws Exception {
        StringBuilder nq = new StringBuilder(1 << 22);
        for (int i = 0; i < 30_000; i++) {
            String s = "<http://ex.org/s" + (i % 500) + ">";
            String p = "<http://ex.org/p" + (i % 7) + ">";
            String o = switch (i % 4) {
                case 0 -> "<http://ex.org/o" + (i % 300) + ">";
                case 1 -> "\"str" + (i % 1000) + "\"";
                case 2 -> "\"" + (i % 1000 - 500) + "\"^^<http://www.w3.org/2001/XMLSchema#int>";
                default -> "\"" + ((i % 100) / 8.0) + "\"^^<http://www.w3.org/2001/XMLSchema#double>";
            };
            nq.append(s).append(' ').append(p).append(' ').append(o);
            if (i % 3 != 0) {
                nq.append(" <http://ex.org/g").append(i % 20).append('>');
            }
            nq.append(" .\n");
        }
        writeBoth("stress.nq", nq.toString(), false, false);
        Path ram = dir.resolve("stress.nq.ram.h5");
        Path huge = dir.resolve("stress.nq.huge.h5");
        StoreParity.assertStoresEquivalent(ram, huge,
                "SELECT ?s ?o WHERE { ?s <http://ex.org/p3> ?o }",
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g",
                "SELECT ?s WHERE { GRAPH <http://ex.org/g7> { ?s <http://ex.org/p1> \"str101\" } }");
        StoreParity.assertSameStructure(ram, huge);
    }

    /**
     * BG-126: per-document blank-node scoping must reach INSIDE triple terms
     * (HugeBuildPipeline.scopeNode recurses): two documents that both say
     * {@code _:b0} outside and inside a term stay two nodes, each co-referring
     * with itself, and the merge agrees with the sequential writer's.
     */
    @Test
    void mergeScopesBlankNodesPerDocumentInsideTripleTerms() throws Exception {
        Path src = Files.createDirectories(dir.resolve("ttmerge"));
        for (int i = 1; i <= 2; i++) {
            Files.write(src.resolve("d" + i + ".ttl"), ("@prefix : <http://ex.org/> .\n"
                    + "_:b0 :p <<( _:b0 :q :r )>> .\n_:b0 :label \"doc" + i + "\" .\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        java.util.List<File> inputs = java.util.List.of(src.resolve("d1.ttl").toFile(), src.resolve("d2.ttl").toFile());
        File ram = dir.resolve("ttmerge.ram.h5").toFile();
        File huge = dir.resolve("ttmerge.huge.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(ram).build().write();
        HugeHDF5Writer.Builder().setSources(inputs).setDestination(huge)
                .setTermSpillBatch(4).setIdSpillBatch(4).setMergeFanIn(2).build().write();
        String pre = "PREFIX : <http://ex.org/> ";
        StoreParity.assertStoresEquivalent(ram.toPath(), huge.toPath(),
                pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :label ?l }",
                pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :p <<( ?b :q :r )>> }",
                pre + "SELECT ?l WHERE { ?b :label ?l . ?b :p <<( ?b :q :r )>> }");
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(huge))) {
            assertEquals(java.util.List.of("n=\"2\"^^xsd:integer|"), StoreParity.select(bg.getDataset(),
                    pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :p <<( ?b :q :r )>> }"),
                    "each document's _:b0 co-refers with the one inside its own term");
            assertEquals(java.util.List.of("n=\"2\"^^xsd:integer|"), StoreParity.select(bg.getDataset(),
                    pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :label ?l }"));
        }
    }
}
