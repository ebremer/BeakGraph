package com.ebremer.beakgraph.hdf5.writers.ultra;

import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end parity of the ultra writer against the sequential writer, read
 * back through the UNMODIFIED jHDF-based readers: same graph lists, isomorphic
 * per-graph content, same query answers through both indexes, structurally
 * identical HDF5 metadata and - for single-source builds, where the ultra
 * ingest reuses the sequential blank node labels verbatim - BYTE-identical
 * files (methods 0/2/3 are one format written through one jHDF sequence,
 * SPECIFICATIONS.md). VoID is off by default, so no random labels enter these
 * builds. Multi-source merges use per-document blank-node labels, so the
 * merge test asserts semantic equivalence only.
 */
class UltraWriterParityTest {

    @TempDir
    static Path dir;

    // ------------------------------------------------------------------
    // helpers (the established parity bar of ParallelWriterParityTest)
    // ------------------------------------------------------------------

    private static Path writeBoth(String name, String content, boolean spatial, boolean features) throws Exception {
        File src = dir.resolve(name).toFile();
        Files.write(src.toPath(), content.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve(name + ".seq.h5").toFile();
        File ult = dir.resolve(name + ".ultra.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq)
                .setSpatial(spatial).setFeatures(features).build().write();
        UltraHDF5Writer.Builder().setSource(src).setDestination(ult)
                .setSpatial(spatial).setFeatures(features)
                // deliberately odd core count: shakes out anything that silently
                // assumes the default or an even chunk split
                .setCores(3)
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
        Path seq = dir.resolve("mixed.trig.seq.h5");
        Path ult = dir.resolve("mixed.trig.ultra.h5");
        StoreParity.assertStoresEquivalent(seq, ult,
                "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
                "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
                "SELECT ?s ?o WHERE { GRAPH <http://ex.org/g1> { ?s <http://ex.org/p0> ?o } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o }",
                "SELECT ?g ?s WHERE { GRAPH ?g { ?s <http://ex.org/p0> <http://ex.org/o0> } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/dl> ?o }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/tt> ?o }",
                "SELECT ?x WHERE { <http://ex.org/s0> <http://ex.org/tt2> <<( <http://ex.org/a> <http://ex.org/b> <<( <http://ex.org/x> <http://ex.org/y> ?x )>> )>> }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/list> ?o }");
        StoreParity.assertSameStructure(seq, ult);
        StoreParity.assertSameBytes(seq, ult);
    }

    @Test
    void spatialAndFeaturesParity() throws Exception {
        String trig = StoreParity.SPATIAL_TRIG;
        writeBoth("spatial.trig", trig, true, true);
        Path seq = dir.resolve("spatial.trig.seq.h5");
        Path ult = dir.resolve("spatial.trig.ultra.h5");
        StoreParity.assertStoresEquivalent(seq, ult,
                "SELECT ?s ?p ?o WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }",
                "SELECT ?g WHERE { GRAPH ?g { <http://ex.org/geo1> ?p ?o } }");
        StoreParity.assertSameStructure(seq, ult);
        StoreParity.assertSameBytes(seq, ult);
    }

    @Test
    void stressManyQuadsMatchesSequentialWriter() throws Exception {
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
        Path seq = dir.resolve("stress.nq.seq.h5");
        Path ult = dir.resolve("stress.nq.ultra.h5");
        StoreParity.assertStoresEquivalent(seq, ult,
                "SELECT ?s ?o WHERE { ?s <http://ex.org/p3> ?o }",
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g",
                "SELECT ?s WHERE { GRAPH <http://ex.org/g7> { ?s <http://ex.org/p1> \"str101\" } }");
        StoreParity.assertSameStructure(seq, ult);
        StoreParity.assertSameBytes(seq, ult);
    }

    /** BG-429: JSON-LD and RDF/XML sources are byte-identical across the engines too. */
    @Test
    void jsonLdAndRdfXmlSourcesAreByteIdentical() throws Exception {
        writeBoth("mixed.jsonld", com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.JSONLD_FIXTURE, false, false);
        writeBoth("mixed.rdf", com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.RDFXML_FIXTURE, false, false);
        for (String name : new String[]{"mixed.jsonld", "mixed.rdf"}) {
            Path seq = dir.resolve(name + ".seq.h5");
            Path ult = dir.resolve(name + ".ultra.h5");
            StoreParity.assertStoresEquivalent(seq, ult,
                    "SELECT ?o WHERE { <http://ex.org/doc> ?p ?o }",
                    "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/tag> ?t }");
            StoreParity.assertSameStructure(seq, ult);
            StoreParity.assertSameBytes(seq, ult);
        }
    }

    @Test
    void singleCoreStillWorks() throws Exception {
        String ttl = "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
                   + "<http://ex.org/a> <http://ex.org/q> \"x\" .\n";
        File src = dir.resolve("one.ttl").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve("one.ttl.seq.h5").toFile();
        File ult = dir.resolve("one.ttl.ultra.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq).build().write();
        UltraHDF5Writer.Builder().setSource(src).setDestination(ult).setCores(1).build().write();
        StoreParity.assertStoresEquivalent(seq.toPath(), ult.toPath(),
                "SELECT ?o WHERE { <http://ex.org/a> ?p ?o }");
        StoreParity.assertSameStructure(seq.toPath(), ult.toPath());
        StoreParity.assertSameBytes(seq.toPath(), ult.toPath());
    }

    /**
     * Merge parity: ultra's PARALLEL multi-document ingest against the
     * sequential writer's one-after-another merge of the same tree. Blank node
     * labels differ by design (per-document scoping vs a global counter), so
     * the bar is semantic: same graphs, isomorphic content, same answers -
     * including two documents that both say {@code _:b0} and must stay two
     * distinct nodes.
     */
    /** BG-112: the ultra engine's own VoID path (concurrent per-document accumulation) matches the sequential writer's. */
    @Test
    void voidStatisticsParityInBothModes() throws Exception {
        File src = dir.resolve("voidparity.trig").toFile();
        Files.write(src.toPath(), com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.VOID_FIXTURE.getBytes(StandardCharsets.UTF_8));
        for (com.ebremer.beakgraph.core.VoidMode mode : new com.ebremer.beakgraph.core.VoidMode[]{
                com.ebremer.beakgraph.core.VoidMode.EXACT, com.ebremer.beakgraph.core.VoidMode.SKETCH}) {
            File seq = dir.resolve("voidparity-" + mode + ".seq.h5").toFile();
            File ult = dir.resolve("voidparity-" + mode + ".ultra.h5").toFile();
            HDF5Writer.Builder().setSource(src).setDestination(seq).setVoidMode(mode).build().write();
            UltraHDF5Writer.Builder().setSource(src).setDestination(ult).setVoidMode(mode).setCores(3).build().write();
            StoreParity.assertStoresEquivalent(seq.toPath(), ult.toPath(),
                    "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g");
            StoreParity.assertVoidParity(seq.toPath(), ult.toPath(), "ultra " + mode);
        }
    }

    @Test
    void mergeParityWithSequentialMerge() throws Exception {
        Path src = Files.createDirectories(dir.resolve("mergesrc"));
        Files.write(src.resolve("m1.ttl"),
                "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o1> .\n_:b0 <http://ex.org/bp> \"v1\" .\n"
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(src.resolve("m2.nq"),
                "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o2> <http://ex.org/gm> .\n"
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(src.resolve("m3.ttl"),
                "_:b0 <http://ex.org/bp> \"v2\" .\n<http://ex.org/s> <http://ex.org/p> <http://ex.org/o3> .\n"
                        .getBytes(StandardCharsets.UTF_8));
        List<File> inputs = List.of(src.resolve("m1.ttl").toFile(), src.resolve("m2.nq").toFile(),
                src.resolve("m3.ttl").toFile());
        File seq = dir.resolve("merge.seq.h5").toFile();
        File ult = dir.resolve("merge.ultra.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(seq).build().write();
        UltraHDF5Writer.Builder().setSources(inputs).setDestination(ult).setCores(3).build().write();
        StoreParity.assertStoresEquivalent(seq.toPath(), ult.toPath(),
                "SELECT ?o WHERE { <http://ex.org/s> <http://ex.org/p> ?o }",
                "SELECT ?o WHERE { GRAPH <http://ex.org/gm> { <http://ex.org/s> ?p ?o } }",
                "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?v }");
        // BG-112: the merge path with statistics too.
        File seqV = dir.resolve("merge.void.seq.h5").toFile();
        File ultV = dir.resolve("merge.void.ultra.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(seqV).setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).build().write();
        UltraHDF5Writer.Builder().setSources(inputs).setDestination(ultV).setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setCores(3).build().write();
        StoreParity.assertStoresEquivalent(seqV.toPath(), ultV.toPath(),
                "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?v }");
        StoreParity.assertVoidParity(seqV.toPath(), ultV.toPath(), "ultra merge EXACT");
    }
}
