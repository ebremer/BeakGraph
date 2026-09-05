package com.ebremer.beakgraph.hdf5.writers.plaid;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The plaid writer's whole point is many-file merges with concurrent
 * parsing, so the parity test IS a many-file merge: 24 documents (more than
 * the parse workers, so workers cycle through several files and their row
 * batches interleave nondeterministically), duplicates across files, shared
 * terms, per-document _:b0 blank nodes, and named graphs - built with tiny
 * spill batches, then compared against the sequential in-memory writer's
 * merge of the same sources.
 */
class PlaidWriterParityTest {

    @TempDir
    static Path dir;

    @BeforeAll
    static void requireNativeHdf5() {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
    }

    private static Set<String> graphNames(org.apache.jena.query.Dataset ds) {
        Set<String> names = new TreeSet<>();
        ds.asDatasetGraph().listGraphNodes().forEachRemaining(g -> names.add(g.toString()));
        return names;
    }

    private static Model graphModel(org.apache.jena.query.Dataset ds, String graphUri) {
        String q = (graphUri == null)
                ? "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
                : "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + graphUri + "> { ?s ?p ?o } }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            return qe.execConstruct();
        }
    }

    private static long count(org.apache.jena.query.Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            return qe.execSelect().next().getLiteral("n").getLong();
        }
    }

    @Test
    void manyFileMergeMatchesSequentialMerge() throws Exception {
        Path src = Files.createDirectories(dir.resolve("plaidsrc"));
        List<File> inputs = new ArrayList<>();
        for (int f = 0; f < 24; f++) {
            StringBuilder nq = new StringBuilder();
            for (int i = 0; i < 400; i++) {
                int k = f * 400 + i;
                // Shared subjects/objects across files force cross-file dictionary dedup.
                nq.append("<http://ex.org/s").append(k % 300).append("> <http://ex.org/p")
                  .append(k % 7).append("> ");
                nq.append(switch (k % 3) {
                    case 0 -> "<http://ex.org/o" + (k % 200) + ">";
                    case 1 -> "\"v" + (k % 150) + "\"";
                    // BG-182: alternate canonical and zero-padded spellings so the
                    // seq-vs-plaid isomorphism covers numeric canonicalization.
                    default -> (k % 2 == 0)
                            ? "\"" + (k % 90 - 45) + "\"^^<http://www.w3.org/2001/XMLSchema#int>"
                            : "\"0" + (k % 90) + "\"^^<http://www.w3.org/2001/XMLSchema#int>";
                });
                if (k % 4 == 0) {
                    nq.append(" <http://ex.org/g").append(k % 5).append('>');
                }
                nq.append(" .\n");
                // Deliberate duplicates within and across files.
                if (k % 50 == 0) {
                    nq.append("<http://ex.org/dup> <http://ex.org/p0> <http://ex.org/dup> .\n");
                }
            }
            // The SAME bnode label in every document: 24 distinct nodes.
            nq.append("_:b0 <http://ex.org/bp> \"doc").append(f).append("\" .\n");
            // Ill-typed, base-direction and triple-term objects in every file.
            nq.append("<http://ex.org/dup> <http://ex.org/ill> \"abc\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");
            nq.append("<http://ex.org/dup> <http://ex.org/dl> \"doc").append(f).append("\"@en--ltr .\n");
            nq.append("<http://ex.org/dup> <http://ex.org/tt> <<( <http://ex.org/a> <http://ex.org/b> <http://ex.org/c").append(f % 3).append("> )>> .\n");
            File file = src.resolve(String.format("part%02d.nq", f)).toFile();
            Files.write(file.toPath(), nq.toString().getBytes(StandardCharsets.UTF_8));
            inputs.add(file);
        }

        File seq = dir.resolve("plaid.seq.h5").toFile();
        File plaid = dir.resolve("plaid.plaid.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(seq).build().write();
        PlaidHDF5Writer.Builder()
                .setSources(inputs)
                .setDestination(plaid)
                .setWorkDirectory(Files.createDirectories(dir.resolve("plaidwork")))
                .setCores(6) // more files than workers: batches interleave heavily
                .setTermSpillBatch(512)
                .setIdSpillBatch(1024)
                .setMergeFanIn(3)
                .build()
                .write();

        try (BeakGraph a = new BeakGraph(new HDF5Reader(seq));
             BeakGraph b = new BeakGraph(new HDF5Reader(plaid))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();
            assertEquals(graphNames(da), graphNames(db), "graph lists must match");
            assertTrue(graphModel(da, null).isIsomorphicWith(graphModel(db, null)),
                    "default graphs must be isomorphic");
            for (String g : graphNames(da)) {
                if (!g.startsWith("http")) continue;
                assertTrue(graphModel(da, g).isIsomorphicWith(graphModel(db, g)),
                        "graph <" + g + "> must be isomorphic");
            }
            assertEquals(count(da, "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"),
                         count(db, "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"),
                    "default-graph size must match (duplicates collapsed identically)");
            assertEquals(24, count(db, "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?v }"),
                    "_:b0 from 24 documents must remain 24 distinct blank nodes");
            assertEquals(count(da, "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s <http://ex.org/p2> ?o }"),
                         count(db, "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s <http://ex.org/p2> ?o }"),
                    "zero-padded spellings must collapse identically");
            assertEquals(1, count(db, "SELECT (COUNT(*) AS ?n) WHERE { <http://ex.org/dup> <http://ex.org/ill> \"abc\"^^<http://www.w3.org/2001/XMLSchema#int> }"));
            assertEquals(24, count(db, "SELECT (COUNT(?o) AS ?n) WHERE { <http://ex.org/dup> <http://ex.org/dl> ?o }"));
            assertEquals(3, count(db, "SELECT (COUNT(?o) AS ?n) WHERE { <http://ex.org/dup> <http://ex.org/tt> ?o }"));
        }
    }

    /** BG-182: the SPATIAL graph and the derived features must match the sequential build. */
    /**
     * BG-110: when one of several documents fails, the other parse workers
     * are stopped and DRAINED before the pipeline is closed and the workspace
     * removed - no worker keeps committing into closed sorters, no run file
     * is left open under the deletion, no {@code .bgplaid-*} directory
     * survives.
     */
    @Test
    void aFailingDocumentAbortsTheMergeCleanly() throws Exception {
        Path src = Files.createDirectories(dir.resolve("abortsrc"));
        List<File> inputs = new ArrayList<>();
        for (int f = 0; f < 6; f++) {
            StringBuilder nq = new StringBuilder();
            for (int i = 0; i < 4000; i++) {
                nq.append("<http://ex.org/s").append(i % 500).append("> <http://ex.org/p").append(f)
                  .append("> \"v").append(i).append("\" .\n");
            }
            if (f == 3) {
                nq.append("not an n-quads line @@@\n");
            }
            File file = src.resolve("doc" + f + ".nq").toFile();
            Files.write(file.toPath(), nq.toString().getBytes(StandardCharsets.UTF_8));
            inputs.add(file);
        }
        Path work = Files.createDirectories(dir.resolve("abortwork"));
        File dest = dir.resolve("abort.h5").toFile();
        assertThrows(java.io.IOException.class, () -> PlaidHDF5Writer.Builder()
                .setSources(inputs).setDestination(dest).setWorkDirectory(work).setCores(3)
                .setTermSpillBatch(64).setIdSpillBatch(128).setMergeFanIn(2)
                .build().write());
        try (var entries = Files.list(work)) {
            assertEquals(List.of(), entries.toList(), "no workspace survives the failed merge");
        }
        assertFalse(dest.exists(), "nothing was published");
        try (var entries = Files.list(dir)) {
            assertTrue(entries.noneMatch(p -> p.getFileName().toString().endsWith(".tmp")), "no temp output left");
        }
    }

    /**
     * BG-112: VoID statistics computed from parse workers calling the
     * accumulator concurrently must equal the sequential writer's, in both
     * modes - the statistics graph is isomorphic and every (predicate, value)
     * pair in it agrees.
     */
    @Test
    void voidStatisticsParity() throws Exception {
        Path src = Files.createDirectories(dir.resolve("voidsrc"));
        List<File> inputs = new ArrayList<>();
        for (int f = 0; f < 4; f++) {
            String trig = "@prefix ex: <http://ex.org/> .\n@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
                    + "ex:a" + f + " rdf:type ex:T1 . ex:a" + f + " ex:p ex:b . ex:a" + f + " ex:p \"x" + f + "\" . ex:a" + f + " ex:p ex:b .\n"
                    + "ex:b rdf:type ex:T2 . ex:b ex:q \"1\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
                    + "ex:g1 { ex:c" + f + " rdf:type ex:T1 . ex:c" + f + " ex:p ex:d . ex:c" + f + " ex:p ex:d . ex:d ex:q \"y\"@en . }\n"
                    + "ex:g" + f + " { ex:e ex:p ex:f" + f + " . }\n";
            File file = src.resolve("v" + f + ".trig").toFile();
            Files.write(file.toPath(), trig.getBytes(StandardCharsets.UTF_8));
            inputs.add(file);
        }
        for (com.ebremer.beakgraph.core.VoidMode mode : new com.ebremer.beakgraph.core.VoidMode[]{
                com.ebremer.beakgraph.core.VoidMode.EXACT, com.ebremer.beakgraph.core.VoidMode.SKETCH}) {
            File seq = dir.resolve("void-" + mode + ".seq.h5").toFile();
            File plaid = dir.resolve("void-" + mode + ".plaid.h5").toFile();
            HDF5Writer.Builder().setSources(inputs).setDestination(seq).setVoidMode(mode).build().write();
            PlaidHDF5Writer.Builder().setSources(inputs).setDestination(plaid).setVoidMode(mode)
                    .setWorkDirectory(Files.createDirectories(dir.resolve("voidwork-" + mode))).setCores(3)
                    .setTermSpillBatch(64).setIdSpillBatch(128).setMergeFanIn(2)
                    .build().write();
            com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.assertVoidParity(seq.toPath(), plaid.toPath(), mode.toString());
        }
    }

    @Test
    void spatialAndFeaturesParity() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            @prefix geo: <http://www.opengis.net/ont/geosparql#> .

            ex:geo1 geo:asWKT "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))"^^geo:wktLiteral .
            ex:geo2 geo:asWKT "MULTIPOLYGON(((200 200, 300 200, 300 300, 200 200)),((600 600, 700 600, 700 700, 600 600)))"^^geo:wktLiteral .
            ex:geo3 geo:asWKT "POINT(5000 6000)"^^geo:wktLiteral .
            ex:geo4 geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POLYGON((10 10, 60 10, 60 60, 10 60, 10 10))"^^geo:wktLiteral .
            ex:geo5 geo:asWKT "POLYGON((0 0, 1 0))"^^geo:wktLiteral .
            ex:geo1 ex:label "region one" .
            """;
        File src = dir.resolve("spatial.trig").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve("spatial.seq.h5").toFile();
        File plaid = dir.resolve("spatial.plaid.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq).setSpatial(true).setFeatures(true).build().write();
        PlaidHDF5Writer.Builder()
                .setSpatial(true).setFeatures(true)
                .setSource(src)
                .setDestination(plaid)
                .setWorkDirectory(Files.createDirectories(dir.resolve("spatialwork")))
                .setCores(2)
                .build()
                .write();
        try (BeakGraph a = new BeakGraph(new HDF5Reader(seq));
             BeakGraph b = new BeakGraph(new HDF5Reader(plaid))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();
            assertEquals(graphNames(da), graphNames(db), "graph lists (grid tiles included) must match");
            for (String g : graphNames(da)) {
                if (!g.startsWith("http") && !g.startsWith("urn")) continue;
                assertTrue(graphModel(da, g).isIsomorphicWith(graphModel(db, g)), "graph <" + g + "> must be isomorphic");
            }
            assertTrue(graphModel(da, null).isIsomorphicWith(graphModel(db, null)), "default graphs (features) must be isomorphic");
            assertTrue(count(db, "SELECT (COUNT(*) AS ?n) WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }") > 0);
        }
    }

    @Test
    void singleFileBuildStillWorks() throws Exception {
        File src = dir.resolve("one.ttl").toFile();
        Files.write(src.toPath(),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n<http://ex.org/a> <http://ex.org/q> \"x\" .\n"
                        .getBytes(StandardCharsets.UTF_8));
        File out = dir.resolve("one.plaid.h5").toFile();
        PlaidHDF5Writer.Builder()
                .setSource(src)
                .setDestination(out)
                .setWorkDirectory(Files.createDirectories(dir.resolve("onework")))
                .setCores(2)
                .build()
                .write();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(out))) {
            assertTrue(bg.find(org.apache.jena.graph.NodeFactory.createURI("http://ex.org/a"),
                               org.apache.jena.graph.NodeFactory.createURI("http://ex.org/p"),
                               org.apache.jena.graph.NodeFactory.createURI("http://ex.org/b")).hasNext());
        }
    }
}
