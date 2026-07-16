package com.ebremer.beakgraph.hdf5.writers.plaid;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.huge.NativeHdf5File;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assumptions;
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
        Assumptions.assumeTrue(NativeHdf5File.isAvailable(), "native HDF5 library unavailable");
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
                    default -> "\"" + (k % 90 - 45) + "\"^^<http://www.w3.org/2001/XMLSchema#int>";
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
