package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-125: the -method 1 term sorters spill on a BYTE budget, not only on the
 * record count, so a batch of multi-KB literals (WKT geometry) cannot pin
 * gigabytes before its first run is written; the sort stays exact across the
 * extra runs, and a whole build with a tiny budget is isomorphic to the RAM
 * writer's. BG-249 rides along: the sequential provider's term order is the
 * per-sorter memoizing comparator, which must agree with the shared one.
 */
class TermSpillBudgetTest {

    @TempDir
    Path dir;

    static String big(Random rnd, int length) {
        StringBuilder sb = new StringBuilder(length);
        sb.append("lit-").append(rnd.nextInt(1_000_000)).append('|');
        while (sb.length() < length) sb.append((char) ('a' + rnd.nextInt(26)));
        return sb.toString();
    }

    @Test
    void estimatesScaleWithTheLexicalForm() {
        Node small = NodeFactory.createLiteralString("x");
        Node large = NodeFactory.createLiteralString("x".repeat(4096));
        assertTrue(HugeRecords.estimateBytes(large) >= 2L * 4096, "a 4 KB literal is at least 8 KB of estimate");
        assertTrue(HugeRecords.estimateBytes(small) < 256);
        assertTrue(HugeRecords.estimateBytes(NodeFactory.createURI("http://ex.org/" + "p".repeat(1000))) > 2000);
        Node tt = NodeFactory.createTripleTerm(org.apache.jena.graph.Triple.create(
                NodeFactory.createURI("http://ex.org/s"), NodeFactory.createURI("http://ex.org/p"), large));
        assertTrue(HugeRecords.estimateBytes(tt) > HugeRecords.estimateBytes(large), "a triple term counts its components");
        assertTrue(HugeRecords.TERM_ROW_BYTES.applyAsLong(new HugeRecords.TermRow(large, 1)) > 8192);
    }

    @Test
    void sequentialTermSorterSpillsOnTheByteBudget() throws Exception {
        SorterProvider provider = SorterProvider.sequential(1 << 18, 1 << 21, 4, 64 << 10);
        Random rnd = new Random(5);
        List<Node> terms = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            terms.add(i % 7 == 0
                    ? NodeFactory.createLiteralDT(Integer.toString(rnd.nextInt(100)), XSDDatatype.XSDint)
                    : NodeFactory.createLiteralString(big(rnd, 4096)));
        }
        List<Node> expected = new ArrayList<>(terms);
        expected.sort(NodeComparator.INSTANCE);
        try (RecordSorter<HugeRecords.TermRow> sorter = provider.termSorter(dir, "ocol")) {
            for (int i = 0; i < terms.size(); i++) {
                sorter.add(new HugeRecords.TermRow(terms.get(i), i));
            }
            ExternalSorter<HugeRecords.TermRow> es = (ExternalSorter<HugeRecords.TermRow>) sorter;
            assertTrue(es.runsSpilled() >= 20, "300 x 4 KB literals against a 64 KiB budget spill many runs, got " + es.runsSpilled());
            List<Node> actual = new ArrayList<>();
            try (RecordSorter.SortedCursor<HugeRecords.TermRow> c = sorter.sorted()) {
                while (c.hasNext()) actual.add(c.next().term());
            }
            assertEquals(expected, actual, "memoizing comparator + byte-budgeted runs: exact NodeComparator order");
        }
    }

    @Test
    void wholeBuildWithATinyByteBudgetMatchesTheRamWriter() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Random rnd = new Random(9);
        StringBuilder nq = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            nq.append("<http://ex.org/s").append(i % 50).append("> <http://ex.org/p").append(i % 3).append("> \"")
              .append(big(rnd, 3000)).append("\" .\n");
            nq.append("<http://ex.org/s").append(i % 50).append("> <http://ex.org/n> \"").append(rnd.nextInt(1000))
              .append("\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");
        }
        File src = dir.resolve("big.nq").toFile();
        Files.write(src.toPath(), nq.toString().getBytes(StandardCharsets.UTF_8));
        File ram = dir.resolve("big.ram.h5").toFile();
        File huge = dir.resolve("big.huge.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(ram).setSpatial(false).setFeatures(false).build().write();
        HugeHDF5Writer.Builder().setSource(src).setDestination(huge).setSpatial(false).setFeatures(false)
                .setWorkDirectory(Files.createDirectories(dir.resolve("work")))
                // Record caps far above the input: only the byte budget can spill.
                .setTermSpillBatch(1 << 18).setIdSpillBatch(1 << 21).setMergeFanIn(3)
                .setTermSpillBytes(32 << 10)
                .build().write();
        try (BeakGraph a = new BeakGraph(new HDF5Reader(ram));
             BeakGraph b = new BeakGraph(new HDF5Reader(huge))) {
            Model ma = construct(a);
            Model mb = construct(b);
            assertEquals(600, ma.size());
            assertTrue(ma.isIsomorphicWith(mb), "byte-budgeted build must be isomorphic to the RAM writer's");
        }
    }

    private static Model construct(BeakGraph g) {
        try (QueryExecution qe = QueryExecution.dataset(g.getDataset())
                .query(QueryFactory.create("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")).build()) {
            return qe.execConstruct();
        }
    }
}
