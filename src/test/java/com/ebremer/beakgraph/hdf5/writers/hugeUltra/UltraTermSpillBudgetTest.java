package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.huge.HugeRecords;
import com.ebremer.beakgraph.huge.RecordSorter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** BG-125 for the -method 4/5 term sorters: the byte budget spills background runs too. */
class UltraTermSpillBudgetTest {

    @TempDir
    Path dir;

    private static String big(Random rnd, int length) {
        StringBuilder sb = new StringBuilder(length);
        sb.append("lit-").append(rnd.nextInt(1_000_000)).append('|');
        while (sb.length() < length) sb.append((char) ('a' + rnd.nextInt(26)));
        return sb.toString();
    }

    @Test
    void ultraTermSorterSpillsOnTheByteBudget() throws Exception {
        ForkJoinPool pool = new ForkJoinPool(2);
        try {
            UltraSorterProvider provider = new UltraSorterProvider(1 << 19, 1 << 22, 4, 64 << 10, pool);
            Random rnd = new Random(3);
            List<Node> terms = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                terms.add(NodeFactory.createLiteralString(big(rnd, 4096)));
            }
            List<Node> expected = new ArrayList<>(terms);
            expected.sort(NodeComparator.INSTANCE);
            try (RecordSorter<HugeRecords.TermRow> sorter = provider.termSorter(dir, "ocol")) {
                for (int i = 0; i < terms.size(); i++) {
                    sorter.add(new HugeRecords.TermRow(terms.get(i), i));
                }
                ParallelSpillSorter<HugeRecords.TermRow> ps = (ParallelSpillSorter<HugeRecords.TermRow>) sorter;
                assertTrue(ps.runsSpilled() >= 20, "300 x 4 KB literals against a 64 KiB budget, got " + ps.runsSpilled() + " runs");
                List<Node> actual = new ArrayList<>();
                try (RecordSorter.SortedCursor<HugeRecords.TermRow> c = sorter.sorted()) {
                    while (c.hasNext()) actual.add(c.next().term());
                }
                assertEquals(expected, actual);
            }
        } finally {
            pool.shutdown();
        }
    }

    @Test
    void wholeBuildWithATinyByteBudgetMatchesTheRamWriter() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Random rnd = new Random(4);
        StringBuilder nq = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            nq.append("<http://ex.org/s").append(i % 40).append("> <http://ex.org/p> \"").append(big(rnd, 3000)).append("\" .\n");
        }
        File src = dir.resolve("big.nq").toFile();
        Files.write(src.toPath(), nq.toString().getBytes(StandardCharsets.UTF_8));
        File ram = dir.resolve("big.ram.h5").toFile();
        File hu = dir.resolve("big.ultra.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(ram).setSpatial(false).setFeatures(false).build().write();
        HugeUltraHDF5Writer.Builder().setSource(src).setDestination(hu).setSpatial(false).setFeatures(false)
                .setWorkDirectory(Files.createDirectories(dir.resolve("work"))).setCores(2)
                .setTermSpillBatch(1 << 19).setIdSpillBatch(1 << 22).setMergeFanIn(3)
                .setTermSpillBytes(32 << 10)
                .build().write();
        try (BeakGraph a = new BeakGraph(new HDF5Reader(ram));
             BeakGraph b = new BeakGraph(new HDF5Reader(hu))) {
            Model ma = construct(a);
            Model mb = construct(b);
            assertEquals(200, ma.size());
            assertTrue(ma.isIsomorphicWith(mb));
        }
    }

    private static Model construct(BeakGraph g) {
        try (QueryExecution qe = QueryExecution.dataset(g.getDataset())
                .query(QueryFactory.create("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")).build()) {
            return qe.execConstruct();
        }
    }
}
