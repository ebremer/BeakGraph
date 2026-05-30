package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Validates that the query path uses the accelerated rank/select directory
 * ({@link HDTBitmapDirectory}) and that it agrees exactly with the linear
 * {@code select1} it replaces, including the superblock-width fix that keeps the
 * directory's cumulative counts from overflowing (issue 2.1 / 2.2).
 */
class RankSelectDirectoryTest {

    @TempDir
    Path dir;

    private File build(int n, boolean uniquePredicates) throws Exception {
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n");
        for (int i = 0; i < n; i++) {
            String p = uniquePredicates ? ("ex:p" + i) : "ex:p";
            ttl.append("ex:s").append(i).append(' ').append(p).append(" ex:o").append(i).append(" .\n");
        }
        File t = dir.resolve("rs" + n + (uniquePredicates ? "u" : "") + ".ttl").toFile();
        File h5 = new File(t.getParentFile(), t.getName() + ".h5");
        Files.write(t.toPath(), ttl.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(t).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        return h5;
    }

    private static Set<String> objectsOf(Dataset ds, String subject, String predicate) {
        Set<String> objs = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "PREFIX ex: <http://ex.org/> SELECT ?o WHERE { " + subject + " " + predicate + " ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) objs.add(rs.next().getResource("o").getURI());
        }
        return objs;
    }

    @Test
    void directorySelect1MatchesLinearScanAcrossSuperblocks() throws Exception {
        File h5 = build(700, false); // bitmaps span multiple 512-bit superblocks
        try (HDF5Reader r = new HDF5Reader(h5)) {
            IndexReader ir = r.getIndexReader(Index.GSPO);
            for (char c : new char[]{'S', 'P', 'O'}) {
                BitPackedUnSignedLongBuffer b = ir.getBitmapBuffer(c);
                HDTBitmapDirectory d = ir.getDirectory(c);
                assertNotNull(d, "v" + Params.FORMAT_VERSION + " files must build the rank/select directory for " + c);
                assertTrue(b.getNumEntries() > 512, "component " + c + " should span >1 superblock; was " + b.getNumEntries());
                for (long rank = 1; rank <= b.getNumEntries(); rank++) {
                    long lin = b.select1(rank);
                    assertEquals(lin, d.select1(rank), "select1 mismatch at rank " + rank + " component " + c);
                    if (lin == -1) break;
                }
            }
        }
    }

    @Test
    void fullScanRoundTripsManyTriples() throws Exception {
        int n = 700;
        Dataset ds = new BeakGraph(new HDF5Reader(build(n, false))).getDataset();
        try {
            assertEquals(n, ds.getDefaultModel().size());
            // a subject deep in the index resolves to exactly its own object
            assertEquals(Set.of("http://ex.org/o500"), objectsOf(ds, "ex:s500", "ex:p"));
            assertEquals(Set.of("http://ex.org/o0"), objectsOf(ds, "ex:s0", "ex:p"));
            assertEquals(Set.of("http://ex.org/o699"), objectsOf(ds, "ex:s699", "ex:p"));
        } finally {
            ds.close();
        }
    }

    @Test
    void superblockWidthHandlesEntitySpaceExceedingQuadCount() throws Exception {
        // 127 parsed triples keep the OLD superblock width at 8 bits (max 255), but unique
        // s/p/o plus the generated VOID metadata push the entity space - and thus the
        // cumulative set-bit counts the directory relies on - well past 255. With the
        // directory now driving navigation, an undersized superblock silently returns
        // wrong positions; the widened superblock keeps results correct.
        int n = 127;
        File h5 = build(n, true);

        try (HDF5Reader r = new HDF5Reader(h5)) {
            long bits = r.getIndexReader(Index.GSPO).getBitmapBuffer('P').getNumEntries();
            assertTrue(bits > 255, "scenario must exceed the old 8-bit superblock max to be meaningful; was " + bits);
        }

        Dataset ds = new BeakGraph(new HDF5Reader(h5)).getDataset();
        try {
            assertEquals(n, ds.getDefaultModel().size());
            for (int i : new int[]{0, 63, 64, 100, 126}) {
                assertEquals(Set.of("http://ex.org/o" + i), objectsOf(ds, "ex:s" + i, "ex:p" + i),
                        "triple " + i + " should round-trip through the accelerated directory");
            }
        } finally {
            ds.close();
        }
    }
}
