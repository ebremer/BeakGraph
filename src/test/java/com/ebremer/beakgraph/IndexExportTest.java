package com.ebremer.beakgraph;

import org.junit.jupiter.api.parallel.Isolated;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.jena.IndexExport;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.DatasetGraph;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The index-walking export (IndexExport) must produce EXACTLY the generic
 * StreamRDF writer's output - compared as line sets, since both emit one
 * self-contained NT/NQ line per quad and only ordering may differ - across
 * every term shape the dictionary stores: IRIs (incl. non-ASCII), plain /
 * lang-tagged / typed literals, numerics of each width, escaping-heavy
 * strings, a long zstd-compressed literal, and blank nodes on both ends.
 * The store is built with VoID statistics so an internal metadata graph
 * exists and must be excluded, exactly as the generic path excludes it.
 */
// Mutates JVM-global state (system properties / ARQ modes / a shared server):
// never interleave with other classes should parallel execution be enabled (BG-189).
@Isolated
class IndexExportTest {

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static DatasetGraph dsg;
    static Dataset truth;

    @BeforeAll
    static void build() throws Exception {
        String trig = """
                @prefix ex: <http://ex.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
                ex:s1 ex:name "plain" .
                ex:s1 ex:langy "hello"@en .
                ex:s1 ex:langy "hallo"@de .
                ex:s1 ex:int "5"^^xsd:int .
                ex:s1 ex:num 42 .
                ex:s1 ex:dbl "4.25"^^xsd:double .
                ex:s1 ex:flt "1.5"^^xsd:float .
                ex:s1 ex:lng "123456789012"^^xsd:long .
                ex:s1 ex:esc "line\\nbreak \\"quoted\\" tab\\t backslash \\\\ end" .
                ex:s1 ex:big "%s" .
                ex:s2 ex:p ex:s1 .
                _:b1 ex:p ex:s1 .
                ex:s1 ex:ref _:b1 .
                ex:s1 ex:uni <http://ex.org/uni/éü> .
                ex:g1 { ex:a ex:p2 ex:b . ex:a ex:shared "in-g1" . }
                ex:g2 { ex:d ex:p3 "x"@fr . _:b2 ex:p3 _:b2 . }
                """.formatted("x".repeat(300));
        File src = dir.resolve("ie.trig").toFile();
        File h5 = dir.resolve("ie.trig.h5").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5)
                .setVoidMode(VoidMode.EXACT) // forces an internal urn:x-beakgraph:void graph
                .setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        dsg = bg.getDataset().asDatasetGraph();
        truth = RDFDataMgr.loadDataset(src.getAbsolutePath());
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static List<String> lines(byte[] bytes) {
        String[] arr = new String(bytes, StandardCharsets.UTF_8).split("\n");
        Arrays.sort(arr);
        return List.of(arr);
    }

    private static byte[] fast(boolean quads) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        long before = IndexExport.HITS.get();
        assertTrue(IndexExport.tryWrite((HDF5Reader) bg.getReader(), os, quads),
                "store must qualify for the index export");
        assertEquals(1, IndexExport.HITS.get() - before);
        return os.toByteArray();
    }

    /** The generic writer's output, exactly as BeakGraphCLI.writeExport produces it. */
    private static byte[] slow(boolean quads) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        StreamRDF stream = StreamRDFWriter.getWriterStream(os, quads ? RDFFormat.NQUADS : RDFFormat.NTRIPLES);
        stream.start();
        if (quads) {
            var it = dsg.find();
            while (it.hasNext()) {
                var q = it.next();
                if (!Params.BGVOID.equals(q.getGraph()) && !Params.SPATIAL.equals(q.getGraph())) {
                    stream.quad(q);
                }
            }
        } else {
            var it = dsg.getDefaultGraph().find();
            while (it.hasNext()) {
                stream.triple(it.next());
            }
        }
        stream.finish();
        return os.toByteArray();
    }

    @Test
    void nqMatchesGenericWriterByteForByte() throws Exception {
        assertEquals(lines(slow(true)), lines(fast(true)),
                "index NQ export must emit exactly the generic writer's lines");
    }

    @Test
    void ntMatchesGenericWriterByteForByte() throws Exception {
        assertEquals(lines(slow(false)), lines(fast(false)),
                "index NT export must emit exactly the generic writer's lines");
    }

    @Test
    void nqRoundTripsIsomorphically() throws Exception {
        Dataset back = DatasetFactory.create();
        RDFDataMgr.read(back, new ByteArrayInputStream(fast(true)), Lang.NQUADS);
        assertTrue(back.getDefaultModel().isIsomorphicWith(truth.getDefaultModel()),
                "default graph must round-trip");
        for (String g : new String[]{"http://ex.org/g1", "http://ex.org/g2"}) {
            assertTrue(back.getNamedModel(g).isIsomorphicWith(truth.getNamedModel(g)),
                    "named graph must round-trip: " + g);
        }
    }

    @Test
    void internalMetadataGraphsAreExcluded() throws Exception {
        String out = new String(fast(true), StandardCharsets.UTF_8);
        assertFalse(out.contains("urn:x-beakgraph"),
                "VoID/spatial metadata must not leak into the export");
        // control: the store really does carry the VoID graph
        assertTrue(dsg.containsGraph(Params.BGVOID), "control: the VoID graph must exist in the store");
    }

    @Test
    void disabledPropertyFallsBackToGenericWriter() throws Exception {
        String old = System.setProperty("beakgraph.export.fastpath", "false");
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            assertFalse(IndexExport.tryWrite((HDF5Reader) bg.getReader(), os, true),
                    "disabled fast path must decline");
            assertEquals(0, os.size(), "a declined fast path must write nothing");
        } finally {
            if (old == null) System.clearProperty("beakgraph.export.fastpath");
            else System.setProperty("beakgraph.export.fastpath", old);
        }
    }
}
