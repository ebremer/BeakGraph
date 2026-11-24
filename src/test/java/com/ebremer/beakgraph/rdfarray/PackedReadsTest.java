package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.jena.graph.Node;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Packed reads (RDFARRY.md §3.6): a sequence a tensor or ragged class holds
 * reads its elements from {@code /.BG/arrays}, and every term is the one its
 * canonical elements give - lexical form, datatype, slots, inner levels - in
 * both containers, raw and compressed. With {@code beakgraph.arrays.read=false}
 * a reader reads canonically only.
 */
class PackedReadsTest {

    @TempDir
    Path dir;

    private File build(String name, SequenceArrays.Options o) throws Exception {
        Path src = dir.resolve("packed.ttl");
        Files.writeString(src, DiskEngineArraysTest.data());
        File out = dir.resolve(name).toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).setArrays(o).build().write();
        return out;
    }

    /** Opens {@code store} with packed reads on or off. */
    private static BeakGraph open(File store, boolean packed) throws Exception {
        String before = System.getProperty("beakgraph.arrays.read");
        System.setProperty("beakgraph.arrays.read", Boolean.toString(packed));
        try {
            return BG.getBeakGraph(store);
        } finally {
            if (before == null) {
                System.clearProperty("beakgraph.arrays.read");
            } else {
                System.setProperty("beakgraph.arrays.read", before);
            }
        }
    }

    /** The same term in every part: the same kind, lexical form, datatype and language, level by level. */
    private static void assertSameTerm(Node expected, Node actual, String where) {
        assertEquals(expected, actual, where);
        assertEquals(expected.hashCode(), actual.hashCode(), where);
        if (expected.isNodeSeq()) {
            List<Node> e = expected.getSeq();
            List<Node> a = actual.getSeq();
            assertEquals(e.size(), a.size(), where);
            for (int i = 0; i < e.size(); i++) {
                assertSameTerm(e.get(i), a.get(i), where + "[" + i + "]");
            }
        } else if (expected.isLiteral()) {
            assertEquals(expected.getLiteralLexicalForm(), actual.getLiteralLexicalForm(), where);
            assertEquals(expected.getLiteralDatatypeURI(), actual.getLiteralDatatypeURI(), where);
            assertEquals(expected.getLiteralLanguage(), actual.getLiteralLanguage(), where);
        } else {
            assertEquals(expected.isNodeUndef(), actual.isNodeUndef(), where);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"raw.h5", "raw.zarr", "compressed.h5", "compressed.zarr"})
    void everyPackedSequenceReadsAsItsCanonicalElements(String name) throws Exception {
        File store = build(name, new SequenceArrays.Options(1, 2, name.startsWith("compressed")));
        int packed = 0;
        try (BeakGraph fromArrays = open(store, true); BeakGraph canonical = open(store, false)) {
            PositionalDictionaryReader a = (PositionalDictionaryReader) fromArrays.getReader().getDictionary();
            PositionalDictionaryReader c = (PositionalDictionaryReader) canonical.getReader().getDictionary();
            for (long id = a.firstSequenceObjectId(); id <= a.lastSequenceObjectId(); id++) {
                Node viaArrays = a.getObjects().extract(id);
                Node viaElements = c.getObjects().extract(id);
                assertTrue(viaElements.getSeq().getClass().getSimpleName().equals("SequenceRow"), "canonical: " + id);
                if (viaArrays.getSeq().getClass().getSimpleName().equals("PackedRow")) {
                    packed++;
                }
                assertSameTerm(viaElements, viaArrays, "sequence " + id);
                assertSameTerm(viaElements, a.extractCanonicalSequence(id), "canonical read of " + id);
            }
        }
        // The rosters, matrices, polygons, flags and the other tensors and ragged rows: not the records.
        assertTrue(packed >= 9, packed + " sequences read from the arrays");
    }

    @Test
    void aMatrixRowIsAViewOverItsValues() throws Exception {
        File store = build("matrix.h5", new SequenceArrays.Options(1, 2, false));
        try (BeakGraph bg = open(store, true)) {
            PositionalDictionaryReader d = (PositionalDictionaryReader) bg.getReader().getDictionary();
            int matrices = 0;
            for (long id = d.firstSequenceObjectId(); id <= d.lastSequenceObjectId(); id++) {
                Node n = d.getObjects().extract(id);
                if (n.getSeq().getClass().getSimpleName().equals("PackedRow") && n.getSeq().get(0).isNodeSeq()) {
                    // An inner level is read from the same row of values, not from the dictionary.
                    assertEquals("PackedRow", n.getSeq().get(0).getSeq().getClass().getSimpleName());
                    matrices++;
                }
            }
            assertTrue(matrices >= 3, matrices + " matrices or point lists");
        }
    }

    @Test
    void queriesAnswerTheSameEitherWay() throws Exception {
        File store = build("query.zarr", new SequenceArrays.Options(1, 2, true));
        String q = """
            PREFIX : <http://ex.org/>
            SELECT ?s ?p ?seq WHERE { ?s ?p ?seq FILTER(isSEQ(?seq)) } ORDER BY ?s ?p
            """;
        try (BeakGraph fromArrays = open(store, true); BeakGraph canonical = open(store, false)) {
            List<String> a = rows(fromArrays, q);
            List<String> c = rows(canonical, q);
            assertTrue(a.size() > 20, a.toString());
            assertEquals(c, a);
        }
    }

    private static List<String> rows(BeakGraph bg, String q) {
        List<String> out = new java.util.ArrayList<>();
        try (org.apache.jena.query.QueryExecution qe = org.apache.jena.query.QueryExecutionFactory.create(q, bg.getDataset())) {
            qe.execSelect().forEachRemaining(s -> out.add(s.toString()));
        }
        return out;
    }
}
