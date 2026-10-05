package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.SimpleNodeTable;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The readers' container-profile checks and open-time invariants: what a
 * foreign or damaged file is told, instead of a ClassCastException or a
 * NullPointerException at open (BG-88, BG-281) or a context-free failure
 * deep inside a later query (BG-81, BG-82); plus the reader lifecycle
 * details the review found half-implemented (BG-340, BG-287, BG-84, BG-368).
 */
class ReaderProfileAndInvariantsTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> ";
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:s1 ex:p ex:o1 ; ex:n 1 ; ex:tag "hello"@EN-us ; ex:tag "hola"@es .
        ex:s2 ex:p ex:o2 ; ex:n 2 ; ex:long "%s" .
        ex:g1 { ex:s1 ex:p ex:o1 . ex:s3 ex:q "in g1" . }
        ex:g2 { ex:s1 ex:p ex:o1 . ex:s4 ex:q "in g2" . }
        """.formatted("z".repeat(200));
    private static final String TRIPLE_TERMS = """
        @prefix : <http://ex.org/> .
        :r :says <<( :a :b :c )>> .
        :r :says <<( :a :b :d )>> .
        :r :n 7 .
        """;

    @TempDir
    static Path dir;
    static File plain;
    static File withTripleTerms;

    @BeforeAll
    static void build() throws Exception {
        plain = build("plain.trig", TRIG);
        withTripleTerms = build("tt.ttl", TRIPLE_TERMS);
    }

    private static File build(String name, String text) throws Exception {
        Path src = dir.resolve(name);
        Files.writeString(src, text);
        File h5 = dir.resolve(name + ".h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        return h5;
    }

    private static List<String> rows(File h5, String query) throws Exception {
        List<String> out = new ArrayList<>();
        try (BeakGraph bg = BG.getBeakGraph(h5);
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
                out.add(sb.toString());
            }
        }
        out.sort(null);
        return out;
    }

    private static final String ALL = "SELECT ?g ?s ?p ?o WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }";

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null) t = t.getCause();
        return t.getMessage();
    }

    // --- BG-88 -----------------------------------------------------------

    @Test
    void aFileWithoutTheDictionaryGroupIsNotABeakGraphFile() throws Exception {
        Path f = dir.resolve("nodict.h5");
        try (WritableHdfFile w = HdfFile.write(f)) {
            w.putGroup(Params.BG).putAttribute(Params.FORMAT_VERSION_ATTR, Params.FORMAT_VERSION);
        }
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> new HDF5Reader(f.toFile()));
        assertTrue(ex.getMessage().contains("Not a BeakGraph file") && ex.getMessage().contains(Params.DICTIONARY), ex.getMessage());
        Files.delete(f); // the failed open released the file
    }

    // --- BG-281 ----------------------------------------------------------

    @Test
    void integerAttributesOfAnyWidthAreAccepted() throws Exception {
        // Swap every integer attribute's width: 64-bit counts become 32-bit
        // where they fit, 32-bit widths become 64-bit - what an h5py writer does.
        File swapped = StoreCopies.copy(plain, dir.resolve("swapped.h5").toFile(), (path, attr, value) -> {
            if (value instanceof Long l && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) return (int) (long) l;
            if (value instanceof Integer i) return (long) i;
            return value;
        });
        assertEquals(rows(plain, ALL), rows(swapped, ALL), "the store reads identically through narrower and wider attributes");
    }

    @Test
    void aNonIntegerAttributeIsReportedByName() throws Exception {
        File bad = StoreCopies.copy(plain, dir.resolve("badattr.h5").toFile(), (path, attr, value) ->
                (path.endsWith("/dictionary/entities/offsets") && attr.equals(Params.NUM_ENTRIES)) ? new long[]{1, 2} : value);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> new HDF5Reader(bad));
        String m = rootMessage(ex);
        assertTrue(m.contains(Params.NUM_ENTRIES) && m.contains("offsets") && m.contains("scalar integer"), m);
    }

    // --- BG-81 -----------------------------------------------------------

    @Test
    void anIndexLevelWhoseColumnsDifferInLengthFailsTheOpenOfThatIndex() throws Exception {
        File bad = StoreCopies.copy(plain, dir.resolve("shortbitmap.h5").toFile(), (path, attr, value) ->
                (path.endsWith("/GSPO/Bs") && attr.equals(Params.NUM_ENTRIES)) ? ((Number) value).longValue() - 1 : value);
        try (HDF5Reader reader = new HDF5Reader(bad)) {
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> reader.getIndexReader(Index.GSPO));
            String m = rootMessage(ex);
            assertTrue(m.contains("Bs has") && m.contains("rows but Ss has"), m);
        }
    }

    @Test
    void fcdAttributesMustDescribeEachOther() throws Exception {
        File zeroBlock = StoreCopies.copy(plain, dir.resolve("blocksize0.h5").toFile(), (path, attr, value) ->
                (path.endsWith("/dictionary/literals/strings") && attr.equals(Params.BLOCK_SIZE)) ? 0 : value);
        String m = rootMessage(assertThrows(IllegalStateException.class, () -> new HDF5Reader(zeroBlock)));
        assertTrue(m.contains("blockSize 0"), m);

        File extraBlock = StoreCopies.copy(plain, dir.resolve("numblocks.h5").toFile(), (path, attr, value) ->
                (path.endsWith("/dictionary/literals/strings") && attr.equals(Params.NUM_BLOCKS)) ? ((Number) value).longValue() + 1 : value);
        m = rootMessage(assertThrows(IllegalStateException.class, () -> new HDF5Reader(extraBlock)));
        assertTrue(m.contains("blocks but") && m.contains("entries in blocks of"), m);
    }

    // --- BG-82 -----------------------------------------------------------

    @Test
    void theTripleTermStoreMustMatchTheDatatypeSuffix() throws Exception {
        assertEquals(2, rows(withTripleTerms, "SELECT ?t WHERE { ?r ex:says ?t }").size(), "fixture: two triple terms");
        File bad = StoreCopies.copy(withTripleTerms, dir.resolve("ttshort.h5").toFile(), (path, attr, value) ->
                (path.endsWith("/dictionary/literals/tripleTerms") && attr.equals(Params.NUM_ENTRIES)) ? ((Number) value).longValue() - 3 : value);
        String m = rootMessage(assertThrows(IllegalStateException.class, () -> new HDF5Reader(bad)));
        assertTrue(m.contains("tripleTerms") && m.contains("TRIPLE_TERM suffix"), m);
    }

    // --- BG-340 ----------------------------------------------------------

    @Test
    void aNullBindingReadsLikeAnEmptyOne() throws Exception {
        try (HDF5Reader reader = new HDF5Reader(plain)) {
            Triple pattern = Triple.create(Var.alloc("s"), NodeFactory.createURI("http://ex.org/p"), Var.alloc("o"));
            assertEquals(count(reader.read(Quad.defaultGraphIRI, new BindingNodeId(), pattern, null, reader.getNodeTable())),
                    count(reader.read(Quad.defaultGraphIRI, null, pattern, null, reader.getNodeTable())));
            assertEquals(count(reader.read(Quad.unionGraph, new BindingNodeId(), pattern, null, reader.getNodeTable())),
                    count(reader.read(Quad.unionGraph, null, pattern, null, reader.getNodeTable())));
            assertTrue(count(reader.read(Quad.defaultGraphIRI, null, pattern, null, reader.getNodeTable())) > 0);
        }
    }

    private static int count(Iterator<?> it) {
        int n = 0;
        while (it.hasNext()) { it.next(); n++; }
        return n;
    }

    // --- BG-287 ----------------------------------------------------------

    @Test
    void closeReleasesTheNodeTableCaches() throws Exception {
        HDF5Reader reader = new HDF5Reader(plain);
        SimpleNodeTable table = (SimpleNodeTable) reader.getNodeTable();
        try (BeakGraph bg = new BeakGraph(reader);
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(QueryFactory.create(PREFIX + ALL)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                for (String v : rs.getResultVars()) qs.get(v); // materialize every term through the node table
            }
            assertTrue(table.cachedEntries() > 0, "a materialized result populates the node cache");
        }
        assertFalse(reader.isOpen());
        assertEquals(0, table.cachedEntries(), "a closed reader drops its cached nodes");
    }

    // --- BG-84 -----------------------------------------------------------

    @Test
    void aPreV3StoreLogsAndRunsOnTheLinearSelectFallback() throws Exception {
        File v2 = StoreCopies.copy(plain, dir.resolve("v2.h5").toFile(), (path, attr, value) ->
                (path.endsWith("/" + Params.BG) && attr.equals(Params.FORMAT_VERSION_ATTR)) ? 2 : value);
        List<String> warnings = new CopyOnWriteArrayList<>();
        Logger log = (Logger) LogManager.getLogger(HDF5Reader.class);
        AbstractAppender capture = new AbstractAppender("bg-test-capture", null, null, true, null) {
            @Override public void append(LogEvent event) {
                if (event.getLevel().isMoreSpecificThan(Level.WARN)) warnings.add(event.getMessage().getFormattedMessage());
            }
        };
        capture.start();
        Level before = log.getLevel();
        Configurator.setLevel(HDF5Reader.class.getName(), Level.WARN);
        log.addAppender(capture);
        try (HDF5Reader reader = new HDF5Reader(v2)) {
            assertEquals(2, reader.getFormatVersion());
            assertNotNull(reader.getIndexReader(Index.GSPO).getBitmapBuffer('S'));
            assertNull(reader.getIndexReader(Index.GSPO).getDirectory('S'), "the v2 directory is ignored: lookups use the linear select1");
        } finally {
            log.removeAppender(capture);
            capture.stop();
            Configurator.setLevel(HDF5Reader.class.getName(), before);
        }
        assertTrue(warnings.stream().anyMatch(w -> w.contains("format v2") && w.contains("linear select1")),
                "the fallback is logged: " + warnings);
        assertEquals(rows(plain, ALL), rows(v2, ALL), "the linear fallback answers exactly what the directory does");
    }

    // --- BG-368 ----------------------------------------------------------

    @Test
    void languageTagsAreStoredInFormattedFormAndChecked() throws Exception {
        assertEquals("en-US", MultiTypeDictionaryReader.requireFormattedTag("en-US", "literals"));
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> MultiTypeDictionaryReader.requireFormattedTag("en-us", "literals"));
        assertTrue(ex.getMessage().contains("'en-us'") && ex.getMessage().contains("'en-US'"), ex.getMessage());
        // The source wrote "hello"@EN-us; the store holds Jena's formatted tag and
        // a query with the same raw tag finds it (both sides are formatted).
        List<String> tags = rows(plain, "SELECT ?o WHERE { ex:s1 ex:tag ?o FILTER(lang(?o) = \"en-US\") }");
        assertEquals(List.of("o=\"hello\"@en-US "), tags);
        assertEquals(1, rows(plain, "SELECT ?s WHERE { ?s ex:tag \"hello\"@EN-us }").size());
    }
}
