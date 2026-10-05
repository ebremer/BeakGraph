package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-54: a FILTER whose two-argument function is NOT an
 * ordering comparison (CONTAINS, STRSTARTS, STRENDS, sameTerm, ...) and whose
 * argument is a pattern variable used to reach the range-pushdown code with a
 * {@code null} operator (Jena's {@code getOpName()} is null for every
 * non-operator function) and crashed the query with a NullPointerException.
 * Every index route is exercised - POS (predicate bound), SO (subject and
 * predicate bound), OS (predicate and object bound) and the SPO full scan -
 * and each answer is checked against Jena's in-memory evaluation of the same
 * query, so the filter is proven to be applied, not merely survived.
 */
class FilterPushdownNonComparisonTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        ex:s1 ex:name "axb" ; ex:value 1 .
        ex:s2 ex:name "ab"  ; ex:value 2 .
        ex:s3 ex:name "hello"@en ; ex:value 3 .
        """;

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("filters.ttl").toFile();
        File h5 = dir.resolve("filters.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new StringReader(TTL), null, Lang.TURTLE);
        reference = DatasetFactory.create(m);
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    /** Sorted string forms of every binding row, so answers compare order-free. */
    private static List<String> rows(Dataset d, String where) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d)
                .query(QueryFactory.create(PREFIX + "SELECT * WHERE { " + where + " }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                rs.getResultVars().forEach(v -> sb.append(v).append('=').append(qs.get(v)).append(' '));
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static void assertSameAsJena(String where) {
        List<String> expected = rows(reference, where);
        assertEquals(expected, rows(ds, where), "query: " + where);
    }

    // --- POS route: predicate bound, subject and object variable --------------

    @Test
    void containsOnObjectVariable() {
        assertSameAsJena("?s ex:name ?n FILTER(CONTAINS(?n, \"x\"))");
        assertEquals(1, rows(ds, "?s ex:name ?n FILTER(CONTAINS(?n, \"x\"))").size());
    }

    @Test
    void constantOnTheLeftSide() {
        // The constant-left form went through flipOp(null) as well.
        assertSameAsJena("?s ex:name ?n FILTER(CONTAINS(\"zaxbz\", ?n))");
        assertEquals(1, rows(ds, "?s ex:name ?n FILTER(CONTAINS(\"zaxbz\", ?n))").size());
    }

    @Test
    void strStartsStrEndsSameTerm() {
        assertSameAsJena("?s ex:name ?n FILTER(STRSTARTS(?n, \"a\"))");
        assertSameAsJena("?s ex:name ?n FILTER(STRENDS(?n, \"b\"))");
        assertSameAsJena("?s ex:name ?n FILTER(sameTerm(?n, \"ab\"))");
        assertEquals(1, rows(ds, "?s ex:name ?n FILTER(sameTerm(?n, \"ab\"))").size());
    }

    @Test
    void langMatchesAndEqualityAreUnaffected() {
        assertSameAsJena("?s ex:name ?n FILTER(langMatches(LANG(?n), \"en\"))");
        assertSameAsJena("?s ex:name ?n FILTER(?n = \"ab\")");
        assertSameAsJena("?s ex:name ?n FILTER(?n != \"ab\")");
    }

    @Test
    void orderingPushdownStillWorksNextToOtherFunctions() {
        assertSameAsJena("?s ex:value ?v FILTER(?v >= 2) . ?s ex:name ?n FILTER(STRSTARTS(?n, \"a\"))");
        assertEquals(1, rows(ds, "?s ex:value ?v FILTER(?v >= 2) . ?s ex:name ?n FILTER(STRSTARTS(?n, \"a\"))").size());
        assertSameAsJena("?s ex:value ?v FILTER(2 <= ?v)");
    }

    // --- SO route: subject and predicate bound --------------------------------

    @Test
    void containsOnSubjectPredicateBoundPattern() {
        assertSameAsJena("ex:s1 ex:name ?n FILTER(CONTAINS(?n, \"x\"))");
        assertEquals(1, rows(ds, "ex:s1 ex:name ?n FILTER(CONTAINS(?n, \"x\"))").size());
        assertEquals(0, rows(ds, "ex:s2 ex:name ?n FILTER(CONTAINS(?n, \"x\"))").size());
    }

    // --- OS route: predicate and object bound, subject variable ---------------

    @Test
    void sameTermOnSubjectVariable() {
        assertSameAsJena("?s ex:name \"axb\" FILTER(sameTerm(?s, ex:s1))");
        assertEquals(1, rows(ds, "?s ex:name \"axb\" FILTER(sameTerm(?s, ex:s1))").size());
        assertEquals(0, rows(ds, "?s ex:name \"ab\" FILTER(sameTerm(?s, ex:s1))").size());
    }

    // --- SPO_All route: predicate variable (full graph scan) ------------------

    @Test
    void strEndsOnFullScan() {
        assertSameAsJena("?s ?p ?n FILTER(STRENDS(?n, \"b\"))");
        assertEquals(2, rows(ds, "?s ?p ?n FILTER(STRENDS(?n, \"b\"))").size());
        assertSameAsJena("?s ?p ?n FILTER(sameTerm(?s, ex:s3))");
    }
}
