package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-326: the dictionary orders durations by (total months, total seconds).
 * ARQ compares two MIXED durations (a year/month part and a day/time part)
 * by XSD's four-reference-point rule, which is determinate for some pairs
 * the months-first order reverses: "P1M35D" (63..66 days) is greater than
 * "P2M1D" (60..63 days), yet ranks before it. A range hint snapped around
 * such a constant started the scan past the row, and the surrounding FILTER
 * cannot recover a row the scan never produced. Mixed-duration constants
 * are no longer hints; pure year/month or day/time constants still are
 * (same-class values order as XSD does, and ARQ answers every cross-class
 * comparison false).
 */
class DurationPushdownParityTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:lasts "P1M35D"^^xsd:duration .
        ex:b ex:lasts "P2M1D"^^xsd:duration .
        ex:c ex:lasts "P1Y400D"^^xsd:duration .
        ex:d ex:lasts "P2Y1D"^^xsd:duration .
        ex:e ex:lasts "P1Y"^^xsd:duration .
        ex:f ex:lasts "P400D"^^xsd:duration .
        ex:g ex:lasts "P1M"^^xsd:duration .
        ex:h ex:lasts "P2M"^^xsd:duration .
        ex:i ex:lasts "P35D"^^xsd:duration .
        ex:j ex:lasts "P1MT1H"^^xsd:duration .
        ex:multi ex:d1 "P1M35D"^^xsd:duration ; ex:d2 "P2M1D"^^xsd:duration ;
                 ex:d3 "P1Y400D"^^xsd:duration ; ex:d4 "P2Y1D"^^xsd:duration ; ex:d5 "P1M"^^xsd:duration .
        """;
    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    private static final String[] OPS = {">", ">=", "<", "<="};
    private static final String[] MIXED = {"P2M1D", "P1M35D", "P2Y1D", "P1Y400D", "P1MT1H"};
    private static final String[] PURE = {"P1M", "P400D", "P35D", "PT36H"};

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset truth;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("dur.ttl").toFile();
        File h5 = dir.resolve("dur.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
        truth = RDFDataMgr.loadDataset(ttl.getAbsolutePath());
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static List<String> rows(Dataset dataset, String where) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(dataset)
                .query(QueryFactory.create(PREFIX + "SELECT * WHERE { " + where + " }")).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = rs.getResultVars();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : vars) sb.append(v).append('=').append(row.get(v)).append('|');
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static void parity(String pattern, String constant) {
        for (String op : OPS) {
            String where = pattern + " FILTER(?d " + op + " \"" + constant + "\"^^xsd:duration)";
            assertEquals(rows(truth, where), rows(ds, where), where);
        }
    }

    @Test
    void mixedDurationConstantsAnswerLikeArqOnEveryRoute() {
        for (String c : MIXED) {
            parity("?s ex:lasts ?d", c);          // POS: predicate bound
            parity("ex:multi ?p ?d", c);          // SO: subject bound
            parity("?s ?p ?d", c);                // SPO_All: graph only
        }
        for (String c : PURE) {
            parity("?s ex:lasts ?d", c);
            parity("ex:multi ?p ?d", c);
            parity("?s ?p ?d", c);
        }
    }

    @Test
    void onlyPureDurationConstantsAreRangeHints() {
        long before = FilterBounds.HITS.get();
        rows(ds, "?s ex:lasts ?d FILTER(?d > \"P2M1D\"^^xsd:duration)");
        assertEquals(0, FilterBounds.HITS.get() - before, "a mixed-duration constant must not narrow the scan");
        before = FilterBounds.HITS.get();
        rows(ds, "?s ex:lasts ?d FILTER(?d > \"P1M\"^^xsd:duration)");
        assertTrue(FilterBounds.HITS.get() - before >= 1, "a pure year/month constant is still a hint");
    }

    @Test
    void mixedDurationClassification() {
        for (String c : MIXED) {
            assertTrue(FilterBounds.isMixedDuration(NodeFactory.createLiteralDT(c, XSDDatatype.XSDduration)), c);
            assertFalse(FilterBounds.orderAgreesWithArq(NodeFactory.createLiteralDT(c, XSDDatatype.XSDduration)), c);
        }
        for (String c : PURE) {
            assertFalse(FilterBounds.isMixedDuration(NodeFactory.createLiteralDT(c, XSDDatatype.XSDduration)), c);
            assertTrue(FilterBounds.orderAgreesWithArq(NodeFactory.createLiteralDT(c, XSDDatatype.XSDduration)), c);
        }
        assertFalse(FilterBounds.isMixedDuration(NodeFactory.createLiteralDT("P1X", XSDDatatype.XSDduration)), "ill-formed");
        assertFalse(FilterBounds.isMixedDuration(NodeFactory.createLiteralDT("5", XSDDatatype.XSDint)));
    }
}
