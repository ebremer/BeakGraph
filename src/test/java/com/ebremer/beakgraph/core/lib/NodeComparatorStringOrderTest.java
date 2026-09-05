package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-21: the term order compares strings in UTF-16 code-unit order
 * ({@code String.compareTo}), which SPECIFICATIONS.md §2 and §6 now state
 * (they used to say "code point"). The two orders differ only when a
 * supplementary character (a surrogate pair, D800-DFFF) meets a BMP character
 * in U+E000-U+FFFF: code units put the supplementary character FIRST, code
 * points last. Ids are ranks in this order, so this pins format v5 behaviour
 * for IRIs, blank-node labels, plain and language-tagged literals and
 * composite literals, and looks both spellings up again in a real store.
 */
class NodeComparatorStringOrderTest {

    private static final NodeComparator CMP = NodeComparator.INSTANCE;
    private static final String SUPPLEMENTARY = "\uD83D\uDE00";   // U+1F600, a surrogate pair
    private static final String HIGH_BMP = "\uFF01";              // U+FF01, above the surrogate range
    private static final String CDT_LIST = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/List";

    private static void assertCodeUnitOrder(Node supplementary, Node highBmp) {
        assertTrue(supplementary.toString().codePointAt(supplementary.toString().length() - 2) > 0xFFFF
                || supplementary.toString().contains(SUPPLEMENTARY), "fixture holds a supplementary character");
        assertTrue(CMP.compare(supplementary, highBmp) < 0,
                "UTF-16 code-unit order: the surrogate pair sorts before U+FF01: " + supplementary + " vs " + highBmp);
        assertTrue(CMP.compare(highBmp, supplementary) > 0, "antisymmetric");
        // The documented difference from code-point order, made explicit.
        assertTrue(SUPPLEMENTARY.compareTo(HIGH_BMP) < 0, "String.compareTo puts the surrogate first");
        assertTrue(Integer.compare(SUPPLEMENTARY.codePointAt(0), HIGH_BMP.codePointAt(0)) > 0, "code-point order would put it last");
    }

    @Test
    void irisBlankNodesAndLiteralsUseCodeUnitOrder() {
        assertCodeUnitOrder(NodeFactory.createURI("http://ex/" + SUPPLEMENTARY), NodeFactory.createURI("http://ex/" + HIGH_BMP));
        assertCodeUnitOrder(NodeFactory.createBlankNode("b" + SUPPLEMENTARY), NodeFactory.createBlankNode("b" + HIGH_BMP));
        assertCodeUnitOrder(NodeFactory.createLiteralString("x" + SUPPLEMENTARY), NodeFactory.createLiteralString("x" + HIGH_BMP));
        assertCodeUnitOrder(NodeFactory.createLiteralLang("x" + SUPPLEMENTARY, "en"), NodeFactory.createLiteralLang("x" + HIGH_BMP, "en"));
        assertCodeUnitOrder(NodeFactory.createLiteralDT("[\"" + SUPPLEMENTARY + "\"]", NodeFactory.getType(CDT_LIST)),
                NodeFactory.createLiteralDT("[\"" + HIGH_BMP + "\"]", NodeFactory.getType(CDT_LIST)));
        // The exact-term tie-break (§6.3) on lexical forms follows the same order.
        assertCodeUnitOrder(NodeFactory.createLiteralDT(SUPPLEMENTARY, XSDDatatype.XSDstring), NodeFactory.createLiteralDT(HIGH_BMP, XSDDatatype.XSDstring));
    }

    @TempDir
    Path dir;

    private static int count(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void bothSpellingsAreStoredAndFoundAgain() throws Exception {
        String ttl = "@prefix ex: <http://ex.org/> .\n"
                + "ex:a ex:p <http://ex.org/" + SUPPLEMENTARY + "> .\n"
                + "ex:b ex:p <http://ex.org/" + HIGH_BMP + "> .\n"
                + "ex:c ex:q \"" + SUPPLEMENTARY + "\" .\n"
                + "ex:d ex:q \"" + HIGH_BMP + "\" .\n"
                + "ex:e ex:q \"" + SUPPLEMENTARY + "\"@en .\n"
                + "ex:f ex:q \"" + HIGH_BMP + "\"@en .\n"
                + "ex:g ex:q \"\\uE000\" .\n";
        File src = dir.resolve("order.ttl").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        File h5 = dir.resolve("order.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph bg = BG.getBeakGraph(h5)) {
            Dataset ds = bg.getDataset();
            String pre = "PREFIX ex: <http://ex.org/> ";
            assertEquals(7, count(ds, pre + "SELECT * WHERE { ?s ?p ?o }"));
            assertEquals(1, count(ds, pre + "SELECT ?s WHERE { ?s ex:p <http://ex.org/" + SUPPLEMENTARY + "> }"));
            assertEquals(1, count(ds, pre + "SELECT ?s WHERE { ?s ex:p <http://ex.org/" + HIGH_BMP + "> }"));
            assertEquals(1, count(ds, pre + "SELECT ?s WHERE { ?s ex:q \"" + SUPPLEMENTARY + "\" }"));
            assertEquals(1, count(ds, pre + "SELECT ?s WHERE { ?s ex:q \"" + HIGH_BMP + "\" }"));
            assertEquals(1, count(ds, pre + "SELECT ?s WHERE { ?s ex:q \"" + SUPPLEMENTARY + "\"@en }"));
            assertEquals(1, count(ds, pre + "SELECT ?s WHERE { ?s ex:q \"" + HIGH_BMP + "\"@en }"));
            assertEquals(1, count(ds, pre + "SELECT ?s WHERE { ?s ex:q \"\\uE000\" }"));
            // Range filters over these agree with ARQ (which also compares by code unit).
            assertEquals(2, count(ds, pre + "SELECT ?s WHERE { ?s ex:q ?o FILTER(?o < \"" + HIGH_BMP + "\") }"),
                    "the surrogate-pair string and U+E000 both sort below U+FF01");
        }
    }
}
