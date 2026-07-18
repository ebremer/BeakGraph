package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the composite-literal (cdt:List / cdt:Map) ordering fix in
 * {@link NodeComparator}: composite pairs compare on (datatype IRI, lexical
 * form), never by value.
 *
 * <p>The pre-fix behavior this guards against: well-formed numeric lists
 * compared by VALUE while any pair touching an ill-formed literal fell back to
 * TERM order, and mixing the two is cyclic - verified as
 * {@code [9] < [10] < "[5" < [9]} - so dictionary ids (which ARE comparator
 * ranks) became input-order-dependent: 11 of 12 shuffled sorts of the same
 * literals produced different orders, and lookups could miss stored terms.
 */
class NodeComparatorCdtTest {

    private static final String LIST = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/List";
    private static final String MAP = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map";
    private static final String STRING = "http://www.w3.org/2001/XMLSchema#string";
    private static final String INTEGER = "http://www.w3.org/2001/XMLSchema#integer";

    @TempDir
    static Path dir;

    @org.junit.jupiter.api.BeforeAll
    static void initJena() {
        // Comparator tests touch TypeMapper/NodeValue without building a store
        // first; initialize Jena explicitly so class-init order cannot NPE.
        org.apache.jena.sys.JenaSystem.init();
    }

    private static Node lit(String lex, String dt) {
        return NodeFactory.createLiteralDT(lex, TypeMapper.getInstance().getSafeTypeByName(dt));
    }

    private static int sign(Node a, Node b) {
        return Integer.signum(NodeComparator.INSTANCE.compare(a, b));
    }

    /** The exact trio that used to form a cycle. */
    @Test
    void formerCycleIsNowTransitive() {
        Node nine = lit("[9]", LIST);
        Node ten = lit("[10]", LIST);
        Node illFormed = lit("[5", LIST);
        // One scheme now - lexical: "[10]" < "[5" < "[9]". A consistent chain,
        // where the old value/term mix gave [9] < [10] < "[5" < [9].
        assertEquals(1, sign(nine, ten));
        assertEquals(-1, sign(ten, illFormed));
        assertEquals(-1, sign(illFormed, nine));
    }

    /**
     * Full asymmetry + transitivity sweep over composites (well-formed,
     * ill-formed, nested, blank-node-bearing, List and Map) MIXED with
     * ordinary literals - the mixed pairs stay on the compareAlways path, so
     * the sweep also proves the two schemes cannot disagree into a cycle.
     */
    @Test
    void orderingIsATotalOrderIncludingIllFormed() {
        List<Node> cand = new ArrayList<>();
        for (String lex : new String[]{
                "[1]", "[2]", "[9]", "[10]", "[100]", "[99]", "[5]",
                "[", "[5", "[12,", "[9,",
                "[_:b1]", "[null]", "[\"a\"]", "[<http://ex.org/a>]", "[1, 2]", "[true]"}) {
            cand.add(lit(lex, LIST));
        }
        cand.add(lit("{\"k\": 1}", MAP));
        cand.add(lit("{", MAP));
        cand.add(lit("!!!", STRING));
        cand.add(lit("zzz", STRING));
        cand.add(lit("5", INTEGER));
        cand.add(lit("50", INTEGER));

        int asymmetry = 0;
        int intransitive = 0;
        for (Node a : cand) {
            for (Node b : cand) {
                if (sign(a, b) != -sign(b, a)) {
                    asymmetry++;
                }
            }
        }
        for (Node a : cand) {
            for (Node b : cand) {
                for (Node c : cand) {
                    if (sign(a, b) < 0 && sign(b, c) < 0 && sign(a, c) >= 0) {
                        intransitive++;
                    }
                }
            }
        }
        assertEquals(0, asymmetry, "compare(a,b) must be the negation of compare(b,a)");
        assertEquals(0, intransitive, "a < b < c must imply a < c");
    }

    /** Input order must not leak into the "sorted" order (it did pre-fix: 11/12 seeds disagreed). */
    @Test
    void shuffledSortsAgree() {
        List<Node> pool = new ArrayList<>();
        for (int k = 1; k <= 60; k++) {
            pool.add(lit("[" + k + "]", LIST));
        }
        pool.add(lit("[5", LIST));
        pool.add(lit("[", LIST));
        pool.add(lit("[12,", LIST));
        pool.add(lit("{\"k\": 1}", MAP));

        List<Node> reference = null;
        for (int seed = 0; seed < 12; seed++) {
            List<Node> shuffled = new ArrayList<>(pool);
            Collections.shuffle(shuffled, new Random(seed));
            shuffled.sort(NodeComparator.INSTANCE);
            if (reference == null) {
                reference = shuffled;
            } else {
                assertEquals(reference, shuffled, "sort order depended on input order (seed " + seed + ")");
            }
        }
    }

    @Test
    void compareIsZeroExactlyForIdenticalCompositeTerms() {
        Node a = lit("[1, 2]", LIST);
        Node sameTerm = lit("[1, 2]", LIST);
        Node sameValueDifferentLex = lit("[1,2]", LIST);
        assertEquals(0, sign(a, sameTerm));
        assertTrue(sign(a, sameValueDifferentLex) != 0,
                "CDT has no canonical form: value-equal but lexically distinct literals are distinct terms");
    }

    @Test
    void listsSortBeforeMaps() {
        // Datatype-IRI order (.../List < .../Map), matching compareAlways' value-space rank.
        assertEquals(-1, sign(lit("[1]", LIST), lit("{\"k\": 1}", MAP)));
        assertEquals(-1, sign(lit("[5", LIST), lit("{", MAP)));
    }

    /**
     * End to end: a store containing lists whose VALUE order and LEXICAL order
     * disagree ([9] vs [10] - their dictionary ranks swap under the fix) must
     * round-trip its term set exactly, i.e. the dictionary binary search finds
     * every stored term under the new order. (No ill-formed literal here:
     * RIOT's CDT-aware parser profile rejects those at parse, so they cannot
     * enter a store from a source document at all - pinned by
     * CdtBlankNodeGuardTest#illFormedCompositeIsRejectedByTheParser.)
     */
    @Test
    void storeWithValueLexDisagreeingListsRoundTrips() throws Exception {
        String ttl = """
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            :s :a "[9]"^^cdt:List .
            :s :b "[10]"^^cdt:List .
            :s :d "{\\"k\\": 1}"^^cdt:Map .
            :s :e "plain" .
            """;
        Path src = dir.resolve("cycle.ttl");
        Files.writeString(src, ttl);
        File dest = dir.resolve("cycle.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).build().write();

        Set<String> stored = new TreeSet<>();
        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            bg.getDataset().getDefaultModel().listStatements().forEachRemaining(st -> {
                if (st.getObject().isLiteral()) {
                    Node n = st.getObject().asNode();
                    stored.add(n.getLiteralLexicalForm() + " ^^ " + n.getLiteralDatatypeURI());
                }
            });
        }
        Set<String> expected = new TreeSet<>(Set.of(
            "[9] ^^ " + LIST,
            "[10] ^^ " + LIST,
            "{\"k\": 1} ^^ " + MAP,
            "plain ^^ " + STRING));
        assertEquals(expected, stored);
    }
}
