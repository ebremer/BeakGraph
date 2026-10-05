package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.lib.CachingNodeComparator;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.huge.HugeRecords;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * BG-136: the -method 4/5 term sort rests on ONE invariant -
 * {@code prefixKey(a) != prefixKey(b)} implies unsigned key order equals
 * NodeComparator order - because the dedup merge and the id join then run
 * on NodeComparator directly. Nothing pinned it: the parity fixtures are
 * ASCII http IRIs. This zoo covers every rank, chars 0x00 / 0x7F / 0xFE /
 * 0xFF / 0x100 / surrogate pairs at every prefix position, prefixes versus
 * their extensions, relative IRIs, the default-graph sentinels, literals of
 * every kind and nested triple terms, plus random strings; and it uses the
 * previously dead FAST_TERM_ORDER.
 */
class UltraSorterProviderTest {

    private static List<Node> zoo() {
        List<Node> nodes = new ArrayList<>();
        nodes.add(Quad.defaultGraphIRI);
        nodes.add(Quad.defaultGraphNodeGenerated);
        String[] specials = {"\u0000", "\u0001", "\u007F", "\u00FE", "\u00FF", "\u0100", "\u540D", "\uD83D\uDC26", "a", "z", " "};
        for (int pos = 0; pos <= 8; pos++) {
            for (String s : specials) {
                String label = "x".repeat(pos) + s;
                nodes.add(NodeFactory.createURI("http://ex.org/" + label));
                nodes.add(NodeFactory.createURI(label));                 // relative
                nodes.add(NodeFactory.createBlankNode("b" + label));
                nodes.add(NodeFactory.createBlankNode(label));
            }
        }
        for (String s : new String[]{"ab", "ab ", "abc", "abcdefg", "abcdefgh", "abcdefghi", "abcdefgH", "ABCDEFGH", "http://ex.org/", "http://ex.org/名前", "urn:x-arq:DefaultGraph", "urn:x-arq:DefaultGraphNode"}) {
            nodes.add(NodeFactory.createURI(s));
            nodes.add(NodeFactory.createBlankNode(s));
        }
        nodes.add(NodeFactory.createLiteralString(""));
        nodes.add(NodeFactory.createLiteralString("plain"));
        nodes.add(NodeFactory.createLiteralString("\u00FF\u0100"));
        nodes.add(NodeFactory.createLiteralLang("plain", "en"));
        nodes.add(NodeFactory.createLiteralLang("plain", "fr"));
        nodes.add(NodeFactory.createLiteralDT("1", XSDDatatype.XSDint));
        nodes.add(NodeFactory.createLiteralDT("1", XSDDatatype.XSDinteger));
        nodes.add(NodeFactory.createLiteralDT("1.5", XSDDatatype.XSDdouble));
        nodes.add(NodeFactory.createLiteralDT("2020-01-02T00:00:00Z", XSDDatatype.XSDdateTime));
        nodes.add(NodeFactory.createLiteralDT("P1D", XSDDatatype.XSDduration));
        nodes.add(NodeFactory.createLiteralDT("[1, 2]", NodeFactory.getType("http://w3id.org/awslabs/neptune/SPARQL-CDTs/List")));
        Node a = NodeFactory.createURI("http://ex.org/a"), b = NodeFactory.createURI("http://ex.org/b");
        nodes.add(NodeFactory.createTripleTerm(Triple.create(a, b, a)));
        nodes.add(NodeFactory.createTripleTerm(Triple.create(a, b, NodeFactory.createTripleTerm(Triple.create(b, a, NodeFactory.createLiteralString("n"))))));
        Random rnd = new Random(42);
        for (int i = 0; i < 2000; i++) {
            StringBuilder sb = new StringBuilder();
            int len = rnd.nextInt(12);
            for (int j = 0; j < len; j++) {
                int c = switch (rnd.nextInt(4)) {
                    case 0 -> rnd.nextInt(0x80);
                    case 1 -> 0x80 + rnd.nextInt(0x80);
                    case 2 -> 0x100 + rnd.nextInt(0x1000);
                    default -> 'a' + rnd.nextInt(3);
                };
                if (Character.isSurrogate((char) c)) c = 'q';
                sb.append((char) c);
            }
            String s = sb.toString();
            switch (rnd.nextInt(3)) {
                case 0 -> nodes.add(NodeFactory.createURI("http://r/" + s));
                case 1 -> nodes.add(NodeFactory.createBlankNode(s.isEmpty() ? "e" : s));
                default -> nodes.add(NodeFactory.createURI(s.isEmpty() ? "r" : s));
            }
        }
        return nodes;
    }

    @Test
    void distinctPrefixKeysAgreeWithNodeComparator() {
        List<Node> nodes = zoo();
        long[] keys = new long[nodes.size()];
        for (int i = 0; i < keys.length; i++) keys[i] = UltraSorterProvider.prefixKey(nodes.get(i));
        int decided = 0;
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = 0; j < nodes.size(); j++) {
                if (keys[i] == keys[j]) continue;
                decided++;
                int byKey = Integer.signum(Long.compareUnsigned(keys[i], keys[j]));
                int full = Integer.signum(NodeComparator.INSTANCE.compare(nodes.get(i), nodes.get(j)));
                assertEquals(full, byKey, "prefix key order must agree with NodeComparator for (" + nodes.get(i) + ", " + nodes.get(j) + ")");
            }
        }
        assertTrue(decided > 1000, "the keys decide most pairs: " + decided);
        // Rank 0 sentinels sort first, the null graph is the default graph.
        assertEquals(UltraSorterProvider.prefixKey(Quad.defaultGraphIRI), UltraSorterProvider.prefixKey(null));
        assertTrue(Long.compareUnsigned(UltraSorterProvider.prefixKey(Quad.defaultGraphIRI),
                UltraSorterProvider.prefixKey(NodeFactory.createBlankNode("a"))) < 0);
    }

    @Test
    void fastTermOrderSortsExactlyLikeTermOrder() {
        List<Node> nodes = zoo();
        List<TermRow> rows = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) rows.add(new TermRow(nodes.get(i), i));
        Collections.shuffle(rows, new Random(7));
        List<TermRow> expected = new ArrayList<>(rows);
        expected.sort(HugeRecords.TERM_ORDER);
        List<TermRow> fast = new ArrayList<>(rows);
        fast.sort(UltraSorterProvider.FAST_TERM_ORDER);
        List<TermRow> memo = new ArrayList<>(rows);
        memo.sort(UltraSorterProvider.fastTermOrder(new CachingNodeComparator(16)));
        assertEquals(terms(expected), terms(fast), "prefix-accelerated order on the shared comparator");
        assertEquals(terms(expected), terms(memo), "prefix-accelerated order on a tiny memo");
    }

    private static List<Node> terms(List<TermRow> rows) {
        List<Node> out = new ArrayList<>(rows.size());
        for (TermRow r : rows) out.add(r.term());
        return out;
    }

    @Test
    void groupedRunFormatRoundTrips() throws Exception {
        List<Node> nodes = zoo();
        List<TermRow> rows = new ArrayList<>();
        Random rnd = new Random(3);
        for (int i = 0; i < nodes.size(); i++) {
            int repeats = 1 + rnd.nextInt(4);
            for (int k = 0; k < repeats; k++) rows.add(new TermRow(nodes.get(i), rnd.nextLong(1L << 40)));
        }
        rows.sort(HugeRecords.TERM_ORDER);
        UltraSorterProvider.GroupedTermFormat format = new UltraSorterProvider.GroupedTermFormat();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            format.writeRun(out, rows.toArray(), rows.size());
        }
        List<TermRow> back = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            ParallelSpillSorter.RunFormat.RunStream<TermRow> stream = format.newStream();
            while (true) {
                try {
                    back.add(stream.read(in));
                } catch (EOFException end) {
                    break;
                }
            }
        }
        assertEquals(rows, back, "(term, row) sequence survives the grouped run format");
    }
}
