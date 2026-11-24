/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Adapted for BeakGraph from Apache Jena's rdf-array branch (Apache License
 * 2.0, the ASF header above retained): the cases and assertions are the
 * fork's; the data are BeakGraph stores built from the same documents, and
 * the queries run through each store's dataset.
 */

package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.junit.jupiter.api.Test;

/**
 * The empty slot of a sequence, {@code undef}: in data it holds a position and no
 * value; a variable that meets it stays unbound, as with {@code UNDEF} in VALUES.
 */
class UndefSlotsStoreTest {

    @org.junit.jupiter.api.io.TempDir
    static java.nio.file.Path storeDir;
    private static RdfArrayStores STORES;

    @org.junit.jupiter.api.BeforeAll
    static void openStores() {
        STORES = new RdfArrayStores(storeDir);
    }

    @org.junit.jupiter.api.AfterAll
    static void closeStores() throws Exception {
        STORES.closeAll();
    }

    private static final String PREFIX = "PREFIX : <http://example/>\n";
    private static final String DATA = PREFIX
            + ":x :p (1 undef 3) .\n"
            + ":y :p (1 2 3) .\n"
            + ":z :p (undef undef) .\n"
            + ":v :w 5 .\n";

    private static Graph parse(String data) {
        return STORES.graph(PREFIX+data, Lang.TURTLE);
    }

    private static Graph data() {
        return STORES.graph(DATA, Lang.TURTLE);
    }

    private static Node uri(String local) { return NodeFactory.createURI("http://example/"+local); }
    private static Node i(int i) { return NodeFactory.createLiteralDT(Integer.toString(i), XSDDatatype.XSDinteger); }
    private static Node seq(Node... elts) { return NodeFactory.createSeqNode(elts); }

    private static List<Binding> exec(Graph graph, String queryString) {
        Query query = QueryFactory.create(PREFIX+queryString);
        List<Binding> results = new ArrayList<>();
        try ( QueryExec qExec = RdfArrayStores.exec(graph).query(query).build() ) {
            RowSet rs = qExec.select();
            rs.forEachRemaining(results::add);
        }
        return results;
    }

    private static List<Binding> exec(String queryString) {
        return exec(data(), queryString);
    }

    private static Binding only(String queryString) {
        List<Binding> r = exec(queryString);
        assertEquals(1, r.size(), r.toString());
        return r.get(0);
    }

    private static Binding ofSubject(List<Binding> r, String local) {
        for ( Binding b : r ) {
            if ( uri(local).equals(b.get("s")) )
                return b;
        }
        return null;
    }

    // ---- Data

    @Test public void turtle_slot() {
        assertTrue(data().contains(uri("x"), uri("p"), seq(i(1), Node.UNDEF, i(3))));
    }

    @Test public void turtle_case_insensitive_as_in_sparql() {
        assertTrue(parse(":s :p (UNDEF) .").contains(uri("s"), uri("p"), seq(Node.UNDEF)));
    }

    @org.junit.jupiter.api.Disabled("a sequence subject: refused by BeakGraph (rdf-array data model of 2026-11-03; SPECIFICATIONS.md 9.5); see sequenceSubjectsAreRefused")
    @Test public void turtle_nested_and_subject() {
        Graph g = parse("(undef (undef 1)) :p 2 .");
        assertTrue(g.contains(seq(Node.UNDEF, seq(Node.UNDEF, i(1))), uri("p"), i(2)));
    }

    @Test public void turtle_prefixed_name_is_not_a_slot() {
        Graph g = STORES.graph("PREFIX undef: <http://example/> undef:s undef:p (undef:o undef) .", Lang.TURTLE);
        assertTrue(g.contains(uri("s"), uri("p"), seq(uri("o"), Node.UNDEF)));
    }

    @Test public void turtle_slot_only_in_a_sequence() {
        assertThrows(RiotException.class, ()->parse(":s :p undef ."));
        assertThrows(RiotException.class, ()->parse("undef :p 1 ."));
    }

    @Test public void ntriples_slot() {
        Graph g = STORES.graph("<http://example/s> <http://example/p> ( \"a\" undef ( undef ) ) .", Lang.NTRIPLES);
        Node a = NodeFactory.createLiteralString("a");
        assertTrue(g.contains(uri("s"), uri("p"), seq(a, Node.UNDEF, seq(Node.UNDEF))));
    }

    @Test public void ntriples_slot_only_in_a_sequence() {
        assertThrows(RiotException.class, ()->STORES.graph("<http://example/s> <http://example/p> undef .", Lang.NTRIPLES));
    }

    private static void roundTrip(RDFFormat format, Lang lang) {
        Graph g = data();
        StringWriter w = new StringWriter();
        RDFDataMgr.write(w, g, format);
        assertTrue(w.toString().contains("undef"), w.toString());
        Graph g2 = RDFParser.fromString(w.toString(), lang).toGraph();
        assertTrue(g.isIsomorphicWith(g2), w.toString());
    }

    @Test public void write_read_turtle()    { roundTrip(RDFFormat.TURTLE_PRETTY, Lang.TURTLE); }
    @Test public void write_read_ntriples()  { roundTrip(RDFFormat.NTRIPLES, Lang.NTRIPLES); }

    @Test public void isomorphism() {
        assertTrue(parse(":s :p (_:a undef) .").isIsomorphicWith(parse(":s :p (_:b undef) .")));
        assertFalse(parse(":s :p (_:a undef) .").isIsomorphicWith(parse(":s :p (_:b 1) .")));
        assertFalse(parse(":s :p (1 undef) .").isIsomorphicWith(parse(":s :p (1 1) .")));
    }

    @Test public void sequenceSubjectsAreRefused() {
        // BeakGraph's reading of turtle_nested_and_subject: the build fails on the subject guard.
        IllegalStateException e = assertThrows(IllegalStateException.class, ()->parse("(undef (undef 1)) :p 2 ."));
        assertTrue(e.getMessage().startsWith("Unexpected subject node type"), e.getMessage());
    }

    // ---- Patterns

    @Test public void variable_at_a_slot_stays_unbound() {
        List<Binding> r = exec("SELECT * { ?s :p (?a ?b ?c) }");
        assertEquals(2, r.size());
        Binding x = ofSubject(r, "x");
        assertEquals(i(1), x.get("a"));
        assertNull(x.get("b"));
        assertEquals(i(3), x.get("c"));
        assertEquals(i(2), ofSubject(r, "y").get("b"));
    }

    @Test public void length_still_counts() {
        assertEquals(0, exec("SELECT * { :x :p (?a ?b) }").size());
    }

    @Test public void undef_in_a_pattern_matches_only_a_slot() {
        assertEquals(uri("x"), only("SELECT ?s { ?s :p (?a undef ?c) }").get("s"));
        assertEquals(uri("x"), only("SELECT ?s { ?s :p (1 undef 3) }").get("s"));
    }

    @Test public void constant_does_not_match_a_slot() {
        assertEquals(uri("y"), only("SELECT ?s { ?s :p (1 2 3) }").get("s"));
        assertEquals(uri("y"), only("SELECT ?s { ?s :p (1 2 ?c) }").get("s"));
    }

    @Test public void variable_may_be_bound_at_a_slot_within_a_bgp() {
        // Compatible, as a join of the two triple patterns. :y has 2, not 5.
        for ( String q : List.of("SELECT * { :v :w ?b . ?s :p (?a ?b ?c) }",
                                 "SELECT * { ?s :p (?a ?b ?c) . :v :w ?b }") ) {
            Binding b = only(q);
            assertEquals(uri("x"), b.get("s"));
            assertEquals(i(5), b.get("b"));
        }
    }

    @Test public void variable_may_be_bound_at_a_slot_across_a_join() {
        for ( String q : List.of("SELECT * { VALUES ?b { 5 } ?s :p (?a ?b ?c) }",
                                 "SELECT * { ?s :p (?a ?b ?c) VALUES ?b { 5 } }",
                                 "SELECT * { { :v :w ?b } { ?s :p (?a ?b ?c) } }",
                                 "SELECT * { BIND(5 AS ?b) ?s :p (?a ?b ?c) }") ) {
            Binding b = only(q);
            assertEquals(uri("x"), b.get("s"), q);
            assertEquals(i(5), b.get("b"), q);
        }
    }

    @Test public void repeated_variable() {
        Graph g = parse(":s1 :p (1 undef) . :s2 :p (undef 1) . :s3 :p (1 2) . :s4 :p (undef undef) .");
        List<Binding> r = exec(g, "SELECT * { ?s :p (?v ?v) }");
        assertEquals(3, r.size());
        assertEquals(i(1), ofSubject(r, "s1").get("v"));
        assertEquals(i(1), ofSubject(r, "s2").get("v"));
        assertNull(ofSubject(r, "s4").get("v"));
    }

    @Test public void gap_stops_at_each_slot() {
        List<Binding> r = exec("SELECT ?x { :x :p ( ... ?x ... ) }");
        assertEquals(3, r.size());
        assertEquals(1, r.stream().filter(b->b.get("x")==null).count());
    }

    @Test public void named_gap_keeps_the_slots() {
        assertEquals(seq(Node.UNDEF, i(3)), only("SELECT ?t { :x :p ( ?h ...?t ) }").get("t"));
    }

    @Test public void nested_slot() {
        Graph g = parse(":s :p ( ( undef 1 ) ) .");
        Binding b = exec(g, "SELECT * { ?s :p ( ( ?a ?b ) ) }").get(0);
        assertNull(b.get("a"));
        assertEquals(i(1), b.get("b"));
    }

    @Test public void sequence_with_slot_as_a_value() {
        assertEquals(seq(i(1), Node.UNDEF, i(3)), only("SELECT ?v { :x :p ?v }").get("v"));
        assertEquals(uri("x"), only("SELECT ?s { :x :p ?v . ?s :p ?v }").get("s"));
    }

    @Test public void quads() {
        DatasetGraph dsg = STORES.dataset(PREFIX+":g { :x :p (1 undef) . :v :w 5 }", Lang.TRIG);
        Query q = QueryFactory.create(PREFIX+"SELECT * { GRAPH :g { :v :w ?b . :x :p (?a ?b) } }");
        try ( QueryExec qExec = QueryExec.dataset(dsg).query(q).build() ) {
            Binding b = qExec.select().next();
            assertEquals(i(1), b.get("a"));
            assertEquals(i(5), b.get("b"));
        }
    }

    // ---- The optimizer must not take these variables to be always bound

    @Test public void filter_on_bound() {
        assertEquals(uri("x"), only("SELECT * { ?s :p (?a ?b ?c) FILTER(!BOUND(?b)) }").get("s"));
    }

    @Test public void filter_on_value() {
        assertEquals(uri("y"), only("SELECT * { ?s :p (?a ?b ?c) FILTER(?b = 2) }").get("s"));
        assertEquals(uri("y"), only("SELECT * { ?s :p (?a ?b ?c) FILTER(?b > 0) }").get("s"));
    }

    @Test public void optional_after_a_slot() {
        // ?b is unbound at :x, so the OPTIONAL is free to bind it.
        Binding b = only("SELECT * { ?s :p (?a undef ?c) . OPTIONAL { :v :w ?b } }");
        assertEquals(i(5), b.get("b"));
        List<Binding> r = exec("SELECT * { ?s :p (?a ?b ?c) OPTIONAL { :v :w ?b } }");
        assertEquals(2, r.size());
        assertEquals(i(5), ofSubject(r, "x").get("b"));
        assertEquals(i(2), ofSubject(r, "y").get("b"));
    }

    // ---- Expressions

    @Test public void index_of_a_slot_is_an_error() {
        Binding b = only("SELECT * { :x :p ?s BIND(?s[1] AS ?one) BIND(?s[2] AS ?two) BIND(COALESCE(?s[2], 'none') AS ?d) }");
        assertEquals(i(1), b.get("one"));
        assertNull(b.get("two"));
        assertEquals(NodeFactory.createLiteralString("none"), b.get("d"));
    }

    @Test public void length_and_slice_keep_the_slots() {
        Binding b = only("SELECT * { :x :p ?s BIND(SEQLEN(?s) AS ?n) BIND(?s[2...3] AS ?t) }");
        assertEquals(i(3), b.get("n"));
        assertEquals(seq(Node.UNDEF, i(3)), b.get("t"));
    }

    @Test public void in_skips_slots() {
        assertEquals(1, exec("SELECT * { :x :p ?s FILTER(3 IN ?s) }").size());
        assertEquals(0, exec("SELECT * { :x :p ?s FILTER(2 IN ?s) }").size());
        assertEquals(1, exec("SELECT * { :x :p ?s FILTER(2 NOT IN ?s) }").size());
    }

    @Test public void equality() {
        assertEquals(1, exec("SELECT * { :x :p ?s FILTER(?s = SEQ(1, undef, 3)) }").size());
        assertEquals(1, exec("SELECT * { :x :p ?s FILTER(sameTerm(?s, SEQ(1, undef, 3))) }").size());
        assertEquals(0, exec("SELECT * { :x :p ?s FILTER(?s = SEQ(1, 2, 3)) }").size());
        assertEquals(1, exec("SELECT * { :x :p ?s FILTER(?s != SEQ(1, 2, 3)) }").size());
    }

    @Test public void seq_of_an_unbound_variable_is_a_slot() {
        assertEquals(seq(i(1), Node.UNDEF), only("SELECT ?v { BIND(SEQ(1, ?nothing) AS ?v) }").get("v"));
        assertEquals(seq(i(3), Node.UNDEF, i(1)), only("SELECT ?v { :x :p (?a ?b ?c) BIND(SEQ(?c, ?b, ?a) AS ?v) }").get("v"));
    }

    @Test public void seq_of_any_other_error_is_an_error() {
        assertNull(only("SELECT ?v { BIND(SEQ(1, 1/0) AS ?v) }").get("v"));
    }

    @Test public void unnest_keeps_the_position() {
        List<Binding> r = exec("SELECT * { :x :p ?s UNNEST(?s AS ?m AT ?i) } ORDER BY ?i");
        assertEquals(3, r.size());
        assertEquals(i(2), r.get(1).get("i"));
        assertNull(r.get(1).get("m"));
        assertEquals(i(3), r.get(2).get("m"));
        assertEquals(2, exec("SELECT * { :x :p ?s UNNEST(?s AS ?m AT ?i) FILTER(BOUND(?m)) }").size());
    }

    @Test public void order_by_with_slots() {
        List<Binding> r = exec("SELECT ?s { ?s :p ?v } ORDER BY ?v");
        // A slot sorts before every term.
        assertEquals(uri("z"), r.get(0).get("s"));
        assertEquals(uri("x"), r.get(1).get("s"));
        assertEquals(uri("y"), r.get(2).get("s"));
    }

    // ---- Writing data

    @Test public void construct_round_trip() {
        Query q = QueryFactory.create(PREFIX+"CONSTRUCT { ?s :q (?c ?b ?a) } WHERE { ?s :p (?a ?b ?c) }");
        try ( QueryExec qExec = RdfArrayStores.exec(data()).query(q).build() ) {
            Graph g = qExec.construct();
            assertTrue(g.contains(uri("x"), uri("q"), seq(i(3), Node.UNDEF, i(1))));
            assertTrue(g.contains(uri("y"), uri("q"), seq(i(3), i(2), i(1))));
        }
    }

    @Test public void template_literal_slot() {
        Query q = QueryFactory.create(PREFIX+"CONSTRUCT { :a :b (undef 1) } WHERE {}");
        try ( QueryExec qExec = RdfArrayStores.exec(data()).query(q).build() ) {
            assertTrue(qExec.construct().contains(uri("a"), uri("b"), seq(Node.UNDEF, i(1))));
        }
    }

    @Test public void template_unbound_top_level_still_drops_the_triple() {
        Query q = QueryFactory.create(PREFIX+"CONSTRUCT { :a :b ?nothing } WHERE {}");
        try ( QueryExec qExec = RdfArrayStores.exec(data()).query(q).build() ) {
            assertEquals(0, qExec.construct().size());
        }
    }

    @org.junit.jupiter.api.Disabled("SPARQL Update: BeakGraph stores are read-only")
    @Test public void update() {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        UpdateAction.execute(UpdateFactory.create(PREFIX+"INSERT DATA { :s :p (undef 1) }"), dsg);
        assertTrue(dsg.getDefaultGraph().contains(uri("s"), uri("p"), seq(Node.UNDEF, i(1))));
        UpdateAction.execute(UpdateFactory.create(PREFIX+"INSERT { ?s :q (?b ?a) } WHERE { ?s :p (?a ?b) }"), dsg);
        assertTrue(dsg.getDefaultGraph().contains(uri("s"), uri("q"), seq(i(1), Node.UNDEF)));
    }

    // ---- Syntax

    @Test public void slot_only_in_a_sequence() {
        assertThrows(QueryParseException.class, ()->QueryFactory.create(PREFIX+"SELECT * { ?s :p undef }"));
        assertThrows(QueryParseException.class, ()->QueryFactory.create(PREFIX+"SELECT * { undef :p ?o }"));
        assertThrows(QueryParseException.class, ()->QueryFactory.create(PREFIX+"SELECT * { BIND(undef AS ?x) }"));
    }

    @Test public void values_undef_is_unchanged() {
        assertNull(only("SELECT * { VALUES ?x { undef } }").get("x"));
    }

    @Test public void query_serialization() {
        for ( String s : List.of("SELECT * { ?s :p (1 undef ?c) }",
                                 "SELECT * { BIND(SEQ(1, undef) AS ?v) }",
                                 "CONSTRUCT { ?s :q (undef ?c) } WHERE { ?s :p ?c }") ) {
            Query q = QueryFactory.create(PREFIX+s);
            Query q2 = QueryFactory.create(q.serialize());
            assertEquals(q, q2);
        }
    }

    @Test public void sse_round_trip() {
        Op op = Algebra.compile(QueryFactory.create(PREFIX+"SELECT * { ?s :p (1 undef ?c) }"));
        assertEquals(op, SSE.parseOp(op.toString()));
        assertEquals(seq(i(1), Node.UNDEF), SSE.parseNode("(seqterm 1 undef)"));
    }

    // ---- Result formats

    private static void resultsRoundTrip(Lang lang) {
        Query q = QueryFactory.create(PREFIX+"SELECT ?v { :x :p ?v }");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try ( QueryExec qExec = RdfArrayStores.exec(data()).query(q).build() ) {
            ResultSetMgr.write(out, ResultSet.adapt(qExec.select()), lang);
        }
        ResultSet rs = ResultSetMgr.read(new ByteArrayInputStream(out.toByteArray()), lang);
        assertEquals(seq(i(1), Node.UNDEF, i(3)), rs.nextBinding().get("v"), out.toString());
    }

    @Test public void results_json() { resultsRoundTrip(ResultSetLang.RS_JSON); }
    @Test public void results_xml()  { resultsRoundTrip(ResultSetLang.RS_XML); }
}
