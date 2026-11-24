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

import org.apache.jena.sparql.engine.QueryIterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
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

/** RDF sequences in SPARQL: sequence patterns with gaps, UNNEST, indexes, slices, IN ?seq, functions. */
class SequenceQueriesStoreTest {

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
            + ":x :p (1 2 3) .\n"
            + ":y :p (:a (:b :c)) .\n"
            + ":z :p () .\n"
            + ":w :p 'not a sequence' .\n";

    private static Graph data() {
        return STORES.graph(DATA, Lang.TURTLE);
    }

    private static Node uri(String local) { return NodeFactory.createURI("http://example/"+local); }
    private static Node intLit(int i) { return NodeFactory.createLiteralDT(Integer.toString(i), XSDDatatype.XSDinteger); }

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

    @Test public void exact_sequence_pattern() {
        List<Binding> r = exec("SELECT ?s { ?s :p (1 2 3) }");
        assertEquals(1, r.size());
        assertEquals(uri("x"), r.get(0).get("s"));
    }

    @Test public void sequence_pattern_with_variables() {
        List<Binding> r = exec("SELECT ?a ?b { :x :p (1 ?a ?b) }");
        assertEquals(1, r.size());
        assertEquals(intLit(2), r.get(0).get("a"));
        assertEquals(intLit(3), r.get(0).get("b"));
    }

    @Test public void sequence_pattern_length_must_match() {
        assertEquals(0, exec("SELECT ?a { :x :p (1 ?a) }").size());
        assertEquals(0, exec("SELECT ?a { :x :p (?a ?a ?a) }").size());
    }

    @Test public void repeated_variable_in_pattern() {
        Graph g = STORES.graph(PREFIX+":s :p (5 5) . :t :p (5 6) .", Lang.TURTLE);
        List<Binding> r = exec(g, "SELECT ?s { ?s :p (?v ?v) }");
        assertEquals(1, r.size());
        assertEquals(uri("s"), r.get(0).get("s"));
    }

    @Test public void nested_sequence_pattern() {
        List<Binding> r = exec("SELECT ?s ?c { ?s :p (:a (?b ?c)) }");
        assertEquals(1, r.size());
        assertEquals(uri("y"), r.get(0).get("s"));
        assertEquals(uri("c"), r.get(0).get("c"));
    }

    @Test public void empty_sequence_pattern() {
        List<Binding> r = exec("SELECT ?s { ?s :p () }");
        assertEquals(1, r.size());
        assertEquals(uri("z"), r.get(0).get("s"));
    }

    // ---- Gaps: "..." matches any run of members; "...?rest" also binds it, as a sequence.

    @Test public void gap_first_and_last() {
        List<Binding> r = exec("SELECT ?first ?last { :x :p ( ?first ... ?last ) }");
        assertEquals(1, r.size());
        assertEquals(intLit(1), r.get(0).get("first"));
        assertEquals(intLit(3), r.get(0).get("last"));
        // The gap may be empty.
        assertEquals(1, exec("SELECT ?a ?b { :y :p ( ?a ... ?b ) }").size());
        // But there must be a first and a last member.
        assertEquals(0, exec("SELECT ?a ?b { :z :p ( ?a ... ?b ) }").size());
    }

    @Test public void gap_second_member() {
        List<Binding> r = exec("SELECT ?e { :x :p ( ?a ?e ... ) }");
        assertEquals(1, r.size());
        assertEquals(intLit(2), r.get(0).get("e"));
        assertEquals(0, exec("SELECT ?e { :x :p ( ?a ?b ?c ?e ... ) }").size());
    }

    @Test public void gap_every_member() {
        List<Binding> r = exec("SELECT ?s ?e { ?s :p ( ... ?e ... ) } ORDER BY ?s ?e");
        // :x has 3 elements, :y has 2, :z none, :w is not a sequence.
        assertEquals(5, r.size());
        assertEquals(uri("x"), r.get(0).get("s"));
        assertEquals(uri("y"), r.get(3).get("s"));
    }

    @Test public void gap_with_constant_member() {
        assertEquals(1, exec("SELECT ?s { ?s :p ( :a ... ) }").size());
        assertEquals(0, exec("SELECT ?s { ?s :p ( ... :a ) }").size());
        assertEquals(1, exec("SELECT ?s { ?s :p ( ... :a ... ) }").size());
    }

    @Test public void gaps_nested() {
        // The first member of the second member.
        List<Binding> r = exec("SELECT ?e { :y :p ( ?a ( ?e ... ) ... ) }");
        assertEquals(1, r.size());
        assertEquals(uri("b"), r.get(0).get("e"));
    }

    @Test public void named_gap_head_and_tail() {
        List<Binding> r = exec("SELECT ?h ?t { :x :p ( ?h ...?t ) }");
        assertEquals(1, r.size());
        assertEquals(intLit(1), r.get(0).get("h"));
        assertEquals(NodeFactory.createSeqNode(intLit(2), intLit(3)), r.get(0).get("t"));
        List<Binding> r2 = exec("SELECT ?pre ?x { :x :p ( ...?pre ?x ... ) } ORDER BY ?x");
        assertEquals(3, r2.size());
        assertEquals(NodeFactory.createSeqNode(), r2.get(0).get("pre"));
        assertEquals(NodeFactory.createSeqNode(intLit(1), intLit(2)), r2.get(2).get("pre"));
    }

    @Test public void named_gap_repeated_must_match_equal_runs() {
        Graph g = STORES.graph(PREFIX+":s :p (1 2 1 2) . :t :p (1 2 2 1) .", Lang.TURTLE);
        List<Binding> r = exec(g, "SELECT ?s ?a { ?s :p ( ...?a ...?a ) }");
        assertEquals(1, r.size());
        assertEquals(uri("s"), r.get(0).get("s"));
        assertEquals(NodeFactory.createSeqNode(intLit(1), intLit(2)), r.get(0).get("a"));
    }

    @Test public void named_gap_bound_by_earlier_pattern() {
        Graph g = STORES.graph(PREFIX+":s :p (10 20 30) ; :tail (20 30) ; :other (30) .", Lang.TURTLE);
        List<Binding> r = exec(g, "SELECT ?h { :s :tail ?t . :s :p ( ?h ...?t ) }");
        assertEquals(1, r.size());
        assertEquals(intLit(10), r.get(0).get("h"));
        assertEquals(0, exec(g, "SELECT ?h { :s :other ?t . :s :p ( ?h ...?t ) }").size());
    }

    @Test public void gap_token_versus_gap_then_member() {
        // "( ?a ... ?b )" is a gap and then a member; "( ?a ...?b )" is a named gap.
        List<Binding> r1 = exec("SELECT ?a ?b { :x :p ( ?a ... ?b ) }");
        assertEquals(intLit(3), r1.get(0).get("b"));
        List<Binding> r2 = exec("SELECT ?a ?b { :x :p ( ?a ...?b ) }");
        assertEquals(NodeFactory.createSeqNode(intLit(2), intLit(3)), r2.get(0).get("b"));
    }

    @Test public void lexing_of_ellipsis() {
        // "1..." is 1 then a gap; ":a..." is :a then a gap; ":a...b" is one prefixed name.
        assertEquals(1, exec("SELECT ?s { ?s :p ( 1... ) }").size());
        assertEquals(1, exec("SELECT ?s { ?s :p ( :a... ) }").size());
        Query q = QueryFactory.create(PREFIX+"SELECT * { ?s :p ( :a...b ) }");
        assertTrue(q.toString().contains(":a...b"), q.toString());
        assertFalse(q.toString().contains(":a ..."), q.toString());
    }

    @Test public void no_gap_in_templates_or_data() {
        assertThrows(QueryParseException.class, () -> QueryFactory.create(PREFIX+"CONSTRUCT { ?s :p ( ?a ... ) } WHERE { ?s :p ?a }"));
        assertThrows(QueryParseException.class, () -> UpdateFactory.create(PREFIX+"INSERT DATA { :n :p ( 1 ... ) }"));
    }

    // ---- UNNEST

    @Test public void unnest_members_and_positions() {
        List<Binding> r = exec("SELECT ?i ?e { :x :p ?seq UNNEST(?seq AS ?e AT ?i) } ORDER BY ?i");
        assertEquals(3, r.size());
        for ( int i = 0 ; i < 3 ; i++ ) {
            assertEquals(intLit(i+1), r.get(i).get("i"));
            assertEquals(intLit(i+1), r.get(i).get("e"));
        }
        List<Binding> r2 = exec("SELECT ?s ?e { ?s :p ?seq UNNEST(?seq AS ?e) } ORDER BY ?s");
        assertEquals(5, r2.size());
    }

    @Test public void unnest_of_non_sequence_or_unbound_yields_nothing() {
        assertEquals(0, exec("SELECT ?e { :w :p ?o UNNEST(?o AS ?e) }").size());
        assertEquals(0, exec("SELECT ?e { :z :p ?o UNNEST(?o AS ?e) }").size());
        assertEquals(0, exec("SELECT ?e { :x :p ?o UNNEST(?nothing AS ?e) }").size());
        assertEquals(0, exec("SELECT ?e { :x :p ?o UNNEST(?o[7] AS ?e) }").size());
    }

    @Test public void unnest_of_an_expression() {
        List<Binding> r = exec("SELECT ?e ?i { UNNEST(SEQ(:a, :b) AS ?e AT ?i) } ORDER BY ?i");
        assertEquals(2, r.size());
        assertEquals(uri("b"), r.get(1).get("e"));
    }

    @Test public void unnest_joins_with_an_already_bound_variable() {
        // ?e bound earlier: keep the members equal to it.
        List<Binding> r = exec("SELECT ?i { VALUES ?e { 2 } :x :p ?seq UNNEST(?seq AS ?e AT ?i) }");
        assertEquals(1, r.size());
        assertEquals(intLit(2), r.get(0).get("i"));
    }

    // ---- Expressions: index, slices, IN ?seq

    @Test public void functions_seqlen_isseq_index() {
        List<Binding> r = exec("SELECT ?s ?n ?second ?isseq { ?s :p ?seq . FILTER(isSEQ(?seq)) BIND(SEQLEN(?seq) AS ?n) BIND(?seq[2] AS ?second) BIND(isSEQ(?seq) AS ?isseq) } ORDER BY ?n");
        assertEquals(3, r.size());
        // :z
        assertEquals(intLit(0), r.get(0).get("n"));
        assertFalse(r.get(0).contains("second"));   // out of range: unbound
        // :y
        assertEquals(intLit(2), r.get(1).get("n"));
        assertEquals(NodeFactory.createSeqNode(uri("b"), uri("c")), r.get(1).get("second"));
        // :x
        assertEquals(intLit(3), r.get(2).get("n"));
        assertEquals(intLit(2), r.get(2).get("second"));
        assertEquals(NodeFactory.createLiteralByValue(true), r.get(2).get("isseq"));
    }

    @Test public void isseq_false_for_other_terms() {
        List<Binding> r = exec("SELECT ?s { ?s :p ?o FILTER(!isSEQ(?o)) }");
        assertEquals(1, r.size());
        assertEquals(uri("w"), r.get(0).get("s"));
    }

    @Test public void chained_index_in_expression() {
        List<Binding> r = exec("SELECT ?e { :y :p ?seq BIND(?seq[2][1] AS ?e) }");
        assertEquals(1, r.size());
        assertEquals(uri("b"), r.get(0).get("e"));
    }

    @Test public void slices() {
        List<Binding> r = exec("SELECT ?a ?b ?c ?d ?e { :x :p ?s BIND(?s[2...3] AS ?a) BIND(?s[2...] AS ?b) BIND(?s[...2] AS ?c)"
                               + " BIND(?s[2...9] AS ?d) BIND(?s[3...2] AS ?e) }");
        assertEquals(1, r.size());
        assertEquals(NodeFactory.createSeqNode(intLit(2), intLit(3)), r.get(0).get("a"));
        assertEquals(NodeFactory.createSeqNode(intLit(2), intLit(3)), r.get(0).get("b"));
        assertEquals(NodeFactory.createSeqNode(intLit(1), intLit(2)), r.get(0).get("c"));
        assertEquals(NodeFactory.createSeqNode(intLit(2), intLit(3)), r.get(0).get("d"));    // clamped
        assertEquals(NodeFactory.createSeqNode(), r.get(0).get("e"));                        // empty range
        // Clamped: ()[2...] is ().
        List<Binding> r2 = exec("SELECT ?a { :z :p ?s BIND(?s[2...] AS ?a) }");
        assertEquals(NodeFactory.createSeqNode(), r2.get(0).get("a"));
        // Errors leave the variable unbound: not a sequence, from < 1, a non-integer bound.
        List<Binding> r3 = exec("SELECT ?a ?b ?c { :x :p ?s . :w :p ?w BIND(?w[1...2] AS ?a) BIND(?s[0...2] AS ?b) BIND(?s[1...'2'] AS ?c) }");
        assertFalse(r3.get(0).contains("a"));
        assertFalse(r3.get(0).contains("b"));
        assertFalse(r3.get(0).contains("c"));
        // The ends are expressions; a slice may be indexed.
        List<Binding> r4 = exec("SELECT ?a ?b { :x :p ?s BIND(?s[SEQLEN(?s) - 1 ... SEQLEN(?s)] AS ?a) BIND(?s[2...][1] AS ?b) }");
        assertEquals(NodeFactory.createSeqNode(intLit(2), intLit(3)), r4.get(0).get("a"));
        assertEquals(intLit(2), r4.get(0).get("b"));
    }

    @Test public void slice_syntax_errors() {
        // "[...]" alone is not a slice; "[2:4]" lexes ":4" as a prefixed name.
        assertThrows(QueryParseException.class, () -> QueryFactory.create(PREFIX+"SELECT * { ?s :p ?o BIND(?o[...] AS ?x) }"));
        assertThrows(QueryParseException.class, () -> QueryFactory.create(PREFIX+"SELECT * { ?s :p ?o BIND(?o[2:4] AS ?x) }"));
    }

    @Test public void in_sequence() {
        assertEquals(1, exec("SELECT ?s { ?s :p ?seq FILTER(2 IN ?seq) }").size());
        assertEquals(2, exec("SELECT ?s { ?s :p ?seq FILTER(isSEQ(?seq) && 2 NOT IN ?seq) }").size());
        // The needle may be a sequence; the right-hand side may be SEQ(...) or indexed.
        assertEquals(1, exec("SELECT ?s { ?s :p ?seq FILTER(SEQ(:b, :c) IN ?seq) }").size());
        assertEquals(1, exec("SELECT ?s { ?s :p ?seq FILTER(:c IN ?seq[2]) }").size());
        assertEquals(1, exec("SELECT ?s { ?s :p ?seq FILTER(?seq IN SEQ(SEQ(1, 2, 3))) }").size());
        // A member not comparable with the needle is not equal to it.
        assertEquals(1, exec("SELECT ?s { ?s :p ?seq FILTER(:a IN ?seq) }").size());
        // Not a sequence: an error, so the filter fails.
        assertEquals(0, exec("SELECT ?s { :w :p ?o FILTER(1 IN ?o) }").size());
        // "IN ( ... )" keeps its SPARQL 1.1 meaning: ?o IN (?seq) asks whether ?o equals ?seq.
        assertEquals(0, exec("SELECT ?s { ?s :p ?seq FILTER(2 IN (?seq)) }").size());
        assertEquals(4, exec("SELECT ?s { ?s :p ?seq FILTER(?seq IN (?seq)) }").size());
    }

    @Test public void seq_constructor() {
        List<Binding> r = exec("SELECT ?seq { BIND(SEQ(1, :a, SEQ()) AS ?seq) }");
        assertEquals(1, r.size());
        assertEquals(NodeFactory.createSeqNode(intLit(1), uri("a"), NodeFactory.createSeqNode()), r.get(0).get("seq"));
    }

    @Test public void sequences_in_order_by_and_equality() {
        List<Binding> r = exec("SELECT ?s { ?s :p ?seq FILTER(isSEQ(?seq)) } ORDER BY ?seq");
        assertEquals(3, r.size());
        assertEquals(uri("z"), r.get(0).get("s"));    // () sorts first
        List<Binding> r2 = exec("SELECT ?s { ?s :p ?seq FILTER(?seq = (SEQ(1, 2, 3))) }");
        assertEquals(1, r2.size());
        assertEquals(uri("x"), r2.get(0).get("s"));
    }

    @Test public void construct_sequence_from_pattern() {
        Query query = QueryFactory.create(PREFIX+"CONSTRUCT { ?s :r (?a ?b) } WHERE { ?s :p (?a ?b ?c) }");
        Model m = ModelFactory.createModelForGraph(data());
        try ( QueryExecution qExec = QueryExecutionFactory.create(query, m) ) {
            Graph g = qExec.execConstruct().getGraph();
            assertEquals(1, g.size());
            Node o = g.find().next().getObject();
            assertEquals(NodeFactory.createSeqNode(intLit(1), intLit(2)), o);
        }
    }

    @org.junit.jupiter.api.Disabled("SPARQL Update: BeakGraph stores are read-only")
    @Test public void update_insert_data_and_delete_data() {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        UpdateAction.parseExecute(PREFIX+"INSERT DATA { :n :p (4 5) . :n :q () }", dsg);
        assertEquals(2, dsg.getDefaultGraph().size());
        assertEquals(1, exec(dsg.getDefaultGraph(), "SELECT ?e { :n :p ( ... ?e ) FILTER(?e = 5) }").size());
        UpdateAction.parseExecute(PREFIX+"DELETE DATA { :n :p (4 5) }", dsg);
        assertEquals(1, dsg.getDefaultGraph().size());
    }

    @Test public void dataset_quad_matching() {
        DatasetGraph dsg = STORES.dataset(PREFIX+":g { :x :p (1 2 3) } :h { :y :p (7 8) }", Lang.TRIG);
        Query query = QueryFactory.create(PREFIX+"SELECT ?g ?e { GRAPH ?g { ?s :p ( ?a ?e ... ) } } ORDER BY ?g");
        try ( QueryExec qExec = QueryExec.dataset(dsg).query(query).build() ) {
            List<Binding> r = new ArrayList<>();
            qExec.select().forEachRemaining(r::add);
            assertEquals(2, r.size());
            assertEquals(uri("g"), r.get(0).get("g"));
            assertEquals(intLit(2), r.get(0).get("e"));
            assertEquals(uri("h"), r.get(1).get("g"));
            assertEquals(intLit(8), r.get(1).get("e"));
        }
    }

    @Test public void query_serialization_round_trip() {
        Query q1 = QueryFactory.create(PREFIX+"SELECT * { ?s :p ( ?a ?e ... ) ; :q (1 ?x ()) ; :r ( ... ?z ...?rest ) ; :t ( ?h ...?t ) ."
                                       + " UNNEST(?z AS ?m AT ?i) UNNEST(?rest AS ?n)"
                                       + " FILTER(?x[1] = SEQLEN(?z) && ?m IN ?rest && ?m NOT IN SEQ(1, 2)[1] && ?x IN (?z))"
                                       + " BIND(?z[2...3] AS ?s1) BIND(?z[2...] AS ?s2) BIND(?z[...2] AS ?s3) BIND(SEQ(?s1, ?s2) AS ?s4) }");
        String str = q1.toString();
        assertTrue(str.contains("( ?a ?e ... )"), str);
        assertTrue(str.contains("( ... ?z ...?rest )"), str);
        assertTrue(str.contains("UNNEST(?z AS ?m AT ?i)"), str);
        assertTrue(str.contains("UNNEST(?rest AS ?n)"), str);
        assertTrue(str.contains("?z[2...3]"), str);
        assertTrue(str.contains("?z[2...]"), str);
        assertTrue(str.contains("?z[...2]"), str);
        assertTrue(str.contains("SEQ(?s1, ?s2)"), str);
        Query q2 = QueryFactory.create(str);
        assertEquals(q1, q2);
    }

    @Test public void algebra_sse_round_trip() {
        Query q = QueryFactory.create(PREFIX+"SELECT * { ?s :p ( ?a ?e ... ) ; :q (1 ?x ()) ; :r ( ...?pre ?z ... )"
                                      + " UNNEST(?z AS ?m AT ?i) UNNEST(?pre AS ?n)"
                                      + " FILTER(?x[1] = SEQLEN(?z) && isSEQ(SEQ(1)) && ?m IN ?pre && ?m NOT IN ?pre)"
                                      + " BIND(?z[2...3] AS ?s1) BIND(?z[2...] AS ?s2) BIND(?z[...2] AS ?s3) }");
        Op op1 = Algebra.compile(q);
        String sse = op1.toString();
        assertTrue(sse.contains("(seqterm ?a ?e (seqgap))"), sse);
        assertTrue(sse.contains("(seqterm (seqgap ?pre) ?z (seqgap))"), sse);
        assertTrue(sse.replaceAll("\\s+", " ").contains("(seqterm 1 ?x (seqterm))"), sse);
        assertTrue(sse.contains("(unnest (?m ?i) ?z"), sse);
        assertTrue(sse.contains("(unnest (?n) ?pre"), sse);
        assertTrue(sse.contains("(seqSlice ?z 2 3)"), sse);
        assertTrue(sse.contains("(seqSliceFrom ?z 2)"), sse);
        assertTrue(sse.contains("(seqSliceTo ?z 2)"), sse);
        assertTrue(sse.contains("(inSeq ?m ?pre)"), sse);
        assertTrue(sse.contains("(notInSeq ?m ?pre)"), sse);
        Op op2 = SSE.parseOp(sse);
        assertEquals(op1, op2);
    }

    @Test public void algebra_optimizer_and_reference_engine() {
        // The optimizer (filter placement, variable renaming in sub-queries) and the reference
        // engine see the new operator and terms.
        Query q = QueryFactory.create(PREFIX+"SELECT ?s ?i { { SELECT ?s ?seq { ?s :p ?seq FILTER(isSEQ(?seq)) } }"
                                      + " UNNEST(?seq AS ?e AT ?i) FILTER(?e = 2) ?s :p ( ... ?e ... ) }");
        Op op = Algebra.optimize(Algebra.compile(q));
        assertTrue(op.toString().contains("unnest"), op.toString());
        List<Binding> r = exec(q.toString());
        assertEquals(1, r.size());
        assertEquals(intLit(2), r.get(0).get("i"));
        try ( QueryExec qExec = RdfArrayStores.exec(data()).query(q).set(org.apache.jena.query.ARQ.optimization, false).build() ) {
            assertEquals(1, Iter.count(qExec.select()));
        }
        Op opRef = Algebra.compile(q);
        QueryIterator qIter = Algebra.execRef(opRef, DatasetGraphFactory.wrap(data()));
        assertEquals(1, Iter.count(qIter));
    }

    @Test public void json_results_carry_sequences() {
        Query query = QueryFactory.create(PREFIX+"SELECT ?seq { :y :p ?seq }");
        Model m = ModelFactory.createModelForGraph(data());
        try ( QueryExecution qExec = QueryExecutionFactory.create(query, m) ) {
            ResultSet rs = qExec.execSelect();
            String json = ResultSetFormatter.asText(rs);
            assertTrue(json.contains("( :a ( :b :c ) )") || json.contains("( <http://example/a>"), json);
        }
        try ( QueryExecution qExec = QueryExecutionFactory.create(query, m) ) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            ResultSetFormatter.outputAsJSON(out, qExec.execSelect());
            String json = out.toString();
            assertTrue(json.contains("\"seq\""), json);
        }
    }

    /** rdf:first and rdf:rest triples are ordinary triples: printing a query must not fold them into ( ). */
    @Test public void printedQueryKeepsLadderTriples() {
        String q = PREFIX + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                 + "SELECT * { ?s :p ?l . ?l rdf:first 1 ; rdf:rest rdf:nil }";
        Query query = QueryFactory.create(q);
        String printed = query.serialize();
        assertFalse(printed.contains("("), printed);
        assertEquals(query, QueryFactory.create(printed));
    }

    @Test public void printedQueryKeepsSequences() {
        Query query = QueryFactory.create(PREFIX + "SELECT * { ?s :p ( 1 ( ?x ) () ) . ( ?s ) :q ?o }");
        assertEquals(query, QueryFactory.create(query.serialize()));
    }

    /** Going back from the algebra, a property function's list of arguments is one sequence term. */
    @Test public void opAsQueryWritesPropertyFunctionListsAsSequences() {
        for ( String args : new String[] {"( ?a ?b )", "()"} ) {
            Query query = QueryFactory.create(PREFIX + "PREFIX apf: <http://jena.apache.org/ARQ/property#>\n"
                                            + "SELECT * { ?s apf:splitIRI " + args + " }");
            Op op = Algebra.optimize(Algebra.compile(query));
            String printed = org.apache.jena.sparql.algebra.OpAsQuery.asQuery(op).serialize();
            assertFalse(printed.contains("rdf-syntax-ns"), printed);
            assertTrue(printed.contains(args), printed);
        }
    }
}
