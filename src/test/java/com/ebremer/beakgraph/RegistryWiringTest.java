package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.QueryEngineBG;
import com.ebremer.beakgraph.hdf5.jena.AggregateCountFastPath;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.OpExecutorBG;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.pfunction.PropertyFunctionRegistry;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for H7: loading BeakGraph must not mutate global Jena
 * semantics for other datasets in the same JVM.
 * <ul>
 *   <li>The standard rdfs:member property function must stay registered
 *       globally (it used to be remove()d JVM-wide).</li>
 *   <li>BG stores treat rdfs:member as a plain stored predicate - excluded
 *       from property-function rewriting via a dataset-scoped registry, not by
 *       changing the global one.</li>
 *   <li>The BG OpExecutor factory is wired into the dataset's context, not the
 *       global ARQ context.</li>
 *   <li>The globally registered geof:sfIntersects must actually evaluate
 *       intersection (it used to return TRUE unconditionally).</li>
 * </ul>
 */
class RegistryWiringTest {

    private static final String TTL = """
        @prefix ex:   <http://ex.org/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        ex:bag rdfs:member ex:item .
        ex:s1 ex:p ex:o1 .
        """;

    private static final String GEO_WKT = "http://www.opengis.net/ont/geosparql#wktLiteral";
    private static final String GEOF = "http://www.opengis.net/def/function/geosparql/";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("wiring.ttl").toFile();
        File h5 = dir.resolve("wiring.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    // --- global registries stay intact -------------------------------------

    @Test
    void aReplacedGlobalStageGeneratorIsReWrappedOnTheNextOpen() throws Exception {
        org.apache.jena.sparql.engine.main.StageGenerator before =
                org.apache.jena.sparql.engine.main.StageBuilder.chooseStageGenerator(ARQ.getContext());
        assertTrue(before instanceof com.ebremer.beakgraph.hdf5.jena.StageGeneratorDirectorBG, "control: BeakGraph is wired");
        try {
            // Another component replaces (does not wrap) the global generator.
            org.apache.jena.sparql.engine.main.StageBuilder.setGenerator(ARQ.getContext(),
                    org.apache.jena.sparql.engine.main.StageBuilder.standardGenerator());
            assertFalse(org.apache.jena.sparql.engine.main.StageBuilder.chooseStageGenerator(ARQ.getContext())
                    instanceof com.ebremer.beakgraph.hdf5.jena.StageGeneratorDirectorBG);
            try (BeakGraph again = new BeakGraph(new HDF5Reader(dir.resolve("wiring.ttl.h5").toFile()))) {
                assertTrue(org.apache.jena.sparql.engine.main.StageBuilder.chooseStageGenerator(ARQ.getContext())
                        instanceof com.ebremer.beakgraph.hdf5.jena.StageGeneratorDirectorBG,
                        "opening a BeakGraph re-installs the director (BG-63)");
                assertTrue(org.apache.jena.sparql.engine.main.StageBuilder.chooseStageGenerator(
                        again.getDataset().asDatasetGraph().getContext())
                        instanceof com.ebremer.beakgraph.hdf5.jena.StageGeneratorDirectorBG,
                        "the dataset's own context carries the director regardless of the global slot");
            }
        } finally {
            org.apache.jena.sparql.engine.main.StageBuilder.setGenerator(ARQ.getContext(), before);
        }
    }

    @Test
    void rdfsMemberStaysRegisteredGlobally() {
        assertNotNull(PropertyFunctionRegistry.get().get(RDFS.member.getURI()),
            "loading BeakGraph must not remove the standard rdfs:member property function");
    }

    @Test
    void rdfsMemberStillWorksOnPlainJenaModels() {
        // rdf:_1 containership answered via the rdfs:member property function
        // (which requires the subject to be typed as a container).
        Model m = ModelFactory.createDefaultModel();
        Resource c = m.createResource("http://ex.org/c");
        Resource item = m.createResource("http://ex.org/i");
        c.addProperty(RDF.type, RDF.Seq);
        c.addProperty(RDF.li(1), item);
        try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
                "SELECT ?x WHERE { <http://ex.org/c> rdfs:member ?x }"), m)) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext(), "container membership must be answered");
            assertEquals("http://ex.org/i", rs.next().getResource("x").getURI());
            assertFalse(rs.hasNext());
        }
    }

    @Test
    void globalOpExecutorFactoryIsNotHijacked() {
        assertNotSame(OpExecutorBG.opExecFactoryBG, QC.getFactory(ARQ.getContext()),
            "the BG OpExecutor factory must not be installed JVM-globally");
    }

    // --- per-dataset wiring --------------------------------------------------

    @Test
    void datasetContextCarriesBGExecutorAndScopedPropertyFunctions() {
        assertSame(OpExecutorBG.opExecFactoryBG, QC.getFactory(ds.asDatasetGraph().getContext()),
            "BG datasets must carry their executor in their own context");
        PropertyFunctionRegistry scoped =
            PropertyFunctionRegistry.chooseRegistry(ds.asDatasetGraph().getContext());
        assertNull(scoped.get(RDFS.member.getURI()),
            "BG datasets must not rewrite rdfs:member patterns into container membership");
    }

    @Test
    void rdfsMemberIsAPlainPredicateInBGData() {
        // The stored triple uses rdfs:member literally; the property function
        // (which expands to rdf:_N lookups) would answer nothing here.
        Set<String> items = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
                "SELECT ?x WHERE { <http://ex.org/bag> rdfs:member ?x }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get("x");
                if (n != null && n.isURIResource()) items.add(n.asResource().getURI());
            }
        }
        assertEquals(Set.of("http://ex.org/item"), items);
    }

    // --- sfIntersects fallback is sound --------------------------------------

    private static boolean intersects(String wkt1, String wkt2) {
        String q = "ASK { FILTER(<" + GEOF + "sfIntersects>(" +
            "\"" + wkt1 + "\"^^<" + GEO_WKT + ">, \"" + wkt2 + "\"^^<" + GEO_WKT + ">)) }";
        try (QueryExecution qe = QueryExecutionFactory.create(
                QueryFactory.create(q), ModelFactory.createDefaultModel())) {
            return qe.execAsk();
        }
    }

    @Test
    void sfIntersectsEvaluatesGeometry() {
        assertTrue(intersects("POLYGON((0 0,10 0,10 10,0 10,0 0))", "POINT(5 5)"),
            "point inside polygon");
        assertTrue(intersects("POLYGON((0 0,10 0,10 10,0 10,0 0))", "POLYGON((5 5,15 5,15 15,5 15,5 5))"),
            "overlapping polygons intersect even though neither is within the other");
        assertFalse(intersects("POLYGON((0 0,10 0,10 10,0 10,0 0))", "POINT(50 50)"),
            "disjoint geometries must NOT intersect (previously always true)");
        assertTrue(intersects("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(5 5)",
                "POLYGON((0 0,10 0,10 10,0 10,0 0))"),
            "CRS-prefixed wktLiteral must be handled");
    }

    // --- BG-338: the Model path gets the same wiring as the dataset path ----

    private static java.util.List<String> members(Model model) {
        java.util.List<String> out = new java.util.ArrayList<>();
        try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
                "SELECT ?x WHERE { <http://ex.org/bag> rdfs:member ?x }"), model)) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) out.add(rs.next().getResource("x").getURI());
        }
        return out;
    }

    private static long countViaModel(Model model) {
        try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(
                "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"), model)) {
            return qe.execSelect().next().getLiteral("c").getLong();
        }
    }

    @Test
    void modelPathsAnswerRdfsMemberLikeTheDataset() {
        // ds.getDefaultModel() and a Model over the graph are Jena-wrapped into a
        // fresh DatasetGraphOne whose context is not the BGDatasetGraph's. With
        // Jena 6.2 the global container property function happens to answer a
        // literal rdfs:member triple too, so this is an equivalence check across
        // the three routes rather than a reproduction of a miss.
        assertEquals(java.util.List.of("http://ex.org/item"), members(ds.getDefaultModel()));
        assertEquals(java.util.List.of("http://ex.org/item"), members(ModelFactory.createModelForGraph(bg)));
    }

    @Test
    void modelPathsRunOnTheBGExecutor() {
        // What the Model path really lost: the BG OpExecutor (filter pushdown,
        // spatial seeding, the DISTINCT/COUNT fast paths) lives in the dataset
        // context, which a DatasetGraphOne never carries. QueryEngineBG installs
        // it per execution; the COUNT fast path's hit counter is the witness.
        for (Model model : new Model[]{ds.getDefaultModel(), ModelFactory.createModelForGraph(bg)}) {
            long before = AggregateCountFastPath.HITS.get();
            assertEquals(2, countViaModel(model));
            assertEquals(1, AggregateCountFastPath.HITS.get() - before,
                    "the BG COUNT fast path must run for a Model over a BeakGraph");
        }
        assertTrue(QueryEngineBG.isBeakGraphDataset(DatasetGraphFactory.wrap(bg)));
        assertTrue(QueryEngineBG.isBeakGraphDataset(ds.asDatasetGraph()));
        // Plain Jena models keep Jena's engine (see rdfsMemberStillWorksOnPlainJenaModels).
        assertFalse(QueryEngineBG.isBeakGraphDataset(DatasetGraphFactory.wrap(ModelFactory.createDefaultModel().getGraph())));
    }


    @Test
    void propertyFunctionsRegisteredLaterAreVisible() {
        // BG-6: the dataset's scoped registry used to be a one-time copy of the
        // global one, so a function registered after the first BeakGraph opened
        // was invisible here (the pattern ran as a plain stored predicate).
        String uri = "http://ex.org/pf/late-" + System.nanoTime();
        org.apache.jena.sparql.pfunction.PropertyFunctionFactory factory = u -> new org.apache.jena.sparql.pfunction.PFuncSimple() {
            @Override
            public org.apache.jena.sparql.engine.QueryIterator execEvaluated(
                    org.apache.jena.sparql.engine.binding.Binding binding, org.apache.jena.graph.Node subject,
                    org.apache.jena.graph.Node predicate, org.apache.jena.graph.Node object,
                    org.apache.jena.sparql.engine.ExecutionContext execCxt) {
                org.apache.jena.sparql.engine.binding.Binding b = org.apache.jena.sparql.engine.binding.BindingFactory.binding(
                        binding, org.apache.jena.sparql.core.Var.alloc(object), org.apache.jena.graph.NodeFactory.createLiteralString("late"));
                return org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper.create(java.util.List.of(b).iterator(), execCxt);
            }
        };
        PropertyFunctionRegistry.get().put(uri, factory);
        try {
            PropertyFunctionRegistry scoped = PropertyFunctionRegistry.chooseRegistry(ds.asDatasetGraph().getContext());
            assertTrue(scoped.isRegistered(uri), "the scoped registry sees the late registration");
            assertTrue(scoped.manages(uri));
            assertNotNull(scoped.get(uri));
            assertFalse(scoped.isRegistered(RDFS.member.getURI()), "rdfs:member stays masked");
            java.util.Set<String> keys = new java.util.HashSet<>();
            scoped.keys().forEachRemaining(keys::add);
            assertTrue(keys.contains(uri));
            assertFalse(keys.contains(RDFS.member.getURI()));
            try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                    "SELECT ?o WHERE { <http://ex.org/s> <" + uri + "> ?o }")).build()) {
                ResultSet rs = qe.execSelect();
                assertTrue(rs.hasNext(), "the property function must answer on a BG dataset");
                assertEquals("late", rs.next().getLiteral("o").getString());
            }
        } finally {
            PropertyFunctionRegistry.get().remove(uri);
        }
        assertFalse(PropertyFunctionRegistry.chooseRegistry(ds.asDatasetGraph().getContext()).isRegistered(uri),
                "and an unregistration is visible too");
    }
}
