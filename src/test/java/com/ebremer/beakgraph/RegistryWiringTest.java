package com.ebremer.beakgraph;

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
}
