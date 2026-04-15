package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.fuseki.RelativeIRIResolver;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end regression test: builds an HDF5 file from Turtle and queries it,
 * covering both bugs fixed in this work -
 * <ul>
 *   <li>document-relative IRIs ({@code <>}, {@code <sibling>}) are stored
 *       relative and resolve against the serving URL;</li>
 *   <li>a concrete subject / object in a triple pattern restricts the scan
 *       instead of returning every triple.</li>
 * </ul>
 */
class RelativeIriRoundTripTest {

    private static final String TTL = """
        @prefix geo:  <http://www.opengis.net/ont/geosparql#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix sdo:  <https://schema.org/> .

        <> a geo:FeatureCollection ;
           rdfs:label "doc" .

        <image.png> a sdo:ImageObject .

        <http://snomed.info/id/123> a sdo:Thing .
        """;

    private static final String FC_QUERY =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "
          + "SELECT ?s WHERE { ?s a geo:FeatureCollection }";

    @TempDir
    static Path dir;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("doc.ttl").toFile();
        File h5 = dir.resolve("doc.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl)
                .setDestination(h5)
                .setSpatial(false)
                .setFeatures(false)
                .build()
                .write();
        ds = new BeakGraph(new HDF5Reader(h5)).getDataset();
    }

    private static int count(String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) {
                rs.next();
                n++;
            }
            return n;
        }
    }

    @Test
    void emptyReferenceIsStoredRelative() {
        // <> must be stored as the empty relative reference - not resolved
        // against the process working directory (the original bug).
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(FC_QUERY)).build()) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext());
            assertEquals("", rs.next().get("s").asNode().getURI());
        }
    }

    @Test
    void resolverProducesTheServingUrl() {
        String base = "http://example.org/data/doc.ttl.h5";
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(FC_QUERY)).build()) {
            ResultSet rs = new RelativeIRIResolver(base).resolve(qe.execSelect());
            assertTrue(rs.hasNext());
            assertEquals(base, rs.next().get("s").asNode().getURI());
        }
    }

    @Test
    void fullScanReturnsEveryTriple() {
        assertEquals(4, count("SELECT * WHERE { ?s ?p ?o }"));
    }

    @Test
    void concreteSubjectRestrictsTheScan() {
        // regression: before the fix this returned every triple in the graph
        assertEquals(1, count(
                "SELECT * WHERE { <http://snomed.info/id/123> ?p ?o }"));
    }

    @Test
    void concreteObjectRestrictsTheScan() {
        assertEquals(1, count(
                "SELECT * WHERE { ?s ?p <https://schema.org/Thing> }"));
    }

    @Test
    void rejectsHdf5FileFromANewerFormatVersion() throws Exception {
        File future = dir.resolve("future.h5").toFile();
        try (WritableHdfFile w = HdfFile.write(future.toPath())) {
            WritableGroup bg = w.putGroup(Params.BG);
            bg.putAttribute("formatVersion", Params.FORMAT_VERSION + 1000);
        }
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new HDF5Reader(future));
        // fails clearly at open time instead of crashing deep in a dictionary read
        assertTrue(ex.getMessage().contains(String.valueOf(Params.FORMAT_VERSION + 1000)));
    }
}
