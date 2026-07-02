package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for the H8 write path: a wktLiteral carrying the optional
 * GeoSPARQL CRS prefix must be spatially indexed. Previously the raw lexical
 * form went straight into a JTS WKTReader, failed to parse, and the geometry
 * was silently dropped from the index (classified "degenerate").
 */
class SpatialCrsIndexingTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:f1 geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POLYGON((0 0,64 0,64 64,0 64,0 0))"^^geo:wktLiteral .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("crs.ttl").toFile();
        File h5 = dir.resolve("crs.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(true).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    @Test
    void crsPrefixedGeometryIsIndexed() {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "SELECT * WHERE { GRAPH <" + Params.SPATIALSTRING + "> { ?s ?p ?o } }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            assertTrue(n > 0, "the spatial graph must contain index rows for a CRS-prefixed geometry");
        }
    }
}
