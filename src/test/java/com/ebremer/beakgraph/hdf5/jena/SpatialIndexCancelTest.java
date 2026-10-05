package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-71: the spatial candidate collection (up to 31 scales x 256 ranges of
 * GPOS scans) used to run inside the iterator's constructor, before the
 * solver had wrapped the chain in its abortable stages and without ever
 * consulting the cancel signal - a query timeout could not stop it. It now
 * runs at the first hasNext() and throws QueryCancelledException as soon as
 * the signal is set.
 */
class SpatialIndexCancelTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:a geo:asWKT "POLYGON((150 150,160 150,160 160,150 160,150 150))"^^geo:wktLiteral .
        ex:b geo:asWKT "POLYGON((0 0,1000 0,1000 1000,0 1000,0 0))"^^geo:wktLiteral .
        ex:c geo:asWKT "POLYGON((5000 5000,5100 5000,5100 5100,5000 5100,5000 5000))"^^geo:wktLiteral .
        """;
    private static final String REGION = "POLYGON((100 100,300 100,300 300,100 300,100 100))";

    @TempDir
    static Path dir;
    static BeakGraph bg;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("sp.ttl").toFile();
        File h5 = dir.resolve("sp.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(true).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        assertTrue(SpatialIndexIterator.isAvailable(bg), "the fixture must carry the spatial index");
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static SpatialIndexIterator seeded(AtomicBoolean cancel) {
        Iterator<BindingNodeId> root = List.of(new BindingNodeId()).iterator();
        return new SpatialIndexIterator(root, bg, Var.alloc("f"),
                new PatternMatchBG.SpatialContext(Var.alloc("w"), REGION), cancel);
    }

    @Test
    void candidatesAreCollectedAtTheFirstRowAndHonourTheCancelSignal() {
        AtomicBoolean cancel = new AtomicBoolean(true);
        // Construction does no work, so even a cancelled execution builds the
        // iterator without throwing - the first pull is where the check lands.
        SpatialIndexIterator it = seeded(cancel);
        assertThrows(QueryCancelledException.class, it::hasNext);

        cancel.set(false);
        SpatialIndexIterator live = seeded(cancel);
        int n = 0;
        while (live.hasNext()) {
            live.next();
            n++;
        }
        assertEquals(2, n, "the two geometries whose bbox overlaps the region are the candidates (ex:c is far away)");
    }
}
