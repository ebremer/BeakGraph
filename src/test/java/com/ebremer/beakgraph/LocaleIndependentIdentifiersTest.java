package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-354: grid-tile graph IRIs and blank-node labels were
 * minted with {@code String.format("%d")} / {@code "%020d"} and no Locale.
 * java.util.Formatter localizes digits under the JVM's FORMAT locale, so a
 * store built on an Arabic-, Persian- or Bengali-language install stored
 * graph names like {@code urn:x-beakgraph:grid:٠:١٢:٣} and blank nodes with
 * non-ASCII labels: stored data that differed by build machine and no
 * longer matched the documented {@code urn:x-beakgraph:grid:{level}:{x}:{y}}
 * and {@code b%020d} forms. Identifiers are now built from ASCII digits
 * regardless of locale.
 */
class LocaleIndependentIdentifiersTest {

    private static final Pattern GRID = Pattern.compile("urn:x-beakgraph:grid:[0-9]+:-?[0-9]+:-?[0-9]+");
    private static final Pattern BNODE = Pattern.compile("b[0-9]{20}");

    @TempDir
    static Path dir;
    static Locale savedFormat;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildUnderArabicDigits() throws Exception {
        savedFormat = Locale.getDefault(Locale.Category.FORMAT);
        Locale arabic = Locale.forLanguageTag("ar-EG");
        Locale.setDefault(Locale.Category.FORMAT, arabic);
        // Precondition: this JVM really localizes %d here, or the test is vacuous.
        assertNotEquals("12", String.format("%d", 12), "JDK locale data must localize digits for ar-EG");

        String ttl = "@prefix ex: <http://ex.org/> .\n"
                + "@prefix geo: <http://www.opengis.net/ont/geosparql#> .\n"
                + "ex:f1 geo:asWKT \"POLYGON((10 10,1500 10,1500 1500,10 1500,10 10))\"^^geo:wktLiteral .\n"
                + "ex:f1 ex:has [ ex:v 1 ] , [ ex:v 2 ] .\n";
        File src = dir.resolve("locale.ttl").toFile();
        File h5 = dir.resolve("locale.h5").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(true).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void restore() {
        if (bg != null) bg.close();
        if (savedFormat != null) Locale.setDefault(Locale.Category.FORMAT, savedFormat);
    }

    private static List<Node> nodes(String query, String var) {
        List<Node> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) out.add(rs.next().get(var).asNode());
        }
        return out;
    }

    @Test
    void gridTileGraphNamesUseAsciiDigits() {
        List<Node> graphs = nodes("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } FILTER(STRSTARTS(STR(?g), \"urn:x-beakgraph:grid:\")) }", "g");
        assertFalse(graphs.isEmpty(), "a spatial store has grid-tile graphs");
        for (Node g : graphs) {
            assertTrue(GRID.matcher(g.getURI()).matches(), "non-ASCII grid graph name: " + g.getURI());
        }
        // Tile (0, 0, 0) covers the polygon's origin corner (512-unit tiles).
        assertTrue(graphs.contains(Params.gridGraph(0, 0, 0)), "expected tile 0:0:0 among " + graphs);
        assertEquals(1, nodes("SELECT ?s WHERE { GRAPH <urn:x-beakgraph:grid:0:0:0> { ?s ?p ?o } } LIMIT 1", "s").size());
    }

    @Test
    void blankNodeLabelsUseAsciiDigits() {
        List<Node> bnodes = nodes("SELECT ?b WHERE { <http://ex.org/f1> <http://ex.org/has> ?b }", "b");
        assertEquals(2, bnodes.size());
        for (Node b : bnodes) {
            assertTrue(b.isBlank());
            assertTrue(BNODE.matcher(b.getBlankNodeLabel()).matches(), "non-ASCII blank node label: " + b.getBlankNodeLabel());
        }
    }

    @Test
    void helpersAreLocaleIndependent() {
        assertEquals("urn:x-beakgraph:grid:3:12:-7", Params.gridGraph(3, 12, -7).getURI());
        assertEquals("b00000000000000000012", Params.blankNodeLabel(12));
        assertEquals("b4_00000000000000000012", Params.blankNodeLabel("b4_", 12));
        // Same digits the Formatter would produce under a Latin locale.
        assertEquals(String.format(Locale.ROOT, "b%020d", 987654321L), Params.blankNodeLabel(987654321L));
        assertEquals(String.format(Locale.ROOT, "urn:x-beakgraph:grid:%d:%d:%d", 2, 40, 41), Params.gridGraph(2, 40, 41).getURI());
    }
}
