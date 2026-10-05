package com.ebremer.beakgraph.features;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.cmdline.BeakGraphCLI;
import com.ebremer.beakgraph.cmdline.ExportFormatValidator;
import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.CustomFileTypeDetector;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-362: the only locale regression test forced de_DE (comma decimal
 * separator). Arabic localizes {@code %d} digits, and Turkish maps I/i
 * through dotless and dotted forms - {@code "trig".toUpperCase()} is
 * {@code "TRİG"} there, so a default-locale case mapping on a protocol string
 * or a file extension silently stops matching. Every locale-sensitive site
 * (identifier minting, -export format names, file-type detection, exports)
 * is run here under ar, tr-TR and de-DE and must behave exactly as under
 * the root locale.
 */
class LocaleInvarianceTest {

    private static final Pattern GRID = Pattern.compile("urn:x-beakgraph:grid:[0-9]+:-?[0-9]+:-?[0-9]+");
    private static final Pattern BNODE = Pattern.compile("b[0-9]{20}");
    private static final String TTL = "@prefix ex: <http://ex.org/> .\n"
            + "@prefix geo: <http://www.opengis.net/ont/geosparql#> .\n"
            + "ex:f1 geo:asWKT \"POLYGON((10 10,1500 10,1500 1500,10 1500,10 10))\"^^geo:wktLiteral .\n"
            + "ex:f1 ex:has [ ex:v 1 ] , [ ex:v 2 ] .\n"
            + "ex:f1 ex:label \"Işık\" ; ex:when \"2020-01-01T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n";

    @TempDir
    Path dir;
    private Locale saved;

    static Stream<Locale> locales() {
        return Stream.of(Locale.forLanguageTag("ar-EG"), Locale.forLanguageTag("tr-TR"), Locale.GERMANY);
    }

    @BeforeEach
    void saveLocale() {
        saved = Locale.getDefault();
    }

    @AfterEach
    void restoreLocale() {
        Locale.setDefault(saved);
    }

    private static List<Node> nodes(Dataset ds, String query, String var) {
        List<Node> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) out.add(rs.next().get(var).asNode());
        }
        return out;
    }

    private File build(String name, boolean spatial) throws Exception {
        File src = dir.resolve(name + ".ttl").toFile();
        File h5 = dir.resolve(name + ".h5").toFile();
        Files.writeString(src.toPath(), TTL, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(spatial).setFeatures(spatial).build().write();
        return h5;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("locales")
    void identifiersAreAsciiUnderEveryLocale(Locale locale) throws Exception {
        Locale.setDefault(locale);
        if ("ar".equals(locale.getLanguage())) {
            assertNotEquals("12", String.format("%d", 12), "precondition: the JDK localizes digits for " + locale);
        }
        if ("tr".equals(locale.getLanguage())) {
            assertNotEquals("TRIG", "trig".toUpperCase(), "precondition: Turkish dotted I");
        }
        File h5 = build("ids-" + locale.toLanguageTag(), true);
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            List<Node> graphs = nodes(ds, "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } FILTER(STRSTARTS(STR(?g), \"urn:x-beakgraph:grid:\")) }", "g");
            assertFalse(graphs.isEmpty());
            for (Node g : graphs) assertTrue(GRID.matcher(g.getURI()).matches(), "non-ASCII grid graph name under " + locale + ": " + g);
            List<Node> bnodes = nodes(ds, "SELECT ?b WHERE { <http://ex.org/f1> <http://ex.org/has> ?b }", "b");
            assertEquals(2, bnodes.size());
            for (Node b : bnodes) assertTrue(BNODE.matcher(b.getBlankNodeLabel()).matches(), "non-ASCII bnode label under " + locale + ": " + b);
            // Feature WKT (centroid / axes) stays parseable: '.' decimal separator.
            org.locationtech.jts.io.WKTReader reader = new org.locationtech.jts.io.WKTReader();
            int wkts = 0;
            for (Node w : nodes(ds, "SELECT ?w WHERE { ?s ?p ?w FILTER(datatype(?w) = <http://www.opengis.net/ont/geosparql#wktLiteral>) }", "w")) {
                reader.read(w.getLiteralLexicalForm());   // throws on locale-corrupted WKT
                wkts++;
            }
            assertTrue(wkts > 1, "the source polygon plus generated feature geometries");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("locales")
    void formatNamesAndFileTypesAreCaseMappedLocaleIndependently(Locale locale) throws Exception {
        Locale.setDefault(locale);
        assertEquals("TRIG", ExportFormatValidator.normalize("trig"));
        assertEquals("JSONLD", ExportFormatValidator.normalize("json-ld"));
        assertEquals("JSONLD", ExportFormatValidator.normalize("jsonld"));
        assertEquals("NT", ExportFormatValidator.normalize("nt"));
        new ExportFormatValidator().validate("-export", "trig");   // must not throw under tr-TR
        assertTrue(RdfSources.isSupported("DATA.TRIG"));
        assertTrue(RdfSources.isSupported("DATA.TRIG.GZ"));
        assertTrue(RdfSources.isSupported("Index.TTL.ZIP"));
        CustomFileTypeDetector detector = new CustomFileTypeDetector();
        assertEquals("text/turtle", detector.probeContentType(Path.of("FILE.TTL")));
        assertEquals("application/ld+json", detector.probeContentType(Path.of("FILE.JSONLD")));
        assertEquals("application/x-hdf5", detector.probeContentType(Path.of("BIG.H5")));
        assertEquals("application/n-triples", detector.probeContentType(Path.of("TRIPLES.NT")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("locales")
    void exportsAreByteIdenticalToTheRootLocale(Locale locale) throws Exception {
        File h5 = build("export-" + locale.toLanguageTag(), false);
        byte[] underLocale = export(h5, locale, "under-" + locale.toLanguageTag());
        byte[] underRoot = export(h5, Locale.ROOT, "root-" + locale.toLanguageTag());
        assertTrue(underRoot.length > 0);
        assertArrayEquals(underRoot, underLocale, "-export NQ must not depend on the default locale (" + locale + ")");
        String text = new String(underRoot, StandardCharsets.UTF_8);
        assertTrue(text.contains("\"Işık\""), "non-ASCII literal content survives: " + text);
    }

    private byte[] export(File h5, Locale locale, String subdir) throws Exception {
        Locale.setDefault(locale);
        Path out = Files.createDirectories(dir.resolve(subdir));
        File copy = out.resolve("data.h5").toFile();
        Files.copy(h5.toPath(), copy.toPath());
        Parameters p = new Parameters();
        p.src = copy;
        p.export = "NQ";
        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.export();
        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "export under " + locale + " must succeed");
        return Files.readAllBytes(out.resolve("data.nq"));
    }
}
