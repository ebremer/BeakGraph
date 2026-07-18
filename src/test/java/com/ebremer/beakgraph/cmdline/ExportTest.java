package com.ebremer.beakgraph.cmdline;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * -export: dumping a BeakGraph back to RDF. Round-trips must reproduce the
 * source data (isomorphic; BeakGraph's internal VoID/spatial metadata graphs
 * are excluded), TTL/NT must auto-upgrade to TRIG/NQ when user named graphs
 * exist, and -compress must gzip with the extra .gz extension.
 */
class ExportTest {

    @TempDir
    Path dir;

    private static final String TRIPLES =
            "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
          + "<http://ex.org/a> <http://ex.org/name> \"Alice\" .\n"
          + "<http://ex.org/a> <http://ex.org/n> \"5\"^^<http://www.w3.org/2001/XMLSchema#int> .\n";

    private static final String QUADS = TRIPLES
          + "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o> <http://ex.org/g1> .\n";

    private File buildStore(String name, String nquads) throws Exception {
        File src = dir.resolve(name + ".nq").toFile();
        Files.write(src.toPath(), nquads.getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve(name + ".h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).build().write();
        return h5;
    }

    private void runExport(File src, String format, boolean compress) {
        Parameters p = new Parameters();
        p.src = src;
        p.export = format;
        p.compress = compress;
        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.export();
        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(),
                "export of " + src + " as " + format + " must succeed");
    }

    private Dataset parse(Path file, Lang lang, boolean gzipped) throws Exception {
        Dataset ds = DatasetFactory.create();
        try (InputStream in = gzipped
                ? new GZIPInputStream(Files.newInputStream(file))
                : Files.newInputStream(file)) {
            RDFDataMgr.read(ds, in, lang);
        }
        return ds;
    }

    private static Model expectedTriples() {
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new java.io.ByteArrayInputStream(TRIPLES.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
        return m;
    }

    @Test
    void ntExportRoundTripsAndExcludesInternalGraphs() throws Exception {
        File h5 = buildStore("plain", TRIPLES);
        runExport(h5, "NT", false);
        Path out = dir.resolve("plain.nt");
        assertTrue(Files.exists(out), "plain.nt must be created next to plain.h5");
        Dataset ds = parse(out, Lang.NTRIPLES, false);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expectedTriples()),
                "exported triples must round-trip the source data");
        assertFalse(ds.asDatasetGraph().listGraphNodes().hasNext(),
                "VoID/spatial metadata graphs must not leak into the export");
    }

    @Test
    void ntExportPreservesBaseDirectionOnBothPaths() throws Exception {
        // rdf:dirLangString (format v4): the fastpath's NodeFormatterNT and the
        // generic StreamRDF writer must both emit the @lang--dir form, and stay
        // byte-identical to each other (the fastpath's documented contract).
        String dirTriples =
                "<http://ex.org/a> <http://ex.org/name> \"hello\"@en--ltr .\n"
              + "<http://ex.org/a> <http://ex.org/name> \"shalom\"@he--rtl .\n"
              + "<http://ex.org/a> <http://ex.org/name> \"hello\"@en .\n";
        File h5 = buildStore("dirs", dirTriples);
        Path out = dir.resolve("dirs.nt");

        runExport(h5, "NT", false);
        byte[] fastpath = Files.readAllBytes(out);
        String text = new String(fastpath, StandardCharsets.UTF_8);
        assertTrue(text.contains("\"hello\"@en--ltr") && text.contains("\"shalom\"@he--rtl"),
                "fastpath export must emit base directions:\n" + text);

        Files.delete(out);
        System.setProperty("beakgraph.export.fastpath", "false");
        try {
            runExport(h5, "NT", false);
        } finally {
            System.clearProperty("beakgraph.export.fastpath");
        }
        byte[] generic = Files.readAllBytes(out);
        org.junit.jupiter.api.Assertions.assertArrayEquals(fastpath, generic,
                "fastpath must stay byte-identical to the generic writer");

        Dataset ds = parse(out, Lang.NTRIPLES, false);
        Model expected = ModelFactory.createDefaultModel();
        RDFDataMgr.read(expected, new java.io.ByteArrayInputStream(
                dirTriples.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expected),
                "exported base-direction literals must round-trip term-exactly");
    }

    @Test
    void ntExportRoundTripsCompositeLiteralsOnBothPaths() throws Exception {
        // Composite (cdt:) literals stress the term formatter: the map's lexical
        // form embeds quotes that must be escaped in N-Triples. Fastpath and
        // generic writer must agree byte-for-byte and round-trip term-exactly.
        String cdtTriples =
                "<http://ex.org/a> <http://ex.org/list> \"[1, 2, 3]\"^^<http://w3id.org/awslabs/neptune/SPARQL-CDTs/List> .\n"
              + "<http://ex.org/a> <http://ex.org/map> \"{\\\"k\\\": 5}\"^^<http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map> .\n"
              + "<http://ex.org/a> <http://ex.org/nested> \"[[1, 2], [3]]\"^^<http://w3id.org/awslabs/neptune/SPARQL-CDTs/List> .\n";
        File h5 = buildStore("cdt", cdtTriples);
        Path out = dir.resolve("cdt.nt");

        runExport(h5, "NT", false);
        byte[] fastpath = Files.readAllBytes(out);
        String text = new String(fastpath, StandardCharsets.UTF_8);
        assertTrue(text.contains("{\\\"k\\\": 5}"),
                "map literal quotes must be NT-escaped:\n" + text);

        Files.delete(out);
        System.setProperty("beakgraph.export.fastpath", "false");
        try {
            runExport(h5, "NT", false);
        } finally {
            System.clearProperty("beakgraph.export.fastpath");
        }
        byte[] generic = Files.readAllBytes(out);
        org.junit.jupiter.api.Assertions.assertArrayEquals(fastpath, generic,
                "fastpath must stay byte-identical to the generic writer");

        Dataset ds = parse(out, Lang.NTRIPLES, false);
        Model expected = ModelFactory.createDefaultModel();
        RDFDataMgr.read(expected, new java.io.ByteArrayInputStream(
                cdtTriples.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expected),
                "exported composite literals must round-trip term-exactly");
    }

    @Test
    void ntExportRoundTripsTripleTermsOnBothPaths() throws Exception {
        // RDF 1.2 triple terms (format v5): the fastpath's per-id memoized
        // NodeFormatterNT text and the generic StreamRDF writer must both emit
        // the <<( s p o )>> form - nested terms included - and stay
        // byte-identical to each other.
        String ttTriples =
                "<http://ex.org/r> <http://ex.org/says> <<( <http://ex.org/a> <http://ex.org/b> <http://ex.org/c> )>> .\n"
              + "<http://ex.org/r> <http://ex.org/says2> <<( <http://ex.org/a> <http://ex.org/b> <<( <http://ex.org/x> <http://ex.org/y> \"lit\" )>> )>> .\n"
              + "<http://ex.org/r> <http://ex.org/num> <<( <http://ex.org/a> <http://ex.org/v> \"42\"^^<http://www.w3.org/2001/XMLSchema#int> )>> .\n";
        File h5 = buildStore("tterms", ttTriples);
        Path out = dir.resolve("tterms.nt");

        runExport(h5, "NT", false);
        byte[] fastpath = Files.readAllBytes(out);
        String text = new String(fastpath, StandardCharsets.UTF_8);
        assertTrue(text.contains("<<(") && text.contains(")>>"),
                "fastpath export must emit triple-term syntax:\n" + text);

        Files.delete(out);
        System.setProperty("beakgraph.export.fastpath", "false");
        try {
            runExport(h5, "NT", false);
        } finally {
            System.clearProperty("beakgraph.export.fastpath");
        }
        byte[] generic = Files.readAllBytes(out);
        org.junit.jupiter.api.Assertions.assertArrayEquals(fastpath, generic,
                "fastpath must stay byte-identical to the generic writer");

        Dataset ds = parse(out, Lang.NTRIPLES, false);
        Model expected = ModelFactory.createDefaultModel();
        RDFDataMgr.read(expected, new java.io.ByteArrayInputStream(
                ttTriples.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expected),
                "exported triple terms must round-trip term-exactly");
    }

    @Test
    void ntUpgradesToNqWhenNamedGraphsExist() throws Exception {
        File h5 = buildStore("upg", QUADS);
        runExport(h5, "NT", false);
        assertFalse(Files.exists(dir.resolve("upg.nt")), "NT must not be written for a quad store");
        Path out = dir.resolve("upg.nq");
        assertTrue(Files.exists(out), "the export must upgrade NT -> NQ");
        Dataset ds = parse(out, Lang.NQUADS, false);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expectedTriples()));
        assertTrue(ds.asDatasetGraph().containsGraph(
                        org.apache.jena.graph.NodeFactory.createURI("http://ex.org/g1")),
                "the named graph must survive the round trip");
        assertEquals(1, ds.getNamedModel("http://ex.org/g1").size());
    }

    @Test
    void ttlUpgradesToTrigWhenNamedGraphsExist() throws Exception {
        File h5 = buildStore("upgt", QUADS);
        runExport(h5, "TTL", false);
        assertFalse(Files.exists(dir.resolve("upgt.ttl")));
        Path out = dir.resolve("upgt.trig");
        assertTrue(Files.exists(out), "the export must upgrade TTL -> TRIG");
        Dataset ds = parse(out, Lang.TRIG, false);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expectedTriples()));
        assertEquals(1, ds.getNamedModel("http://ex.org/g1").size());
    }

    @Test
    void ttlStaysTtlForDefaultOnlyStores() throws Exception {
        File h5 = buildStore("ttlonly", TRIPLES);
        runExport(h5, "TTL", false);
        Path out = dir.resolve("ttlonly.ttl");
        assertTrue(Files.exists(out));
        Dataset ds = parse(out, Lang.TURTLE, false);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expectedTriples()));
    }

    @Test
    void compressedExportGetsGzExtensionAndGzipContent() throws Exception {
        File h5 = buildStore("gz", QUADS);
        runExport(h5, "NQ", true);
        Path out = dir.resolve("gz.nq.gz");
        assertTrue(Files.exists(out), "-compress must add .gz");
        Dataset ds = parse(out, Lang.NQUADS, true);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expectedTriples()));
        assertEquals(1, ds.getNamedModel("http://ex.org/g1").size());
    }

    @Test
    void jsonLdExportRoundTrips() throws Exception {
        File h5 = buildStore("jld", QUADS);
        runExport(h5, "JSON-LD", false);
        Path out = dir.resolve("jld.jsonld");
        assertTrue(Files.exists(out));
        Dataset ds = parse(out, Lang.JSONLD, false);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expectedTriples()));
        assertEquals(1, ds.getNamedModel("http://ex.org/g1").size());
    }

    @Test
    void directorySourceExportsEveryStore() throws Exception {
        Path sub = Files.createDirectories(dir.resolve("many"));
        for (String name : new String[]{"one", "two"}) {
            File src = sub.resolve(name + ".nt").toFile();
            Files.write(src.toPath(), TRIPLES.getBytes(StandardCharsets.UTF_8));
            HDF5Writer.Builder().setSource(src).setDestination(sub.resolve(name + ".h5").toFile())
                    .build().write();
        }
        runExport(sub.toFile(), "NT", false);
        assertTrue(Files.exists(sub.resolve("one.nt")));
        assertTrue(Files.exists(sub.resolve("two.nt")));
        Dataset ds = parse(sub.resolve("two.nt"), Lang.NTRIPLES, false);
        assertTrue(ds.getDefaultModel().isIsomorphicWith(expectedTriples()));
    }

    @Test
    void exportOptionParsesAndRejectsUnknownFormats() {
        Parameters p = new Parameters();
        com.beust.jcommander.JCommander.newBuilder().addObject(p).build()
                .parse("-src", "x.h5", "-export", "json-ld", "-compress");
        assertEquals("json-ld", p.export);
        assertTrue(p.compress);
        assertEquals("JSONLD", ExportFormatValidator.normalize(p.export));
        org.junit.jupiter.api.Assertions.assertThrows(com.beust.jcommander.ParameterException.class,
                () -> com.beust.jcommander.JCommander.newBuilder().addObject(new Parameters()).build()
                        .parse("-src", "x.h5", "-export", "RDFXML"),
                "unsupported export formats must be rejected");
    }
}
