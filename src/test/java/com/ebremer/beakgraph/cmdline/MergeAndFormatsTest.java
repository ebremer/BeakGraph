package com.ebremer.beakgraph.cmdline;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two source-handling features of the CLI: every supported syntax
 * (Turtle, N-Triples, N-Quads, TriG, RDF/XML, JSON-LD - plain, .gz, .zip)
 * converts, and -merge folds a whole source tree into ONE store with
 * per-document blank-node scoping.
 */
class MergeAndFormatsTest {

    @TempDir
    Path dir;

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static void write(Path p, String content) throws Exception {
        Files.write(p, content.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeGz(Path p, String content) throws Exception {
        try (GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(p.toFile()))) {
            gz.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void writeZip(Path p, String entryName, String content) throws Exception {
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(p.toFile()))) {
            zos.putNextEntry(new ZipEntry(entryName));
            zos.write(content.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }
    }

    private static boolean ask(File h5, String query) throws Exception {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                    .query(QueryFactory.create(query)).build()) {
                return qe.execAsk();
            }
        }
    }

    private static long count(File h5, String selectCountQuery) throws Exception {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                    .query(QueryFactory.create(selectCountQuery)).build()) {
                return qe.execSelect().next().getLiteral("n").getLong();
            }
        }
    }

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    @Test
    void sourceFilterAcceptsExactlyTheSupportedNames() {
        for (String ext : new String[]{"ttl", "nt", "nq", "trig", "rdf", "jsonld"}) {
            assertTrue(RdfSources.isSupported("data." + ext), ext);
            assertTrue(RdfSources.isSupported("data." + ext + ".gz"), ext + ".gz");
            assertTrue(RdfSources.isSupported("data." + ext + ".zip"), ext + ".zip");
            assertTrue(RdfSources.isSupported("DATA." + ext.toUpperCase() + ".GZ"), "case-insensitive " + ext);
        }
        assertFalse(RdfSources.isSupported("data.txt"));
        assertFalse(RdfSources.isSupported("data.h5"));
        assertFalse(RdfSources.isSupported("data.zip"), "a zip without an RDF extension is not identifiable");
        assertFalse(RdfSources.isSupported("data.gz"));
        assertFalse(RdfSources.isSupported("data.owl"));
    }

    @Test
    void allFormatsConvertIndividually() throws Exception {
        Path src = Files.createDirectories(dir.resolve("srcfmt"));
        write(src.resolve("a.ttl"), "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o-ttl> .\n");
        write(src.resolve("b.nt"), "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o-nt> .\n");
        write(src.resolve("c.nq"), "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o-nq> <http://ex.org/g-nq> .\n");
        write(src.resolve("d.trig"), "@prefix ex: <http://ex.org/> . ex:gtrig { ex:s ex:p ex:otrig . }\n");
        write(src.resolve("e.rdf"), """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://ex.org/">
              <rdf:Description rdf:about="http://ex.org/s">
                <ex:p rdf:resource="http://ex.org/o-rdf"/>
              </rdf:Description>
            </rdf:RDF>
            """);
        write(src.resolve("f.jsonld"), """
            {"@id": "http://ex.org/s", "http://ex.org/p": {"@id": "http://ex.org/o-jsonld"}}
            """);
        writeGz(src.resolve("g.nt.gz"), "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o-ntgz> .\n");
        writeZip(src.resolve("h.trig.zip"), "h.trig",
                "<http://ex.org/g-zip> { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-trigzip> . }\n");

        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outfmt").toFile();
        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();

        assertEquals(8, cli.getFileCounter().getRDFFileCount(), "every format must pass the source filter");
        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "every format must convert");

        Path out = dir.resolve("outfmt");
        assertTrue(ask(out.resolve("a.h5").toFile(), "ASK { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-ttl> }"));
        assertTrue(ask(out.resolve("b.h5").toFile(), "ASK { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-nt> }"));
        assertTrue(ask(out.resolve("c.h5").toFile(), "ASK { GRAPH <http://ex.org/g-nq> { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-nq> } }"));
        assertTrue(ask(out.resolve("d.h5").toFile(), "ASK { GRAPH <http://ex.org/gtrig> { <http://ex.org/s> <http://ex.org/p> <http://ex.org/otrig> } }"));
        assertTrue(ask(out.resolve("e.h5").toFile(), "ASK { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-rdf> }"));
        assertTrue(ask(out.resolve("f.h5").toFile(), "ASK { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-jsonld> }"));
        // .gz / .zip keep the inner extension in the mapped name (last extension is replaced)
        assertTrue(ask(out.resolve("g.nt.h5").toFile(), "ASK { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-ntgz> }"));
        assertTrue(ask(out.resolve("h.trig.h5").toFile(), "ASK { GRAPH <http://ex.org/g-zip> { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o-trigzip> } }"));
    }

    private Path mergeSourceTree(String name) throws Exception {
        Path src = Files.createDirectories(dir.resolve(name));
        write(src.resolve("m1.ttl"), "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o1> .\n");
        write(src.resolve("m2.nq"), "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o2> <http://ex.org/gm> .\n");
        writeZip(src.resolve("m3.nt.zip"), "m3.nt", "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o3> .\n");
        // The SAME bnode label in two documents: they are distinct nodes and
        // must stay distinct in the merged store.
        write(src.resolve("bn1.ttl"), "_:b0 <http://ex.org/bp> \"v1\" .\n");
        write(src.resolve("bn2.ttl"), "_:b0 <http://ex.org/bp> \"v2\" .\n");
        // BG-390: the SAME relative references in two documents are distinct
        // resources; stored relative to -src they must stay apart, while
        // <../shared.png> from both lands on the one root-level resource.
        Files.createDirectories(src.resolve("a"));
        Files.createDirectories(src.resolve("b"));
        write(src.resolve("a").resolve("x.ttl"), "<> <http://ex.org/label> \"doc a\" ; <http://ex.org/thumb> <img.png> ; <http://ex.org/up> <../shared.png> .\n");
        write(src.resolve("b").resolve("y.ttl"), "<> <http://ex.org/label> \"doc b\" ; <http://ex.org/thumb> <img.png> ; <http://ex.org/up> <../shared.png> .\n");
        // BG-429: JSON-LD with an inline context, a @list container, a @graph
        // block and a blank node; RDF/XML with parseType="Collection" and
        // rdf:nodeID - every engine's merge parses them (one parser
        // configuration, RdfSources.parser).
        write(src.resolve("m4.jsonld"), """
            {
              "@context": {"ex": "http://ex.org/", "items": {"@id": "ex:items", "@container": "@list"}},
              "@graph": [
                {"@id": "ex:jdoc", "items": [{"@id": "ex:i1"}, {"@id": "ex:i2"}], "ex:anon": {"ex:tag": "from jsonld"}},
                {"@id": "ex:gjson", "@graph": [{"@id": "ex:s", "ex:p": {"@id": "ex:o4"}}]}
              ]
            }
            """);
        write(src.resolve("m5.rdf"), """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://ex.org/">
              <rdf:Description rdf:about="http://ex.org/xdoc">
                <ex:parts rdf:parseType="Collection">
                  <rdf:Description rdf:about="http://ex.org/x1"/>
                  <rdf:Description rdf:about="http://ex.org/x2"/>
                </ex:parts>
                <ex:anon rdf:nodeID="n1"/>
              </rdf:Description>
              <rdf:Description rdf:nodeID="n1"><ex:tag>from rdfxml</ex:tag></rdf:Description>
            </rdf:RDF>
            """);
        return src;
    }

    private void assertMerged(File h5) throws Exception {
        assertTrue(h5.exists() && h5.length() > 0, "merged store must exist: " + h5);
        assertTrue(ask(h5, "ASK { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o1> }"), "from m1.ttl");
        assertTrue(ask(h5, "ASK { GRAPH <http://ex.org/gm> { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o2> } }"), "from m2.nq");
        assertTrue(ask(h5, "ASK { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o3> }"), "from m3.nt.zip");
        assertTrue(ask(h5, "ASK { ?b <http://ex.org/bp> \"v1\" }"), "bnode statement from bn1.ttl");
        assertTrue(ask(h5, "ASK { ?b <http://ex.org/bp> \"v2\" }"), "bnode statement from bn2.ttl");
        assertEquals(2, count(h5, "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?o }"),
                "_:b0 from two documents must remain two distinct blank nodes");
        // Relative references are stored relative to -src (STR() compares the
        // stored relative form; a <...> in the query would resolve absolutely).
        assertTrue(ask(h5, "ASK { ?s <http://ex.org/label> \"doc a\" FILTER(STR(?s) = \"a/x.ttl\") }"), "<> of a/x.ttl");
        assertTrue(ask(h5, "ASK { ?s <http://ex.org/label> \"doc b\" FILTER(STR(?s) = \"b/y.ttl\") }"), "<> of b/y.ttl");
        assertTrue(ask(h5, "ASK { ?s <http://ex.org/thumb> ?o FILTER(STR(?s) = \"a/x.ttl\" && STR(?o) = \"a/img.png\") }"), "<img.png> of a/x.ttl");
        assertTrue(ask(h5, "ASK { ?s <http://ex.org/thumb> ?o FILTER(STR(?s) = \"b/y.ttl\" && STR(?o) = \"b/img.png\") }"), "<img.png> of b/y.ttl");
        assertEquals(2, count(h5, "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s <http://ex.org/label> ?l }"),
                "<> from two documents must remain two distinct subjects");
        assertEquals(1, count(h5, "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s <http://ex.org/up> ?o FILTER(STR(?o) = \"shared.png\") }"),
                "<../shared.png> from both documents is the one root-level resource");
        // JSON-LD: named graph from @graph, RDF list from the @list container, blank node.
        String rdf = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ";
        assertTrue(ask(h5, "ASK { GRAPH <http://ex.org/gjson> { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o4> } }"), "@graph block from m4.jsonld");
        assertTrue(ask(h5, rdf + "ASK { <http://ex.org/jdoc> <http://ex.org/items> ?l . ?l rdf:first <http://ex.org/i1> ; rdf:rest ?r . ?r rdf:first <http://ex.org/i2> ; rdf:rest rdf:nil }"),
                "@list container from m4.jsonld");
        assertTrue(ask(h5, "ASK { <http://ex.org/jdoc> <http://ex.org/anon> ?b . ?b <http://ex.org/tag> \"from jsonld\" FILTER(isBlank(?b)) }"), "blank node from m4.jsonld");
        // RDF/XML: parseType="Collection" list, rdf:nodeID blank node.
        assertTrue(ask(h5, rdf + "ASK { <http://ex.org/xdoc> <http://ex.org/parts> ?l . ?l rdf:first <http://ex.org/x1> ; rdf:rest ?r . ?r rdf:first <http://ex.org/x2> ; rdf:rest rdf:nil }"),
                "parseType=Collection from m5.rdf");
        assertTrue(ask(h5, "ASK { <http://ex.org/xdoc> <http://ex.org/anon> ?b . ?b <http://ex.org/tag> \"from rdfxml\" FILTER(isBlank(?b)) }"), "rdf:nodeID from m5.rdf");
        assertEquals(2, count(h5, "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/tag> ?t }"),
                "the JSON-LD and RDF/XML blank nodes are distinct from each other (and from bn1/bn2's)");
        assertEquals(4, count(h5, "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { { ?b <http://ex.org/tag> ?t } UNION { ?b <http://ex.org/bp> ?v } }"),
                "four documents, four blank nodes");
    }

    @Test
    void mergeCombinesAllSourcesIntoOneFile() throws Exception {
        Path src = mergeSourceTree("srcmerge");
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outmerge").resolve("all.h5").toFile();
        p.merge = true;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(9, cli.getFileCounter().getRDFFileCount());
        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "the merge must succeed");
        assertMerged(p.dest);
        assertEquals(1, p.dest.getParentFile().listFiles().length, "exactly one output file, no per-source .h5");
    }

    @Test
    void mergeWithParallelWriter() throws Exception {
        Path src = mergeSourceTree("srcmergepar");
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outmergepar").resolve("all.h5").toFile();
        p.merge = true;
        p.method = 2;
        p.cores = 2;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "the parallel merge must succeed");
        assertMerged(p.dest);
    }

    @Test
    void mergeWithUltraWriter() throws Exception {
        Path src = mergeSourceTree("srcmergeultra");
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outmergeultra").resolve("all.h5").toFile();
        p.merge = true;
        p.method = 3;
        p.cores = 2;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "the ultra merge must succeed");
        assertMerged(p.dest);
    }

    @Test
    void mergeWithHugeWriter() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Path src = mergeSourceTree("srcmergehuge");
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outmergehuge").resolve("all.h5").toFile();
        p.merge = true;
        p.huge = true;
        p.workdir = dir.resolve("workmergehuge").toFile();

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "the -huge merge must succeed");
        assertMerged(p.dest);
    }

    @Test
    void mergeWithHugeUltraWriter() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Path src = mergeSourceTree("srcmergehugeultra");
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outmergehugeultra").resolve("all.h5").toFile();
        p.merge = true;
        p.method = 4;
        p.cores = 3;
        p.workdir = dir.resolve("workmergehugeultra").toFile();

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "the -method 4 merge must succeed");
        assertMerged(p.dest);
    }

    @Test
    void mergeWithPlaidWriter() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Path src = mergeSourceTree("srcmergeplaid");
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outmergeplaid").resolve("all.h5").toFile();
        p.merge = true;
        p.method = 5;
        p.cores = 3;
        p.workdir = dir.resolve("workmergeplaid").toFile();

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "the -method 5 merge must succeed");
        assertMerged(p.dest);
    }

    /**
     * BG-422: -dest names a directory by INTENT, not only when it already
     * exists - a trailing separator or a name without a store suffix means
     * "put merged.h5 in there"; a name ending in .h5 is the file, and its
     * missing parent directories are created.
     */
    @Test
    void mergeDestinationIntentIsHonouredForDirectoriesThatDoNotExistYet() throws Exception {
        Path src = mergeSourceTree("srcmergeintent");
        String[][] cases = {
            {dir.resolve("outA").toString() + File.separator, "outA/merged.h5"},
            {dir.resolve("outB").toString(), "outB/merged.h5"},
            {dir.resolve("outC").resolve("store.h5").toString(), "outC/store.h5"},
            {dir.resolve("outD").resolve("store.HDF5").toString(), "outD/store.HDF5"},
        };
        for (String[] c : cases) {
            Parameters p = new Parameters();
            com.beust.jcommander.JCommander.newBuilder().addObject(p).build()
                    .parse("-src", src.toString(), "-dest", c[0], "-merge");
            assertTrue(p.dest instanceof Parameters.DestinationFile, "the -dest converter keeps the raw argument");
            BeakGraphCLI cli = new BeakGraphCLI(p);
            cli.merge();
            assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), c[0]);
            File expected = dir.resolve(c[1]).toFile();
            assertMerged(expected);
            if (c[1].endsWith("merged.h5")) {
                assertFalse(new File(c[0].replaceAll("[/\\\\]+$", "")).isFile(), c[0] + " must not be written as a suffix-less file");
            }
        }
        assertTrue(((Parameters.DestinationFile) parseDest(dir.resolve("x").toString() + "/")).trailingSeparator());
        assertFalse(((Parameters.DestinationFile) parseDest(dir.resolve("x").toString())).trailingSeparator());
    }

    private static File parseDest(String arg) {
        Parameters p = new Parameters();
        com.beust.jcommander.JCommander.newBuilder().addObject(p).build().parse("-dest", arg);
        return p.dest;
    }

    @Test
    void mergeIntoExistingDirectoryWritesMergedH5() throws Exception {
        Path src = mergeSourceTree("srcmergedir");
        Path out = Files.createDirectories(dir.resolve("outmergedir"));
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = out.toFile();
        p.merge = true;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount());
        assertMerged(out.resolve("merged.h5").toFile());
    }
}
