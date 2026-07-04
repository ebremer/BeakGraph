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

        assertEquals(5, cli.getFileCounter().getRDFFileCount());
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
        p.parallel = true;
        p.cores = 2;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.merge();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "the parallel merge must succeed");
        assertMerged(p.dest);
    }

    @Test
    void mergeWithHugeWriter() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                com.ebremer.beakgraph.huge.NativeHdf5File.isAvailable(),
                "native HDF5 library unavailable");
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
