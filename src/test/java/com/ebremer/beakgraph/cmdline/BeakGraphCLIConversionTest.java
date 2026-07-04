package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Conversion-run accounting. FileProcessor runs inside Futures that are never
 * inspected, so before the fix any failure thrown outside its try block -
 * including the NPE from a missing -dest - was swallowed by FutureTask: the
 * run logged nothing, counted nothing, and reported every file as a
 * successful conversion.
 */
class BeakGraphCLIConversionTest {

    @TempDir
    Path dir;

    @Test
    void failedConversionsAreCountedAndGoodFilesStillConvert() throws Exception {
        Path src = Files.createDirectories(dir.resolve("src"));
        Files.write(src.resolve("good.ttl"),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));
        Files.write(src.resolve("bad.ttl"),
                "this is not turtle @@@\n".getBytes(StandardCharsets.UTF_8));

        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("out").toFile();

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();

        assertEquals(2, cli.getFileCounter().getRDFFileCount());
        assertEquals(1, cli.getFileCounter().getFailedConversionFileCount(),
                "the unparseable source must be counted as a failed conversion");
        File goodH5 = dir.resolve("out").resolve("good.h5").toFile();
        assertTrue(goodH5.exists() && goodH5.length() > 0,
                "the well-formed source must still convert");
    }

    @Test
    void hugeFlagConvertsThroughDiskBasedWriter() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                com.ebremer.beakgraph.huge.NativeHdf5File.isAvailable(),
                "native HDF5 library unavailable");
        Path src = Files.createDirectories(dir.resolve("srchuge"));
        Files.write(src.resolve("data.ttl"),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));

        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outhuge").toFile();
        p.huge = true;
        p.workdir = dir.resolve("workhuge").toFile(); // exercised even when absent: CLI creates it

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(),
                "the -huge conversion must succeed");
        File h5 = dir.resolve("outhuge").resolve("data.h5").toFile();
        assertTrue(h5.exists() && h5.length() > 0, "the -huge writer must produce the .h5 file");
        // The produced store must be readable by the standard reader stack.
        try (com.ebremer.beakgraph.core.BeakGraph bg = new com.ebremer.beakgraph.core.BeakGraph(
                new com.ebremer.beakgraph.hdf5.readers.HDF5Reader(h5))) {
            assertTrue(bg.find(org.apache.jena.graph.NodeFactory.createURI("http://ex.org/a"),
                               org.apache.jena.graph.NodeFactory.createURI("http://ex.org/p"),
                               org.apache.jena.graph.NodeFactory.createURI("http://ex.org/b")).hasNext(),
                    "the converted triple must be queryable");
        }
    }

    @Test
    void parallelFlagConvertsThroughParallelWriter() throws Exception {
        Path src = Files.createDirectories(dir.resolve("srcpar"));
        Files.write(src.resolve("data.ttl"),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));

        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("outpar").toFile();
        p.parallel = true;
        p.cores = 2;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(),
                "the -parallel conversion must succeed");
        File h5 = dir.resolve("outpar").resolve("data.h5").toFile();
        assertTrue(h5.exists() && h5.length() > 0, "the -parallel writer must produce the .h5 file");
        // The produced store must be readable by the standard reader stack.
        try (com.ebremer.beakgraph.core.BeakGraph bg = new com.ebremer.beakgraph.core.BeakGraph(
                new com.ebremer.beakgraph.hdf5.readers.HDF5Reader(h5))) {
            assertTrue(bg.find(org.apache.jena.graph.NodeFactory.createURI("http://ex.org/a"),
                               org.apache.jena.graph.NodeFactory.createURI("http://ex.org/p"),
                               org.apache.jena.graph.NodeFactory.createURI("http://ex.org/b")).hasNext(),
                    "the converted triple must be queryable");
        }
    }

    @Test
    void parallelAndCoresOptionsParse() {
        // The exact spellings the writer is documented with: -parallel and -cores.
        Parameters p = new Parameters();
        com.beust.jcommander.JCommander.newBuilder().addObject(p).build()
                .parse("-src", "x", "-parallel", "-cores", "6");
        assertTrue(p.parallel, "-parallel must set the flag");
        assertEquals(6, p.cores, "-cores must override the default");
        assertEquals(4, new Parameters().cores, "-cores must default to 4");
    }

    @Test
    void missingDestinationFailsEveryFileLoudlyInsteadOfSilently() throws Exception {
        // main() refuses to start without -dest; if a processor is ever reached
        // without one anyway, the failure must land in the counter, not vanish
        // inside a discarded Future.
        Path src = Files.createDirectories(dir.resolve("src2"));
        Files.write(src.resolve("a.ttl"),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));

        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = null;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();

        assertEquals(1, cli.getFileCounter().getFailedConversionFileCount());
    }
}
