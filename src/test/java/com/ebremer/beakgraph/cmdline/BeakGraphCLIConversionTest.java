package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import java.util.List;

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
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
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

    private void convertsWithMethod(int method, String tag) throws Exception {
        Path src = Files.createDirectories(dir.resolve("src" + tag));
        Files.write(src.resolve("data.ttl"),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));

        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("out" + tag).toFile();
        p.method = method;
        p.cores = 2;

        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();

        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(),
                "the -method " + method + " conversion must succeed");
        File h5 = dir.resolve("out" + tag).resolve("data.h5").toFile();
        assertTrue(h5.exists() && h5.length() > 0, "the -method " + method + " writer must produce the .h5 file");
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
    void methodTwoConvertsThroughParallelWriter() throws Exception {
        convertsWithMethod(2, "par");
    }

    @Test
    void methodThreeConvertsThroughUltraWriter() throws Exception {
        convertsWithMethod(3, "ultra");
    }

    @Test
    void methodAndCoresOptionsParse() {
        // The exact spellings the writers are documented with: -method and -cores.
        Parameters p = new Parameters();
        com.beust.jcommander.JCommander.newBuilder().addObject(p).build()
                .parse("-src", "x", "-method", "3", "-cores", "6");
        assertEquals(3, p.method, "-method must set the engine");
        assertEquals(6, p.cores, "-cores must override the default");
        assertEquals(4, new Parameters().cores, "-cores must default to 4");
        assertEquals(0, new Parameters().method, "-method must default to the in-memory writer");
        org.junit.jupiter.api.Assertions.assertThrows(com.beust.jcommander.ParameterException.class,
                () -> com.beust.jcommander.JCommander.newBuilder().addObject(new Parameters()).build()
                        .parse("-src", "x", "-method", "6"),
                "-method outside 0..5 must be rejected");
    }

    @Test
    void methodFiveConvertsThroughPlaidWriter() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        convertsWithMethod(5, "plaid");
    }

    @Test
    void methodFourConvertsThroughHugeUltraWriter() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        convertsWithMethod(4, "hugeultra");
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

    // --- BG-417: "-src ." ----------------------------------------------------

    @Test
    void relativeSourceDirectoryMapsEveryFile() {
        // Path.of(".").normalize() is the empty path and nothing startsWith it:
        // every file under "-src ." used to be rejected as "not under -src".
        Path out = Path.of("out");
        assertEquals(out.toAbsolutePath().normalize().resolve("a.h5"),
                BeakGraphCLI.mapToDestinationWithNewExtension(Path.of("./a.ttl"), Path.of("."), out, "h5"));
        assertEquals(out.toAbsolutePath().normalize().resolve("sub").resolve("b.h5"),
                BeakGraphCLI.mapToDestinationWithNewExtension(Path.of("./sub/b.nt"), Path.of("."), out, "h5"));
        assertEquals(out.toAbsolutePath().normalize().resolve("c.h5"),
                BeakGraphCLI.mapToDestinationWithNewExtension(Path.of("c.ttl"), Path.of(""), out, "h5"));
    }

    // --- BG-261: a single-file -src ---------------------------------------------

    @Test
    void singleFileSourceWritesTheNamedDestination() throws Exception {
        Path src = dir.resolve("single.ttl");
        Files.writeString(src, "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n", StandardCharsets.UTF_8);
        Path outFile = dir.resolve("named").resolve("out.h5");
        Files.createDirectories(outFile.getParent());
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = outFile.toFile();
        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();
        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount());
        assertTrue(outFile.toFile().isFile() && outFile.toFile().length() > 0, "-dest names the output file");
        assertFalse(outFile.resolve(".h5").toFile().exists(), "-dest must not become a directory holding '.h5'");

        // -dest as an existing directory: <dest>/<name>.h5
        Path outDir = Files.createDirectories(dir.resolve("outdir"));
        p.dest = outDir.toFile();
        cli = new BeakGraphCLI(p);
        cli.traverse();
        assertEquals(0, cli.getFileCounter().getFailedConversionFileCount());
        assertTrue(outDir.resolve("single.h5").toFile().isFile());
    }

    // --- BG-420: colliding destinations and skipped existing files ------------

    @Test
    void sourcesDifferingOnlyByExtensionAreReportedNotRaced() throws Exception {
        Path src = Files.createDirectories(dir.resolve("collide"));
        Files.writeString(src.resolve("a.ttl"), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n", StandardCharsets.UTF_8);
        Files.writeString(src.resolve("a.nt"), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/c> .\n", StandardCharsets.UTF_8);
        Files.writeString(src.resolve("b.ttl"), "<http://ex.org/b> <http://ex.org/p> <http://ex.org/c> .\n", StandardCharsets.UTF_8);
        var plan = BeakGraphCLI.planDestinations(List.of(src.resolve("a.nt"), src.resolve("a.ttl"), src.resolve("b.ttl")),
                src.toFile(), dir.resolve("collide-out").toFile());
        assertEquals(2, plan.size());
        assertEquals(2, plan.get(dir.resolve("collide-out").toAbsolutePath().normalize().resolve("a.h5")).size());

        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("collide-out").toFile();
        p.threads = 2;
        BeakGraphCLI cli = new BeakGraphCLI(p);
        cli.traverse();
        assertEquals(3, cli.getFileCounter().getRDFFileCount());
        assertEquals(1, cli.getFileCounter().getFailedConversionFileCount(), "the second source for a.h5 is a reported failure");
        assertTrue(dir.resolve("collide-out").resolve("a.h5").toFile().length() > 0);
        assertTrue(dir.resolve("collide-out").resolve("b.h5").toFile().length() > 0);
        try (var tmps = Files.list(dir.resolve("collide-out"))) {
            assertTrue(tmps.noneMatch(f -> f.getFileName().toString().endsWith(".tmp")), "no temp file left behind");
        }
    }

    @Test
    void existingDestinationsAreSkippedAndCounted() throws Exception {
        Path src = Files.createDirectories(dir.resolve("skip"));
        Files.writeString(src.resolve("s.ttl"), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n", StandardCharsets.UTF_8);
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("skip-out").toFile();
        new BeakGraphCLI(p).traverse();
        BeakGraphCLI second = new BeakGraphCLI(p);
        second.traverse();
        assertEquals(1, second.getFileCounter().getSkippedExistingCount(), "the existing .h5 is skipped, and says so");
        assertEquals(0, second.getFileCounter().getFailedConversionFileCount());
        assertTrue(second.getFileCounter().toString().contains("Skipped (existing)     : 1"));
    }

    // --- BG-143: an Error is a failed conversion, not a silent success ---------

    @Test
    void anErrorInTheWriterIsCountedAsAFailure() throws Exception {
        Path src = Files.createDirectories(dir.resolve("oom"));
        Files.writeString(src.resolve("big.ttl"), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n", StandardCharsets.UTF_8);
        Parameters p = new Parameters();
        p.src = src.toFile();
        p.dest = dir.resolve("oom-out").toFile();
        BeakGraphCLI cli = new BeakGraphCLI(p) {
            @Override
            com.ebremer.beakgraph.core.BeakGraphWriter newWriter(File source, List<File> sources, File dest) {
                return () -> { throw new OutOfMemoryError("simulated"); };
            }
        };
        cli.traverse();
        assertEquals(1, cli.getFileCounter().getRDFFileCount());
        assertEquals(1, cli.getFileCounter().getFailedConversionFileCount(),
                "an Error escaping the writer used to be swallowed by the FutureTask and counted as success");
        assertEquals(0, cli.getFileCounter().getSuccessfulConversionCount());
    }
}
