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
