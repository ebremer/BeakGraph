package com.ebremer.beakgraph.cmdline;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The -verify contract: a healthy file passes (structurally and with -deep),
 * every flavor of damage - empty, truncated, garbage, valid-HDF5-but-not-
 * BeakGraph - fails with exit code 2 and a reason, directories recurse, and
 * an empty scan is its own error (exit 1), never a false "all good".
 */
class VerifyCommandTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:s1 ex:p ex:o1 .
        ex:s1 ex:p "plain" .
        ex:s2 ex:p "hello"@en .
        ex:s2 ex:q "42"^^xsd:integer .
        ex:s3 ex:p ex:o2 .
        """;

    @TempDir
    static Path dir;
    static File good;

    record Result(int code, String output) {}

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("verify.ttl").toFile();
        good = dir.resolve("verify.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(good)
                .setSpatial(false).setFeatures(false)
                .build().write();
    }

    private static Result verify(File target, boolean deep) {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        int code = new VerifyCommand(target, deep, new PrintStream(captured, true)).run();
        return new Result(code, captured.toString());
    }

    @Test
    void healthyFilePassesStructurallyAndDeep() {
        Result structural = verify(good, false);
        assertEquals(0, structural.code(), structural.output());
        assertTrue(structural.output().contains("OK    "), structural.output());
        assertTrue(structural.output().contains("1 OK, 0 FAILED"), structural.output());

        Result deep = verify(good, true);
        assertEquals(0, deep.code(), deep.output());
        assertTrue(deep.output().contains("(deep)"), deep.output());
    }

    @Test
    void tripleTermStorePassesDeepVerify() throws Exception {
        // -deep materializes every triple, which resolves every triple-term row
        // through the cross-dictionary component store - resolution IS the
        // traversal, so a component-id fault would surface as a shortfall here.
        String ttl = """
            @prefix ex: <http://ex.org/> .
            ex:r ex:says <<( ex:a ex:b ex:c )>> .
            ex:r ex:says2 <<( ex:a ex:b <<( ex:x ex:y "lit" )>> )>> .
            ex:s1 ex:p ex:o1 .
            """;
        File src = dir.resolve("ttverify.ttl").toFile();
        File h5 = dir.resolve("ttverify.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        Result deep = verify(h5, true);
        assertEquals(0, deep.code(), deep.output());
        assertTrue(deep.output().contains("1 OK, 0 FAILED"), deep.output());
    }

    @Test
    void emptyFileFails() throws Exception {
        Path empty = dir.resolve("empty.h5");
        Files.write(empty, new byte[0]);
        Result r = verify(empty.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("empty file"), r.output());
    }

    @Test
    void truncatedFileFails() throws Exception {
        Path truncated = dir.resolve("truncated.h5");
        Files.copy(good.toPath(), truncated);
        try (FileChannel fc = FileChannel.open(truncated, StandardOpenOption.WRITE)) {
            fc.truncate(good.length() * 3 / 4);
        }
        Result r = verify(truncated.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("FAIL"), r.output());
    }

    @Test
    void garbageBytesFail() throws Exception {
        Path garbage = dir.resolve("garbage.h5");
        byte[] junk = new byte[4096];
        for (int i = 0; i < junk.length; i++) {
            junk[i] = (byte) (i * 31 + 7);
        }
        Files.write(garbage, junk);
        Result r = verify(garbage.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("cannot open"), r.output());
    }

    @Test
    void validHdf5ButNotBeakGraphFails() throws Exception {
        Path foreign = dir.resolve("foreign.h5");
        try (WritableHdfFile out = HdfFile.write(foreign)) {
            out.putGroup("SomethingElse").putAttribute("hello", 1);
        }
        Result r = verify(foreign.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("Not a BeakGraph"), r.output());
    }

    @Test
    void directoryRecursesIntoSubdirectoriesAndContinuesPastFailures() throws Exception {
        Path tree = dir.resolve("tree");
        Path nested = tree.resolve("a/b");
        Files.createDirectories(nested);
        Files.copy(good.toPath(), tree.resolve("good.h5"));
        Files.copy(good.toPath(), nested.resolve("nested-good.hdf5")); // .hdf5 also picked up
        Files.write(nested.resolve("bad.h5"), new byte[0]);
        Files.write(tree.resolve("ignored.txt"), "not hdf5".getBytes(StandardCharsets.UTF_8));

        Result r = verify(tree.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("good.h5"), r.output());
        assertTrue(r.output().contains("nested-good.hdf5"), r.output());
        assertTrue(r.output().contains("bad.h5"), r.output());
        assertFalse(r.output().contains("ignored.txt"), r.output());
        assertTrue(r.output().contains("Verified 3 file(s): 2 OK, 1 FAILED"), r.output());
    }

    @Test
    void nothingToVerifyIsAnErrorNotSuccess() throws Exception {
        Path emptyDir = dir.resolve("nothing-here");
        Files.createDirectories(emptyDir);
        Result r = verify(emptyDir.toFile(), false);
        assertEquals(1, r.code(), r.output());
        assertTrue(r.output().contains("No BeakGraph"), r.output());
    }
}
