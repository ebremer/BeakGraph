package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * {@code -arrays} on the disk engines (methods 1, 4 and 5): they classify the
 * sequences as the literals encode reaches them, spill the rows, and write
 * each class from its own spill through the in-memory engines' class writer,
 * so their stores are byte-identical to method 0's, arrays and all, in both
 * containers, raw and compressed.
 */
public class DiskEngineArraysTest {

    @TempDir
    Path dir;

    /** The numeric fixture and the record fixture together. */
    public static String data() {
        return SequenceArraysTest.DATA + RecordClassesTest.DATA
                .replace("@prefix : <http://ex.org/> .", "")
                .replace("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .", "");
    }

    private static BeakGraphWriter writer(int method, File src, File dest, SequenceArrays.Options o) throws Exception {
        return switch (method) {
            case 0 -> HDF5Writer.Builder().setSource(src).setDestination(dest).setArrays(o).build();
            case 1 -> {
                HugeHDF5Writer.Builder b = HugeHDF5Writer.Builder().setDestination(dest).setArrays(o);
                b.setSource(src);
                yield b.build();
            }
            case 4 -> {
                HugeUltraHDF5Writer.Builder b = HugeUltraHDF5Writer.Builder().setDestination(dest).setArrays(o).setCores(2);
                b.setSource(src);
                yield b.build();
            }
            case 5 -> {
                PlaidHDF5Writer.Builder b = PlaidHDF5Writer.Builder().setDestination(dest).setArrays(o).setCores(2);
                b.setSource(src);
                yield b.build();
            }
            default -> throw new IllegalArgumentException("method " + method);
        };
    }

    @ParameterizedTest
    @CsvSource({"1,.h5,false", "1,.zarr.zip,false", "1,.h5,true", "1,.zarr.zip,true",
                "4,.h5,false", "4,.zarr.zip,true", "5,.h5,true", "5,.zarr.zip,false"})
    void aDiskEngineWritesMethodZerosArrays(int method, String ext, boolean compress) throws Exception {
        Path src = dir.resolve("data.ttl");
        Files.writeString(src, data());
        SequenceArrays.Options o = new SequenceArrays.Options(1, 2, compress);
        File m0 = dir.resolve("m0" + ext).toFile();
        File disk = dir.resolve("m" + method + ext).toFile();
        writer(0, src.toFile(), m0, o).write();
        writer(method, src.toFile(), disk, o).write();
        assertArrayEquals(Files.readAllBytes(m0.toPath()), Files.readAllBytes(disk.toPath()), "method " + method);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int exit = new com.ebremer.beakgraph.cmdline.VerifyCommand(disk, true, new java.io.PrintStream(out, true)).run();
        assertEquals(0, exit, out.toString());
    }

    /**
     * Thousands of sequences at the default minimum of 16 rows: tensors of
     * several lengths, a ragged rest, readings and component lists as
     * records, rare signatures left out, inner-only sequences, and enough
     * sequences for {@code sequenceArrays} to span blocks.
     */
    @ParameterizedTest
    @CsvSource({"1,.h5", "4,.zarr.zip"})
    void manyRowsAndClassesMatchToo(int method, String ext) throws Exception {
        StringBuilder ttl = new StringBuilder("@prefix : <http://ex.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
        java.util.Random rnd = new java.util.Random(7);
        for (int i = 0; i < 3000; i++) {
            int len = (i % 5 == 0) ? 3 + rnd.nextInt(40) : 7;   // a tensor of 7, and lengths too rare for one
            ttl.append(":w").append(i).append(" :v (");
            for (int j = 0; j < len; j++) {
                ttl.append(' ').append((rnd.nextInt(10) == 0) ? "undef" : Integer.toString(rnd.nextInt(1000) - 500));
            }
            ttl.append(" ) .\n");
            ttl.append(":r").append(i).append(" :reading ( \"").append(java.time.Instant.ofEpochSecond(1_700_000_000L + i * 61L))
                    .append("\"^^xsd:dateTime ").append(rnd.nextInt(100)).append(".5E0 ) .\n");
            if (i % 3 == 0) {
                ttl.append(":c").append(i).append(" :component ( ( \"").append(i % 17).append("\" ( ").append(i)
                        .append(" \"mm\" ) ) ( \"x\" ( ").append(rnd.nextInt(9)).append(" undef ) ) ) .\n");
            }
            if (i % 500 == 0) {
                ttl.append(":rare").append(i).append(" :p ( \"").append(i).append("\" true :a ) .\n");
            }
        }
        Path src = dir.resolve("many.ttl");
        Files.writeString(src, ttl);
        File m0 = dir.resolve("m0" + ext).toFile();
        File disk = dir.resolve("m" + method + ext).toFile();
        writer(0, src.toFile(), m0, SequenceArrays.Options.DEFAULT).write();
        writer(method, src.toFile(), disk, SequenceArrays.Options.DEFAULT).write();
        assertArrayEquals(Files.readAllBytes(m0.toPath()), Files.readAllBytes(disk.toPath()), "method " + method);
        try (com.ebremer.beakgraph.store.OpenStore open = com.ebremer.beakgraph.store.OpenStore.open(disk.toPath())) {
            java.util.List<String> names = open.root().group(".BG").group("arrays").groupNames();
            // i64_7, tensors of the random lengths 16 rows share, the ragged rest, (dtZ,f64) and the
            // component lists; the rare tuples have no class.
            org.junit.jupiter.api.Assertions.assertTrue(names.containsAll(java.util.List.of("i64_7", "i64_r", "rec_1", "rec_2"))
                    && !names.contains("rec_3") && names.size() > 5, names.toString());
        }
    }
}
