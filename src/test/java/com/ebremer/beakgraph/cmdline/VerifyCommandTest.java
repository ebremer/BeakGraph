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
        assertTrue(structural.output().contains("(format v" + com.ebremer.beakgraph.Params.FORMAT_VERSION + ")"),
                "the verdict line names the store's format version (BG-266): " + structural.output());
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

    // --- BG-347: the deep pass must be able to FAIL, and degenerate stores must pass ---

    private static io.jhdf.api.Dataset findDataset(io.jhdf.api.Group group, String pathContains, String name) {
        for (io.jhdf.api.Node child : group.getChildren().values()) {
            if (child instanceof io.jhdf.api.Group g) {
                io.jhdf.api.Dataset found = findDataset(g, pathContains, name);
                if (found != null) return found;
            } else if (child instanceof io.jhdf.api.Dataset d && d.getName().equals(name) && d.getPath().contains(pathContains)) {
                return d;
            }
        }
        return null;
    }

    /**
     * Overwrites the literals dictionary's string bytes in place. The
     * structural pass used to map datasets and enumerate graphs without ever
     * reading a literal, so it passed; its dictionary-order probes (BG-343)
     * now extract a sample and report the damage, and the deep pass - which
     * materializes every triple - must report it too, either through the
     * probes, as a materialization shortfall or as a scan exception. Before
     * this test, a deepScan that agreed with itself by construction would
     * have gone unnoticed.
     */
    @Test
    void corruptedLiteralBytesFailBothPasses() throws Exception {
        Path copy = dir.resolve("corrupt.h5");
        Files.copy(good.toPath(), copy);
        long address;
        long size;
        try (HdfFile hdf = new HdfFile(copy)) {
            io.jhdf.api.Dataset target = findDataset((io.jhdf.api.Group) hdf.getChild(".BG"), "/literals/", "stringbuffer");
            org.junit.jupiter.api.Assertions.assertNotNull(target, "the literals dictionary's string buffer");
            address = ((io.jhdf.api.dataset.ContiguousDataset) target).getDataAddress();
            size = target.getSizeInBytes();
            assertTrue(size > 0);
        }
        try (FileChannel fc = FileChannel.open(copy, StandardOpenOption.WRITE)) {
            byte[] junk = new byte[(int) size];
            java.util.Arrays.fill(junk, (byte) 0xFF);
            fc.write(java.nio.ByteBuffer.wrap(junk), address);
        }
        Result structural = verify(copy.toFile(), false);
        assertEquals(2, structural.code(), "the dictionary probes read a sample of literals: " + structural.output());
        assertTrue(structural.output().contains("dictionary literals"), structural.output());
        Result deep = verify(copy.toFile(), true);
        assertEquals(2, deep.code(), "the deep pass must catch the damage: " + deep.output());
        assertTrue(deep.output().contains("dictionary literals") || deep.output().contains("materialized")
                || deep.output().contains("scan:"), "the reason must name the check: " + deep.output());
        assertTrue(deep.output().contains("1 FAILED"), deep.output());
    }

    // --- BG-343: dictionary order and searchability ---

    /** An in-memory section in whatever order the test wants. */
    private static com.ebremer.beakgraph.core.Dictionary section(java.util.List<org.apache.jena.graph.Node> nodes) {
        return new com.ebremer.beakgraph.core.Dictionary() {
            @Override public long locate(org.apache.jena.graph.Node element) { long s = search(element); return s >= 0 ? s : -1; }
            @Override public long search(org.apache.jena.graph.Node element) {
                // The readers' contract: a binary search assuming NodeComparator order.
                long low = 1, high = nodes.size();
                while (low <= high) {
                    long mid = (low + high) >>> 1;
                    int c = com.ebremer.beakgraph.core.lib.NodeComparator.INSTANCE.compare(extract(mid), element);
                    if (c == 0) return mid;
                    if (c < 0) low = mid + 1; else high = mid - 1;
                }
                return -low - 1;
            }
            @Override public org.apache.jena.graph.Node extract(long id) { return nodes.get((int) id - 1); }
            @Override public long getNumberOfNodes() { return nodes.size(); }
            @Override public java.util.stream.Stream<org.apache.jena.graph.Node> streamNodes() { return nodes.stream(); }
        };
    }

    @Test
    void sectionProbesCatchDriftedOrderInBothModes() {
        java.util.List<org.apache.jena.graph.Node> sorted = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sorted.add(org.apache.jena.graph.NodeFactory.createLiteralString(String.format("lit-%05d", i)));
        }
        sorted.sort(com.ebremer.beakgraph.core.lib.NodeComparator.INSTANCE);
        for (boolean deep : new boolean[]{false, true}) {
            java.util.List<String> problems = new java.util.ArrayList<>();
            VerifyCommand.checkSection("literals", section(sorted), 1, sorted.size(), deep, problems);
            assertEquals(java.util.List.of(), problems, "a sorted section passes (deep=" + deep + ")");
            VerifyCommand.checkSection("literals", section(sorted), 5, 4, deep, problems);
            assertEquals(java.util.List.of(), problems, "an empty range is fine");
        }
        // One adjacent pair swapped somewhere the structural sample does not land.
        java.util.List<org.apache.jena.graph.Node> drifted = new java.util.ArrayList<>(sorted);
        java.util.Collections.swap(drifted, 500, 501);
        java.util.List<String> deepProblems = new java.util.ArrayList<>();
        VerifyCommand.checkSection("literals", section(drifted), 1, drifted.size(), true, deepProblems);
        assertEquals(1, deepProblems.size(), deepProblems.toString());
        assertTrue(deepProblems.get(0).contains("out of order"), deepProblems.get(0));
        // A section sorted under ANOTHER comparator (here: reversed) fails the
        // sampled probes of the structural pass too.
        java.util.List<org.apache.jena.graph.Node> reversed = new java.util.ArrayList<>(sorted);
        java.util.Collections.reverse(reversed);
        java.util.List<String> structural = new java.util.ArrayList<>();
        VerifyCommand.checkSection("entities", section(reversed), 1, reversed.size(), false, structural);
        assertEquals(1, structural.size(), structural.toString());
        assertTrue(structural.get(0).contains("out of order") || structural.get(0).contains("not searchable"), structural.get(0));
        // Sorted, but with an unsearchable id (a search that lands elsewhere).
        java.util.List<String> unsearchable = new java.util.ArrayList<>();
        com.ebremer.beakgraph.core.Dictionary broken = new com.ebremer.beakgraph.core.Dictionary() {
            final com.ebremer.beakgraph.core.Dictionary d = section(sorted);
            @Override public long locate(org.apache.jena.graph.Node e) { return d.locate(e); }
            @Override public long search(org.apache.jena.graph.Node e) { return -1; }
            @Override public org.apache.jena.graph.Node extract(long id) { return d.extract(id); }
            @Override public long getNumberOfNodes() { return d.getNumberOfNodes(); }
            @Override public java.util.stream.Stream<org.apache.jena.graph.Node> streamNodes() { return d.streamNodes(); }
        };
        VerifyCommand.checkSection("predicates", broken, 1, sorted.size(), false, unsearchable);
        assertEquals(1, unsearchable.size(), unsearchable.toString());
        assertTrue(unsearchable.get(0).contains("not searchable"), unsearchable.get(0));
    }

    /** Overwrites {@code needle} with {@code replacement} inside one dataset's payload. */
    private static void patchDataset(Path h5, String pathContains, String name, byte[] needle, byte[] replacement) throws Exception {
        long address;
        long size;
        try (HdfFile hdf = new HdfFile(h5)) {
            io.jhdf.api.Dataset target = findDataset((io.jhdf.api.Group) hdf.getChild(".BG"), pathContains, name);
            org.junit.jupiter.api.Assertions.assertNotNull(target, pathContains + name);
            address = ((io.jhdf.api.dataset.ContiguousDataset) target).getDataAddress();
            size = target.getSizeInBytes();
        }
        try (FileChannel fc = FileChannel.open(h5, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer payload = java.nio.ByteBuffer.allocate((int) size);
            while (payload.hasRemaining() && fc.read(payload, address + payload.position()) > 0) { }
            byte[] bytes = payload.array();
            int at = -1;
            outer:
            for (int i = 0; i + needle.length <= bytes.length; i++) {
                for (int j = 0; j < needle.length; j++) {
                    if (bytes[i + j] != needle[j]) continue outer;
                }
                at = i;
                break;
            }
            assertTrue(at >= 0, "the payload holds " + new String(needle, StandardCharsets.UTF_8) + " verbatim");
            fc.write(java.nio.ByteBuffer.wrap(replacement), address + at);
        }
    }

    /**
     * A store whose literals section is no longer sorted under NodeComparator:
     * the middle literal's text is rewritten in place to one that sorts
     * first. Every id still extracts, so the old structural pass and the old
     * deep pass both said OK while a binary search for it answered nothing.
     */
    @Test
    void driftedDictionaryOrderFailsBothPasses() throws Exception {
        File src = dir.resolve("drift.nq").toFile();
        Files.writeString(src.toPath(), "<http://ex.org/s> <http://ex.org/p> \"aaaa\" .\n"
                + "<http://ex.org/s> <http://ex.org/p> \"mmmm\" .\n"
                + "<http://ex.org/s> <http://ex.org/p> \"zzzz\" .\n", StandardCharsets.UTF_8);
        File h5 = dir.resolve("drift.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        assertEquals(0, verify(h5, true).code());
        patchDataset(h5.toPath(), "/literals/strings/", "stringbuffer",
                "mmmm".getBytes(StandardCharsets.UTF_8), "0000".getBytes(StandardCharsets.UTF_8));
        for (boolean deep : new boolean[]{false, true}) {
            Result r = verify(h5, deep);
            assertEquals(2, r.code(), "deep=" + deep + ": " + r.output());
            assertTrue(r.output().contains("dictionary literals"), r.output());
            assertTrue(r.output().contains("out of order") || r.output().contains("not searchable"), r.output());
        }
    }

    // --- BG-345 / BG-292 / BG-147: the second index is required and exercised ---

    @Test
    void declaredSizeMustFitTheStoredBytes() {
        java.nio.ByteBuffer eight = java.nio.ByteBuffer.allocate(8);
        com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer ok =
                com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer.readView(new com.ebremer.beakgraph.io.ByteBufferBytes(eight), 4, 16);
        assertEquals(4, ok.getNumEntries());
        IllegalStateException tooMany = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer.readView(new com.ebremer.beakgraph.io.ByteBufferBytes(eight), 5, 16));
        assertTrue(tooMany.getMessage().contains("only 8 bytes are stored"), tooMany.getMessage());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer.readView(new com.ebremer.beakgraph.io.ByteBufferBytes(eight), Long.MAX_VALUE, 64));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer.readView(new com.ebremer.beakgraph.io.ByteBufferBytes(eight), -1, 8));
    }

    /** Copies the healthy store and applies an edit through the native HDF5 library (jHDF cannot write). */
    private Path nativeEdit(String name, java.util.function.LongConsumer edit) throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Path copy = dir.resolve(name);
        Files.copy(good.toPath(), copy, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        long fid = hdf.hdf5lib.H5.H5Fopen(copy.toString(), hdf.hdf5lib.HDF5Constants.H5F_ACC_RDWR, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
        try {
            edit.accept(fid);
        } finally {
            hdf.hdf5lib.H5.H5Fclose(fid);
        }
        return copy;
    }

    private static void unlink(long fid, String path) {
        try {
            hdf.hdf5lib.H5.H5Ldelete(fid, path, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void aMissingGposIndexFailsStructurally() throws Exception {
        Path copy = nativeEdit("no-gpos.h5", fid -> unlink(fid, "/.BG/GPOS"));
        for (boolean deep : new boolean[]{false, true}) {
            Result r = verify(copy.toFile(), deep);
            assertEquals(2, r.code(), r.output());
            assertTrue(r.output().contains("index GPOS absent"), r.output());
        }
        // Symmetric: the file is not "half usable" without GSPO either.
        Path noGspo = nativeEdit("no-gspo.h5", fid -> unlink(fid, "/.BG/GSPO"));
        Result r = verify(noGspo.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("index GSPO absent"), r.output());
    }

    @Test
    void aMissingLevelDatasetFailsStructurally() throws Exception {
        Path copy = nativeEdit("no-so.h5", fid -> unlink(fid, "/.BG/GPOS/So"));
        Result r = verify(copy.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("index GPOS: dataset So missing"), r.output());
    }

    private static void scalarAttribute(long objId, String name, long fileType, byte[] value) throws Exception {
        long space = hdf.hdf5lib.H5.H5Screate(hdf.hdf5lib.HDF5Constants.H5S_SCALAR);
        long attr = hdf.hdf5lib.H5.H5Acreate(objId, name, fileType, space,
                hdf.hdf5lib.HDF5Constants.H5P_DEFAULT, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
        try {
            hdf.hdf5lib.H5.H5Awrite(attr, fileType, value);
        } finally {
            hdf.hdf5lib.H5.H5Aclose(attr);
            hdf.hdf5lib.H5.H5Sclose(space);
        }
    }

    /**
     * GPOS/Ss is replaced by an 8-byte dataset that CLAIMS 2^40 x 32-bit
     * entries (the native library refuses to rewrite a jHDF-written
     * attribute in place, so the dataset is rebuilt). The reader used to
     * store the attributes verbatim and fail at query time.
     */
    @Test
    void anOversizedNumEntriesFailsStructurally() throws Exception {
        Path copy = nativeEdit("big-numentries.h5", fid -> {
            try {
                unlink(fid, "/.BG/GPOS/Ss");
                long space = hdf.hdf5lib.H5.H5Screate_simple(1, new long[]{8}, null);
                long dset = hdf.hdf5lib.H5.H5Dcreate(fid, "/.BG/GPOS/Ss", hdf.hdf5lib.HDF5Constants.H5T_STD_I8LE, space,
                        hdf.hdf5lib.HDF5Constants.H5P_DEFAULT, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
                try {
                    hdf.hdf5lib.H5.H5Dwrite(dset, hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8, hdf.hdf5lib.HDF5Constants.H5S_ALL,
                            hdf.hdf5lib.HDF5Constants.H5S_ALL, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT, new byte[8]);
                    scalarAttribute(dset, "numEntries", hdf.hdf5lib.HDF5Constants.H5T_STD_I64LE,
                            java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(1L << 40).array());
                    scalarAttribute(dset, "width", hdf.hdf5lib.HDF5Constants.H5T_STD_I32LE,
                            java.nio.ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(32).array());
                } finally {
                    hdf.hdf5lib.H5.H5Dclose(dset);
                    hdf.hdf5lib.H5.H5Sclose(space);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Result r = verify(copy.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("index GPOS") && r.output().contains("bytes are stored"), r.output());
    }

    /**
     * GPOS payload damage that leaves every header, attribute and the GSPO
     * index intact: the object-id column of GPOS is zero-filled. The old deep
     * pass compared GSPO with GSPO and reported OK; the pass now reconciles
     * GPOS against it.
     */
    @Test
    void deepPassCatchesGposPayloadCorruption() throws Exception {
        Path copy = dir.resolve("gpos-zeroed.h5");
        Files.copy(good.toPath(), copy, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        long address;
        long size;
        try (HdfFile hdf = new HdfFile(copy)) {
            io.jhdf.api.Dataset target = findDataset((io.jhdf.api.Group) hdf.getChild(".BG"), "/GPOS/", "So");
            org.junit.jupiter.api.Assertions.assertNotNull(target, "GPOS/So");
            address = ((io.jhdf.api.dataset.ContiguousDataset) target).getDataAddress();
            size = target.getSizeInBytes();
            assertTrue(size > 0);
        }
        try (FileChannel fc = FileChannel.open(copy, StandardOpenOption.WRITE)) {
            fc.write(java.nio.ByteBuffer.wrap(new byte[(int) size]), address);
        }
        Result structural = verify(copy.toFile(), false);
        assertEquals(0, structural.code(), "structure and dictionaries are intact: " + structural.output());
        Result deep = verify(copy.toFile(), true);
        assertEquals(2, deep.code(), "the deep pass must walk GPOS: " + deep.output());
        assertTrue(deep.output().contains("GPOS"), deep.output());
    }

    // --- BG-350: the format version and numQuads are checked, not merely printed ---

    /** A store whose attributes the native library can rewrite (jHDF-written attributes it refuses to touch). */
    private Path nativeStore(String name, String nq) throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        File src = dir.resolve(name + ".nq").toFile();
        Files.writeString(src.toPath(), nq, StandardCharsets.UTF_8);
        File h5 = dir.resolve(name + ".h5").toFile();
        com.ebremer.beakgraph.huge.HugeHDF5Writer.Builder().setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .setWorkDirectory(Files.createDirectories(dir.resolve("work-" + name))).build().write();
        return h5.toPath();
    }

    /** Deletes and re-creates a scalar attribute (the library refuses to write into an existing one here). */
    private static void rewriteAttribute(long fid, String object, String attr, long fileType, byte[] value) {
        try {
            hdf.hdf5lib.H5.H5Adelete_by_name(fid, object, attr, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
            long obj = hdf.hdf5lib.H5.H5Oopen(fid, object, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
            try {
                long space = hdf.hdf5lib.H5.H5Screate(hdf.hdf5lib.HDF5Constants.H5S_SCALAR);
                long a = hdf.hdf5lib.H5.H5Acreate(obj, attr, fileType, space,
                        hdf.hdf5lib.HDF5Constants.H5P_DEFAULT, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
                try {
                    hdf.hdf5lib.H5.H5Awrite(a, fileType, value);
                } finally {
                    hdf.hdf5lib.H5.H5Aclose(a);
                    hdf.hdf5lib.H5.H5Sclose(space);
                }
            } finally {
                hdf.hdf5lib.H5.H5Oclose(obj);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final String FIVE_QUADS = "<http://ex.org/s1> <http://ex.org/p> <http://ex.org/o1> .\n"
            + "<http://ex.org/s1> <http://ex.org/p> \"plain\" .\n"
            + "<http://ex.org/s2> <http://ex.org/p> \"hello\"@en .\n"
            + "<http://ex.org/s2> <http://ex.org/q> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"
            + "<http://ex.org/s3> <http://ex.org/p> <http://ex.org/o2> <http://ex.org/g> .\n";

    @Test
    void aLostFormatVersionAttributeIsReportedNotTreatedAsLegacy() throws Exception {
        Path store = nativeStore("fv-missing", FIVE_QUADS);
        assertEquals(0, verify(store.toFile(), true).code());
        long fid = hdf.hdf5lib.H5.H5Fopen(store.toString(), hdf.hdf5lib.HDF5Constants.H5F_ACC_RDWR, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
        try {
            hdf.hdf5lib.H5.H5Adelete_by_name(fid, "/.BG", "formatVersion", hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
        } finally {
            hdf.hdf5lib.H5.H5Fclose(fid);
        }
        Result r = verify(store.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("formatVersion attribute is missing"), r.output());
        assertTrue(r.output().contains("rank directories"), r.output());
        assertTrue(r.output().contains("(format v1)"), "the verdict line shows what the reader would use: " + r.output());
    }

    @Test
    void anUnreadableFormatVersionIsCorruptionNotLegacy() throws Exception {
        Path store = nativeStore("fv-zero", FIVE_QUADS);
        long fid = hdf.hdf5lib.H5.H5Fopen(store.toString(), hdf.hdf5lib.HDF5Constants.H5F_ACC_RDWR, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
        try {
            rewriteAttribute(fid, "/.BG", "formatVersion", hdf.hdf5lib.HDF5Constants.H5T_STD_I32LE,
                    java.nio.ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(0).array());
        } finally {
            hdf.hdf5lib.H5.H5Fclose(fid);
        }
        Result r = verify(store.toFile(), false);
        assertEquals(2, r.code(), r.output());
        assertTrue(r.output().contains("cannot open") && r.output().contains("not a version number"), r.output());
    }

    @Test
    void deepPassChecksTheNumQuadsBound() throws Exception {
        Path store = nativeStore("numquads", FIVE_QUADS);
        long fid = hdf.hdf5lib.H5.H5Fopen(store.toString(), hdf.hdf5lib.HDF5Constants.H5F_ACC_RDWR, hdf.hdf5lib.HDF5Constants.H5P_DEFAULT);
        try {
            rewriteAttribute(fid, "/.BG", "numQuads", hdf.hdf5lib.HDF5Constants.H5T_STD_I64LE,
                    java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(2L).array());
        } finally {
            hdf.hdf5lib.H5.H5Fclose(fid);
        }
        assertEquals(0, verify(store.toFile(), false).code(), "numQuads is informational for the structural pass");
        Result deep = verify(store.toFile(), true);
        assertEquals(2, deep.code(), deep.output());
        assertTrue(deep.output().contains("numQuads records only 2"), deep.output());
        assertTrue(deep.output().contains("hold 5 triples"), deep.output());
    }

    @Test
    void degenerateStoresPassBothPasses() throws Exception {
        java.util.Map<String, String> sources = new java.util.LinkedHashMap<>();
        sources.put("empty-none", "");
        sources.put("empty-exact", "");
        sources.put("all-iri", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n<http://ex.org/b> <http://ex.org/p> <http://ex.org/c> .\n");
        sources.put("single", "<http://ex.org/a> <http://ex.org/p> \"one\" .\n");
        sources.put("named-only", "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> <http://ex.org/g> .\n");
        sources.put("triple-term-only", "<http://ex.org/a> <http://ex.org/p> <<( <http://ex.org/x> <http://ex.org/y> <http://ex.org/z> )>> .\n");
        for (var e : sources.entrySet()) {
            String name = e.getKey();
            File src = dir.resolve(name + ".nq").toFile();
            Files.write(src.toPath(), e.getValue().getBytes(StandardCharsets.UTF_8));
            File h5 = dir.resolve(name + ".h5").toFile();
            HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false)
                    .setVoidMode(name.endsWith("exact") ? com.ebremer.beakgraph.core.VoidMode.EXACT : com.ebremer.beakgraph.core.VoidMode.NONE)
                    .build().write();
            for (boolean deep : new boolean[]{false, true}) {
                Result r = verify(h5, deep);
                assertEquals(0, r.code(), name + (deep ? " deep: " : " structural: ") + r.output());
                assertTrue(r.output().contains("1 OK, 0 FAILED"), name + ": " + r.output());
            }
        }
    }
}
