package com.ebremer.beakgraph.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.cmdline.VerifyCommand;
import com.ebremer.beakgraph.core.AtomicPublish;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.WritableDataset;
import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.io.MemorySegmentBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.io.SegmentedBytes;
import com.ebremer.falcon.hdf5.Attribute;
import com.ebremer.falcon.hdf5.Dataset;
import com.ebremer.falcon.hdf5.Group;
import com.ebremer.falcon.hdf5.Hdf5File;
import com.ebremer.falcon.hdf5.Hdf5Object;
import com.ebremer.falcon.zarr.ArraySpec;
import com.ebremer.falcon.zarr.Zarr;
import com.ebremer.falcon.zarr.ZarrArray;
import com.ebremer.falcon.zarr.ZarrGroup;
import com.ebremer.falcon.zarr.ZarrNode;
import com.ebremer.falcon.zarr.datatype.DataType;
import com.ebremer.falcon.zarr.json.JsonObject;
import com.ebremer.falcon.zarr.store.FileSystemStore;
import com.ebremer.falcon.zarr.store.MemoryStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * One store, two containers (SPECIFICATIONS.md §3): what a writer puts in an
 * HDF5 file and in a Zarr store is the same groups, the same arrays byte for
 * byte, and the same attributes - for all six engines - and the readers
 * answer the same from either. Each store is described here through Falcon's
 * own HDF5 and Zarr readers, not through BeakGraph's, so the comparison is of
 * what is in the containers.
 */
class ContainerParityTest {

    @TempDir
    Path dir;

    /** {@link StoreParity#mixedTrig()} without its blank nodes: every build of it holds the same bytes. */
    private static String stableTrig() {
        StringBuilder sb = new StringBuilder();
        for (String line : StoreParity.mixedTrig().split("\n")) {
            if (!line.contains("_:b")) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    private Path write(String name, String content) throws IOException {
        Path p = dir.resolve(name);
        Files.writeString(p, content, StandardCharsets.UTF_8);
        return p;
    }

    private static String sha(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)).substring(0, 16);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /** Every node of an HDF5 store: its path, and for an array its length and bytes, each with its attributes. */
    static SortedMap<String, String> describeHdf5(Path h5) throws IOException {
        SortedMap<String, String> out = new TreeMap<>();
        try (Hdf5File f = Hdf5File.open(h5)) {
            describe(f.root(), out);
        }
        return out;
    }

    private static void describe(Group g, SortedMap<String, String> out) {
        for (Hdf5Object child : g.children()) {
            SortedMap<String, Long> attributes = new TreeMap<>();
            for (Attribute a : child.attributes()) {
                attributes.put(a.name(), a.readLong());
            }
            String path = child.path().substring(1);
            if (child instanceof Group sub) {
                out.put(path, "group " + attributes);
                describe(sub, out);
            } else {
                Dataset d = (Dataset) child;
                assertEquals(Dataset.Layout.CONTIGUOUS, d.layout(), path);
                byte[] bytes = d.readRawBytes();
                out.put(path, "array " + bytes.length + " " + sha(bytes) + " " + attributes);
            }
        }
    }

    /** The same description of a Zarr store, its arrays read through Falcon's codec pipeline. */
    static SortedMap<String, String> describeZarr(Path zarr) {
        SortedMap<String, String> out = new TreeMap<>();
        // Without the consolidated metadata: every node's own zarr.json is what is read.
        describe(Zarr.openGroup(FileSystemStore.openReadOnly(zarr), false), out);
        return out;
    }

    private static void describe(ZarrGroup g, SortedMap<String, String> out) {
        for (ZarrNode child : g.children()) {
            SortedMap<String, Long> attributes = new TreeMap<>();
            for (Map.Entry<String, com.ebremer.falcon.zarr.json.JsonValue> a : child.attributes().members().entrySet()) {
                attributes.put(a.getKey(), a.getValue().asNumber().longValue());
            }
            if (child instanceof ZarrGroup sub) {
                out.put(child.path(), "group " + attributes);
                describe(sub, out);
            } else {
                ZarrArray a = (ZarrArray) child;
                assertEquals(DataType.UINT8, a.dataType(), child.path());
                assertEquals(1, a.rank(), child.path());
                assertEquals(List.of("bytes"), a.codecNames(), child.path());
                byte[] bytes = a.readRawBytes();
                out.put(child.path(), "array " + bytes.length + " " + sha(bytes) + " " + attributes);
            }
        }
    }

    /** Every file of a Zarr store with its digest: the store as it lies on disk. */
    private static SortedMap<String, String> files(Path zarr) throws IOException {
        SortedMap<String, String> out = new TreeMap<>();
        try (Stream<Path> walk = Files.walk(zarr)) {
            for (Path p : walk.filter(Files::isRegularFile).toList()) {
                out.put(zarr.relativize(p).toString().replace('\\', '/'), sha(Files.readAllBytes(p)));
            }
        }
        return out;
    }

    private static void assertNothingBeside(Path store) throws IOException {
        try (Stream<Path> siblings = Files.list(store.getParent())) {
            List<String> stray = siblings.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".partial") || n.endsWith(".tmp") || n.endsWith(".old") || n.endsWith(".new"))
                    .toList();
            assertEquals(List.of(), stray, "left beside " + store);
        }
    }

    @Test
    void everyEngineWritesTheSameStoreInBothContainers() throws Exception {
        Path src = write("stable.trig", stableTrig());
        SortedMap<String, String> first = null;
        SortedMap<String, String> firstFiles = null;
        for (WriterEngines.Engine engine : WriterEngines.all().toList()) {
            Path h5 = dir.resolve(engine.name() + ".h5");
            Path zarr = dir.resolve(engine.name() + ".zarr");
            engine.buildStore(src.toFile(), h5.toFile());
            engine.buildStore(src.toFile(), zarr.toFile());
            assertTrue(Files.isRegularFile(h5), engine.name());
            assertTrue(Files.isDirectory(zarr) && Files.isRegularFile(zarr.resolve("zarr.json")), engine.name());

            SortedMap<String, String> inHdf5 = describeHdf5(h5);
            SortedMap<String, String> inZarr = describeZarr(zarr);
            assertTrue(inHdf5.size() > 40 && inHdf5.containsKey(".BG/GSPO/So"), "the description holds the store: " + inHdf5.keySet());
            assertEquals(inHdf5, inZarr, engine.name() + ": the same groups, arrays, bytes and attributes");

            // And one store whichever engine wrote it, down to the files of the Zarr store.
            SortedMap<String, String> onDisk = files(zarr);
            if (first == null) {
                first = inHdf5;
                firstFiles = onDisk;
            }
            assertEquals(first, inHdf5, engine.name() + " against " + WriterEngines.all().findFirst().orElseThrow());
            assertEquals(firstFiles, onDisk, engine.name() + ": the Zarr store's files");
            assertNothingBeside(zarr);
        }
        // Each array of up to 1 GiB is one chunk, whose file is the array's bytes.
        Path zarr = dir.resolve(WriterEngines.all().findFirst().orElseThrow().name() + ".zarr");
        try (Hdf5File f = Hdf5File.open(dir.resolve(WriterEngines.all().findFirst().orElseThrow().name() + ".h5"))) {
            byte[] expected = f.root().dataset(".BG/GSPO/So").readRawBytes();
            assertArrayEquals(expected, Files.readAllBytes(zarr.resolve(".BG/GSPO/So/c/0")));
        }
    }

    @Test
    void sequencesAreTheSameStoreInBothContainers() throws Exception {
        // Format v7 (RDFARRY.md step 3): the sequence stores and their numSlots
        // attribute, in both containers, from the engines that write them.
        Path src = write("seq.trig", com.ebremer.beakgraph.RdfArraySequenceStorageTest.fixture());
        SortedMap<String, String> first = null;
        for (WriterEngines.Engine engine : WriterEngines.all()
                .filter(e -> e.name().startsWith("method0") || e.name().startsWith("method2")).toList()) {
            Path h5 = dir.resolve("seq-" + engine.name() + ".h5");
            Path zarr = dir.resolve("seq-" + engine.name() + ".zarr");
            engine.buildStore(src.toFile(), h5.toFile());
            engine.buildStore(src.toFile(), zarr.toFile());
            SortedMap<String, String> inHdf5 = describeHdf5(h5);
            assertTrue(inHdf5.containsKey(".BG/dictionary/literals/sequenceStarts")
                    && inHdf5.containsKey(".BG/dictionary/literals/sequenceElements_blocks"), inHdf5.keySet().toString());
            assertTrue(inHdf5.get(".BG/dictionary/literals/sequenceElements").contains("numSlots=4"),
                    inHdf5.get(".BG/dictionary/literals/sequenceElements"));
            assertEquals(inHdf5, describeZarr(zarr), engine.name() + ": the same groups, arrays, bytes and attributes");
            if (first == null) {
                first = inHdf5;
            }
            assertEquals(first, inHdf5, engine.name() + " against method 0");
        }
    }

    @Test
    void theReadersAnswerTheSameFromBothContainers() throws Exception {
        Path src = write("mixed.trig", StoreParity.mixedTrig());
        for (WriterEngines.Engine engine : WriterEngines.all().filter(e -> e.name().startsWith("method0") || e.name().startsWith("method4")).toList()) {
            Path h5 = dir.resolve("q-" + engine.name() + ".h5");
            Path zarr = dir.resolve("q-" + engine.name() + ".zarr");
            engine.buildStore(src.toFile(), h5.toFile());
            engine.buildStore(src.toFile(), zarr.toFile());
            StoreParity.assertStoresEquivalent(h5, zarr,
                    "SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }",
                    "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                    "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o FILTER(?o > 10) }");
            assertEquals(0, new VerifyCommand(zarr.toFile(), true).run(), "-verify -deep of the Zarr store");
            assertEquals(0, new VerifyCommand(h5.toFile(), true).run(), "-verify -deep of the HDF5 file");

            try (HDF5Reader inZarr = new HDF5Reader(zarr.toFile()); HDF5Reader inHdf5 = new HDF5Reader(h5)) {
                assertEquals(Container.ZARR, inZarr.getContainer());
                assertEquals(Container.HDF5, inHdf5.getContainer());
                assertFalse(inZarr.isChannelBacked(), "a directory store is memory-mapped");
                assertEquals(inHdf5.getFormatVersion(), inZarr.getFormatVersion());
                assertEquals(inHdf5.getNumQuads(), inZarr.getNumQuads());
                assertEquals(Params.FORMAT_VERSION, inZarr.getFormatVersion());
            }
        }
    }

    @Test
    void anArrayOfOneChunkIsReadThroughTheSameViewAnHdf5DatasetIs() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path h5 = dir.resolve("view.h5");
        Path zarr = dir.resolve("view.zarr");
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), h5.toFile());
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), zarr.toFile());
        try (OpenStore a = OpenStore.open(h5); OpenStore b = OpenStore.open(zarr)) {
            assertEquals(Container.HDF5, a.container());
            assertEquals(Container.ZARR, b.container());
            for (String name : new String[] {"So", "So_blocks", "Bo", "SBo"}) {
                StoreArray inHdf5 = a.root().group(".BG").group("GSPO").array(name);
                StoreArray inZarr = b.root().group(".BG").group("GSPO").array(name);
                assertEquals("/.BG/GSPO/" + name, inHdf5.path());
                assertEquals(inHdf5.path(), inZarr.path());
                RandomAccessBytes x = inHdf5.bytes();
                RandomAccessBytes y = inZarr.bytes();
                // One class in the hottest call site of the read path, whichever container a JVM has open.
                assertInstanceOf(MemorySegmentBytes.class, x);
                assertInstanceOf(MemorySegmentBytes.class, y);
                assertEquals(x.size(), y.size());
                byte[] p = new byte[(int) x.size()];
                byte[] q = new byte[(int) y.size()];
                x.get(0, p, 0, p.length);
                y.get(0, q, 0, q.length);
                assertArrayEquals(p, q, name);
                assertEquals(inHdf5.hasAttribute(Params.NUM_ENTRIES), inZarr.hasAttribute(Params.NUM_ENTRIES));
                if (inHdf5.hasAttribute(Params.NUM_ENTRIES)) { // a block directory has none
                    assertEquals(inHdf5.attribute(Params.NUM_ENTRIES), inZarr.attribute(Params.NUM_ENTRIES));
                }
                assertEquals(inHdf5.hasAttribute(Params.BLOCK_ENTRIES), inZarr.hasAttribute(Params.BLOCK_ENTRIES));
                assertTrue(inZarr.bytes() == y, "an array is mapped once, however often it is asked for");
            }
            assertEquals(null, b.root().group(".BG").group("absent"));
            assertEquals(null, b.root().group(".BG").group("GSPO").array("absent"));
        }
    }

    @Test
    void anArrayCutIntoSeveralChunksReadsTheSame() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path h5 = dir.resolve("chunks.h5");
        Path zarr = dir.resolve("chunks.zarr");
        WriterEngines.Engine inMemory = WriterEngines.all().findFirst().orElseThrow();
        WriterEngines.Engine disk = WriterEngines.all().filter(WriterEngines.Engine::diskBased).findFirst().orElseThrow();
        inMemory.buildStore(src.toFile(), h5.toFile());
        long previous = ZarrStoreWriter.setChunkMax(100);
        try {
            for (WriterEngines.Engine engine : List.of(inMemory, disk)) {
                engine.buildStore(src.toFile(), zarr.toFile()); // the second build replaces the first
                ZarrArray strings = Zarr.openGroup(FileSystemStore.openReadOnly(zarr)).array(".BG/dictionary/literals/strings/stringbuffer");
                long size = strings.shape()[0];
                long chunk = strings.chunkShape()[0];
                long chunks = strings.gridShape()[0];
                assertTrue(size > 100 && chunks == (size + 99) / 100 && chunk <= 100 && chunk * chunks >= size
                        && chunk * (chunks - 1) < size, size + " bytes in " + chunks + " chunks of " + chunk);
                for (long i = 0; i < chunks; i++) {
                    assertEquals(chunk, Files.size(zarr.resolve(strings.chunkKey(i))), "every chunk is stored whole, the last one padded");
                }
                assertEquals(describeHdf5(h5), describeZarr(zarr), engine.name());
                StoreParity.assertStoresEquivalent(h5, zarr, "SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }");
                assertEquals(0, new VerifyCommand(zarr.toFile(), true).run(), engine.name());
                try (OpenStore open = OpenStore.open(zarr)) {
                    RandomAccessBytes view = open.root().group(".BG").group("dictionary").group("literals").group("strings")
                            .array("stringbuffer").bytes();
                    assertInstanceOf(SegmentedBytes.class, view, "several mappings, each read where a value lies");
                    assertFalse(view.isRemote(), "mapped: the accelerators a remote array goes without are used");
                    byte[] read = new byte[(int) size];
                    view.get(0, read, 0, read.length);
                    assertArrayEquals(strings.readRawBytes(), read);
                    assertThrows(IndexOutOfBoundsException.class, () -> view.get(size));
                }
            }
        } finally {
            ZarrStoreWriter.setChunkMax(previous);
        }
    }

    /** A copy of a directory store in a Falcon store that cannot map its values. */
    private static MemoryStore inMemory(Path zarr) throws IOException {
        MemoryStore memory = new MemoryStore();
        try (Stream<Path> walk = Files.walk(zarr)) {
            for (Path p : walk.filter(Files::isRegularFile).toList()) {
                memory.set(zarr.relativize(p).toString().replace('\\', '/'), Files.readAllBytes(p));
            }
        }
        return memory;
    }

    @Test
    void aStoreThatCannotBeMappedIsReadARangeAtATime() throws Exception {
        Path src = write("mixed.trig", StoreParity.mixedTrig());
        Path zarr = dir.resolve("ranges.zarr");
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), zarr.toFile());
        MemoryStore memory = inMemory(zarr);
        // An all-zero chunk another writer left out reads as the fill value.
        String zeros = ".BG/GSPO/Bo/c/0";
        long zeroBytes = Files.size(zarr.resolve(zeros));
        try (BeakGraph mapped = new BeakGraph(new HDF5Reader(zarr.toFile()));
             BeakGraph ranged = new BeakGraph(new HDF5Reader(ZarrStore.open(memory, "memory"), URI.create("bg:/memory")))) {
            assertTrue(((HDF5Reader) ranged.getReader()).isChannelBacked());
            assertFalse(((HDF5Reader) mapped.getReader()).isChannelBacked());
            for (String query : new String[] {
                    "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }",
                    "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
                    "SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }"}) {
                List<String> expected = StoreParity.select(mapped.getDataset(), query);
                assertFalse(expected.isEmpty(), query);
                assertEquals(expected, StoreParity.select(ranged.getDataset(), query), query);
            }
        }
        memory.delete(zeros);
        try (ZarrStore open = ZarrStore.open(memory, "memory")) {
            RandomAccessBytes view = open.root().group(".BG").group("GSPO").array("Bo").bytes();
            byte[] read = new byte[(int) zeroBytes];
            java.util.Arrays.fill(read, (byte) 7);
            view.get(0, read, 0, read.length);
            assertArrayEquals(new byte[(int) zeroBytes], read, "a chunk that is not stored is zeros");
        }
    }

    /** A copy of a store with one node changed through Falcon, consolidated again as a writer would leave it. */
    private Path forged(Path zarr, String name, java.util.function.Consumer<ZarrGroup> change) throws IOException {
        Path copy = dir.resolve(name + ".zarr");
        try (Stream<Path> walk = Files.walk(zarr)) {
            for (Path p : walk.toList()) {
                Path target = copy.resolve(zarr.relativize(p).toString());
                if (Files.isDirectory(p)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(p, target);
                }
            }
        }
        ZarrGroup root = Zarr.openGroup(FileSystemStore.open(copy), false);
        change.accept(root);
        root.consolidate();
        return copy;
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return c.getMessage();
    }

    @Test
    void whatBreaksTheZarrProfileIsNamedWithItsRule() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path zarr = dir.resolve("good.zarr");
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), zarr.toFile());

        // A compressed array: not its bytes as they are.
        Path compressed = forged(zarr, "compressed", root -> {
            ZarrGroup gspo = root.group(".BG/GSPO");
            byte[] bytes = gspo.array("So").readRawBytes();
            JsonObject attributes = gspo.array("So").attributes();
            gspo.delete("So");
            gspo.createArray("So", ArraySpec.builder(new long[] {bytes.length}, DataType.UINT8).chunkShape(bytes.length)
                    .gzip(1).attributes(attributes).build()).writeRawBytes(bytes);
        });
        try (HDF5Reader reader = new HDF5Reader(compressed.toFile())) {
            String m = rootMessage(assertThrows(IllegalStateException.class, () -> reader.getIndexReader(Index.GSPO)));
            assertTrue(m.contains("/.BG/GSPO/So") && m.contains("bytes codec alone") && m.contains("§3a") && m.contains("gzip"), m);
        }
        assertEquals(2, new VerifyCommand(compressed.toFile(), false).run());

        // Elements that are not bytes, and an array where a group belongs.
        Path typed = forged(zarr, "typed", root -> {
            ZarrGroup gspo = root.group(".BG/GSPO");
            gspo.delete("Sp");
            gspo.createArray("Sp", ArraySpec.builder(new long[] {8}, DataType.INT32).chunkShape(8).build());
            root.group(".BG").delete("GPOS");
            root.group(".BG").createArray("GPOS", ArraySpec.builder(new long[] {8}, DataType.UINT8).chunkShape(8).build());
        });
        try (OpenStore open = OpenStore.open(typed)) {
            StoreGroup bg = open.root().group(".BG");
            String m = assertThrows(IllegalStateException.class, () -> bg.group("GSPO").array("Sp")).getMessage();
            assertTrue(m.contains("/.BG/GSPO/Sp") && m.contains("int32") && m.contains("§3a"), m);
            m = assertThrows(IllegalStateException.class, () -> bg.group("GPOS")).getMessage();
            assertTrue(m.contains("/.BG/GPOS") && m.contains("must be a group") && m.contains("found an array"), m);
            m = assertThrows(IllegalStateException.class, () -> bg.array("GSPO")).getMessage();
            assertTrue(m.contains("/.BG/GSPO") && m.contains("must be an array") && m.contains("found a group"), m);
        }

        // An attribute that is not an integer, and one that is missing.
        Path attributes = forged(zarr, "attributes", root -> {
            ZarrArray so = root.array(".BG/GSPO/So");
            so.setAttributes(JsonObject.builder().put(Params.NUM_ENTRIES, "many").put(Params.WIDTH, 14).build());
        });
        try (OpenStore open = OpenStore.open(attributes)) {
            StoreArray so = open.root().group(".BG").group("GSPO").array("So");
            String m = assertThrows(IllegalStateException.class, () -> so.attribute(Params.NUM_ENTRIES)).getMessage();
            assertTrue(m.contains(Params.NUM_ENTRIES) && m.contains("/.BG/GSPO/So") && m.contains("scalar integer") && m.contains("\"many\""), m);
            assertEquals(14, so.attribute(Params.WIDTH));
            assertFalse(so.hasAttribute(Params.BLOCK_ENTRIES));
            m = assertThrows(IllegalStateException.class, () -> so.attribute(Params.BLOCK_ENTRIES)).getMessage();
            assertTrue(m.contains("Missing attribute") && m.contains(Params.BLOCK_ENTRIES), m);
        }

        // A chunk cut short: the array's metadata and its bytes disagree.
        Path truncated = forged(zarr, "truncated", root -> { });
        Path chunk = truncated.resolve(".BG/GSPO/So/c/0");
        byte[] whole = Files.readAllBytes(chunk);
        Files.write(chunk, java.util.Arrays.copyOf(whole, whole.length - 1));
        try (OpenStore open = OpenStore.open(truncated)) {
            StoreArray so = open.root().group(".BG").group("GSPO").array("So");
            String m = assertThrows(IllegalStateException.class, so::bytes).getMessage();
            assertTrue(m.contains("/.BG/GSPO/So") && m.contains((whole.length - 1) + " bytes") && m.contains("expected " + whole.length), m);
        }
        assertEquals(2, new VerifyCommand(truncated.toFile(), false).run());

        // A chunk that is not stored is the fill value: zeros, but only of an array whose fill value is 0.
        Path absent = forged(zarr, "absent", root -> { });
        long size = Files.size(absent.resolve(".BG/GSPO/Bo/c/0"));
        Files.delete(absent.resolve(".BG/GSPO/Bo/c/0"));
        try (OpenStore open = OpenStore.open(absent)) {
            RandomAccessBytes zeros = open.root().group(".BG").group("GSPO").array("Bo").bytes();
            assertEquals(size, zeros.size());
            byte[] read = new byte[(int) size];
            java.util.Arrays.fill(read, (byte) 7);
            zeros.get(0, read, 0, read.length);
            assertArrayEquals(new byte[(int) size], read);
        }
        Path filled = forged(zarr, "filled", root -> {
            ZarrGroup gspo = root.group(".BG/GSPO");
            gspo.delete("Bo");
            gspo.createArray("Bo", ArraySpec.builder(new long[] {size}, DataType.UINT8).chunkShape(size).fillValue(255).build());
        });
        try (OpenStore open = OpenStore.open(filled)) {
            StoreArray bo = open.root().group(".BG").group("GSPO").array("Bo");
            String m = assertThrows(IllegalStateException.class, bo::bytes).getMessage();
            assertTrue(m.contains("/.BG/GSPO/Bo") && m.contains("not stored") && m.contains("255") && m.contains("§3a"), m);
        }
    }

    @Test
    void whatIsNotAStoreIsSaidSo() throws Exception {
        Path empty = Files.createDirectories(dir.resolve("empty.zarr"));
        UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> new HDF5Reader(empty.toFile()));
        assertTrue(e.getMessage().startsWith("Cannot open " + empty), e.getMessage());

        // A Zarr hierarchy that holds something else.
        Path other = dir.resolve("other.zarr");
        Zarr.createGroup(FileSystemStore.open(other)).createGroup("images");
        IllegalStateException notOurs = assertThrows(IllegalStateException.class, () -> new HDF5Reader(other.toFile()));
        assertTrue(notOurs.getMessage().contains("Not a BeakGraph file") && notOurs.getMessage().contains(Params.BG), notOurs.getMessage());
        assertEquals(2, new VerifyCommand(other.toFile(), false).run());

        // An array at the root, where the root group belongs.
        Path array = dir.resolve("array.zarr");
        Zarr.createArray(FileSystemStore.open(array), ArraySpec.builder(new long[] {4}, DataType.UINT8).chunkShape(4).build());
        e = assertThrows(UncheckedIOException.class, () -> new HDF5Reader(array.toFile()));
        assertTrue(e.getMessage().startsWith("Cannot open " + array), e.getMessage());
    }

    @Test
    void aZarrStoreIsAtItsPathCompleteOrNotAtAll() throws Exception {
        Path store = dir.resolve("atomic.zarr");
        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);

        // Closed without a commit: nothing at the path, nothing beside it.
        try (WritableStore w = WritableStore.create(store, Container.ZARR)) {
            assertEquals(Container.ZARR, w.container());
            w.putGroup(".BG").putDataset("x", data);
            assertFalse(Files.exists(store), "nothing is at the path before the commit");
        }
        assertFalse(Files.exists(store));
        assertNothingBeside(store);

        // An array short of its length: the commit fails and publishes nothing.
        try (WritableStore w = WritableStore.create(store, Container.ZARR)) {
            WritableDataset short3 = w.putGroup(".BG").createDataset("short", 10);
            short3.write(data, 0, 3);
            assertEquals(3, short3.written());
            assertEquals(10, short3.length());
            IOException e = assertThrows(IOException.class, w::commit);
            assertTrue(e.getMessage().contains("/.BG/short") && e.getMessage().contains("3 of 10 bytes"), e.getMessage());
        }
        assertFalse(Files.exists(store));
        assertNothingBeside(store);

        // Complete: pieces in order, attributes of both widths, an empty array, more bytes than declared refused.
        try (WritableStore w = WritableStore.create(store, Container.ZARR)) {
            WritableGroup bg = w.putGroup(".BG");
            bg.putAttribute("small", 7);
            bg.putAttribute("large", Long.MAX_VALUE);
            WritableDataset pieces = bg.createDataset("pieces", 10);
            pieces.write(data, 0, 4);
            pieces.write(data, 4, 0);
            pieces.write(data, 4, 6);
            pieces.putAttribute("width", 3);
            assertThrows(IllegalStateException.class, () -> pieces.write(data, 0, 1));
            bg.putGroup("inner").putDataset("whole", data).putAttribute("numEntries", 10L);
            bg.createDataset("empty", 0);
            assertThrows(IllegalArgumentException.class, () -> bg.putGroup("inner"), "a name is used once");
            w.commit();
            assertThrows(IllegalStateException.class, w::commit);
        }
        assertNothingBeside(store);
        try (OpenStore open = OpenStore.open(store)) {
            StoreGroup bg = open.root().group(".BG");
            assertEquals(7, bg.attribute("small"));
            assertEquals(Long.MAX_VALUE, bg.attribute("large"));
            for (StoreArray a : new StoreArray[] {bg.array("pieces"), bg.group("inner").array("whole")}) {
                byte[] read = new byte[10];
                a.bytes().get(0, read, 0, 10);
                assertArrayEquals(data, read, a.path());
            }
            assertEquals(3, bg.array("pieces").attribute("width"));
            assertEquals(10, bg.group("inner").array("whole").attribute("numEntries"));
            assertEquals(0, bg.array("empty").bytes().size());
        }

        // A second store replaces the first whole; the reader has let go of its files (on Windows too).
        try (WritableStore w = WritableStore.create(store, Container.ZARR)) {
            w.putGroup(".BG").putDataset("only", new byte[] {1, 2, 3});
            w.commit();
        }
        assertNothingBeside(store);
        try (OpenStore open = OpenStore.open(store)) {
            assertEquals(null, open.root().group(".BG").array("pieces"));
            assertEquals(3, open.root().group(".BG").array("only").bytes().size());
        }
    }

    @Test
    void closingAStoreEndsItsViewsAndFreesItsFiles() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path zarr = dir.resolve("closing.zarr");
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), zarr.toFile());
        OpenStore open = OpenStore.open(zarr);
        RandomAccessBytes view = open.root().group(".BG").group("GSPO").array("So").bytes();
        view.get(0);
        open.close();
        assertThrows(IllegalStateException.class, () -> view.get(0), "a view read after the close");
        open.close(); // again: nothing happens

        // Unmapped, so the store can be rebuilt in place and removed - on Windows too.
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), zarr.toFile());
        assertEquals(0, new VerifyCommand(zarr.toFile(), true).run());
        AtomicPublish.discard(zarr);
        assertFalse(Files.exists(zarr));
    }

    @Test
    void aStoreOpensFromItsConsolidatedMetadataAlone() throws Exception {
        Path src = write("mixed.trig", StoreParity.mixedTrig());
        Path zarr = dir.resolve("consolidated.zarr");
        for (WriterEngines.Engine engine : WriterEngines.all().filter(e -> e.name().startsWith("method0") || e.name().startsWith("method1")).toList()) {
            engine.buildStore(src.toFile(), zarr.toFile());
            ZarrGroup root = Zarr.openGroup(FileSystemStore.openReadOnly(zarr));
            assertTrue(root.isConsolidated(), engine.name() + ": the root's zarr.json describes the whole hierarchy");
            String all = "SELECT ?g ?s ?p ?o WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }";
            List<String> expected;
            try (BeakGraph bg = new BeakGraph(new HDF5Reader(zarr.toFile()))) {
                expected = StoreParity.select(bg.getDataset(), all);
            }
            // Every other zarr.json gone: the reader needed none of them (SPECIFICATIONS.md §3a, item 7).
            Path stripped = forged(zarr, "stripped-" + engine.name(), r -> { });
            long removed = 0;
            try (Stream<Path> walk = Files.walk(stripped)) {
                for (Path p : walk.filter(p -> p.getFileName().toString().equals("zarr.json") && !p.getParent().equals(stripped)).toList()) {
                    Files.delete(p);
                    removed++;
                }
            }
            assertTrue(removed > 40, removed + " documents removed");
            try (BeakGraph bg = new BeakGraph(new HDF5Reader(stripped.toFile()))) {
                assertEquals(expected, StoreParity.select(bg.getDataset(), all), engine.name());
            }
            assertEquals(0, new VerifyCommand(stripped.toFile(), true).run());
        }
    }

    @Test
    void aReaderHoldsAllOfAStoreFromTheMomentItOpens() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path other = write("other.trig", "<http://ex.org/x> <http://ex.org/y> <http://ex.org/z> .\n");
        Path zarr = dir.resolve("held.zarr");
        WriterEngines.Engine engine = WriterEngines.all().findFirst().orElseThrow();
        engine.buildStore(src.toFile(), zarr.toFile());
        long arrays = describeZarr(zarr).values().stream().filter(v -> v.startsWith("array")).count();
        try (ZarrStore open = ZarrStore.open(zarr)) {
            assertEquals(0, open.viewCount(), "nothing is mapped until it is asked for");
            open.root().group(".BG").load();
            assertEquals(arrays, open.viewCount(), "every array of the group is mapped");
            open.root().group(".BG").load();
            assertEquals(arrays, open.viewCount());
        }
        String all = "SELECT ?g ?s ?p ?o WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }";
        List<String> expected;
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(zarr.toFile()))) {
            expected = StoreParity.select(bg.getDataset(), all);
        }
        assertTrue(expected.size() > 20);

        // A reader that has not yet asked for an index, and the store rebuilt underneath it from another source.
        Path kept = dir.resolve("held.zarr.new");
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(zarr.toFile()))) {
            if (System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win")) {
                // Windows does not move a directory whose files are mapped: the old store stays, the new one is kept.
                IOException e = assertThrows(IOException.class, () -> engine.buildStore(other.toFile(), zarr.toFile()));
                assertTrue(e.getMessage().contains("KEPT") && e.getMessage().contains(kept.getFileName().toString()), e.getMessage());
                assertTrue(Container.isZarrStore(kept), "the finished build is kept beside the destination");
                assertEquals(expected, StoreParity.select(bg.getDataset(), all), "the old store is whole, and still read");
            } else {
                // Elsewhere the directory is replaced, and the reader goes on reading the old one: it mapped all of it.
                engine.buildStore(other.toFile(), zarr.toFile());
                assertFalse(Files.exists(kept));
                assertEquals(expected, StoreParity.select(bg.getDataset(), all), "the reader holds the store it opened");
            }
        }
        if (Files.exists(kept)) {
            // The reader gone, the kept build takes its place - and an earlier kept build is replaced by a later one.
            AtomicPublish.publish(kept, zarr);
        }
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(zarr.toFile()))) {
            assertEquals(1, StoreParity.select(bg.getDataset(), all).size(), "the new store");
        }
        assertNothingBeside(zarr);
    }

    @Test
    void aDirectoryThatIsNotAStoreIsNeverReplaced() throws Exception {
        Path src = write("stable.trig", stableTrig());
        // Someone's directory, named like a store.
        Path theirs = Files.createDirectories(dir.resolve("theirs.zarr"));
        Files.writeString(theirs.resolve("notes.txt"), "keep me");
        for (WriterEngines.Engine engine : WriterEngines.all().toList()) {
            IOException e = assertThrows(IOException.class, () -> engine.buildStore(src.toFile(), theirs.toFile()), engine.name());
            assertTrue(e.getMessage().contains("Cannot build " + theirs) && e.getMessage().contains("not a Zarr store"), e.getMessage());
        }
        assertThrows(IllegalArgumentException.class, () -> WritableStore.create(theirs, Container.ZARR));
        // A finished build that finds one at its destination is kept beside it, at once.
        Path built = dir.resolve("built.tmp");
        Files.createDirectories(built);
        Files.writeString(built.resolve("zarr.json"), "{}");
        long start = System.nanoTime();
        IOException e = assertThrows(IOException.class, () -> AtomicPublish.publish(built, theirs));
        assertTrue((System.nanoTime() - start) < 1_000_000_000L, "no retries: nothing would change");
        assertTrue(e.getMessage().contains("KEPT at") && e.getMessage().contains("theirs.zarr.new"), e.getMessage());
        assertTrue(Files.isRegularFile(dir.resolve("theirs.zarr.new/zarr.json")));
        assertEquals("keep me", Files.readString(theirs.resolve("notes.txt")));
        try (Stream<Path> entries = Files.list(theirs)) {
            assertEquals(1, entries.count(), "and nothing was added to it");
        }
        // The same directory under an HDF5 name: not replaced by a file either.
        Path theirsH5 = Files.createDirectories(dir.resolve("theirs.h5"));
        Files.writeString(theirsH5.resolve("notes.txt"), "keep me too");
        assertThrows(IOException.class, () -> WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), theirsH5.toFile()));
        assertEquals("keep me too", Files.readString(theirsH5.resolve("notes.txt")));

        assertTrue(Container.isReplaceable(dir.resolve("nothing-there")));
        assertTrue(Container.isReplaceable(src), "a file");
        assertTrue(Container.isReplaceable(Files.createDirectories(dir.resolve("empty"))));
        assertFalse(Container.isReplaceable(theirs));
    }

    @Test
    void aStoreThatIsADirectoryIsPublishedAndDiscardedWhole() throws Exception {
        Path dest = dir.resolve("published.zarr");
        // A failed build's directory goes, with all it holds; the destination is untouched.
        Files.createDirectories(dest.resolve("old"));
        Files.writeString(dest.resolve("old/file"), "old");
        Files.writeString(dest.resolve("zarr.json"), "{}"); // a store: the only directory a build replaces
        List<Path> temps = new ArrayList<>();
        assertThrows(IOException.class, () -> AtomicPublish.build(dest, tmp -> {
            temps.add(tmp);
            Files.createDirectories(tmp.resolve("a/b"));
            Files.writeString(tmp.resolve("a/b/file"), "half");
            throw new IOException("the build failed");
        }));
        assertFalse(Files.exists(temps.get(0)));
        assertEquals("old", Files.readString(dest.resolve("old/file")));

        // A finished one replaces the destination, directory for directory.
        AtomicPublish.build(dest, tmp -> {
            Files.createDirectories(tmp.resolve("new"));
            Files.writeString(tmp.resolve("new/file"), "new");
        });
        assertEquals("new", Files.readString(dest.resolve("new/file")));
        assertFalse(Files.exists(dest.resolve("old")));
        assertNothingBeside(dest);

        // And a file's place can be taken by a directory, and a directory's by a file.
        Path file = dir.resolve("was-a-file.zarr");
        Files.writeString(file, "a file");
        AtomicPublish.build(file, tmp -> {
            Files.createDirectories(tmp.resolve("d"));
            Files.writeString(tmp.resolve("zarr.json"), "{}");
        });
        assertTrue(Files.isDirectory(file.resolve("d")));
        AtomicPublish.build(file, tmp -> Files.writeString(tmp, "a file again"));
        assertEquals("a file again", Files.readString(file));
        assertNothingBeside(file);
    }

    @Test
    void aContainerIsNamedByADestinationAndFoundByWhatIsThere() throws Exception {
        assertEquals(Container.ZARR, Container.ofName("a.zarr"));
        assertEquals(Container.ZARR, Container.ofName("A.ZARR"));
        assertEquals(Container.HDF5, Container.ofName("a.h5"));
        assertEquals(Container.HDF5, Container.ofName("a.zarr.h5"));
        assertEquals(Container.HDF5, Container.ofName("a"));
        assertEquals(Container.HDF5, Container.ofName(null));
        assertEquals(Container.ZARR, Container.ofDestination(dir.resolve("x/y.zarr")));
        assertEquals("zarr", Container.ZARR.extension());
        assertEquals("h5", Container.HDF5.extension());

        Path file = write("looks-like.zarr", "a file");
        assertEquals(Container.HDF5, Container.ofExisting(file), "what is there decides, not its name");
        Path plain = Files.createDirectories(dir.resolve("plain"));
        assertEquals(Container.ZARR, Container.ofExisting(plain));
        assertFalse(Container.isZarrStore(plain));
        Files.writeString(plain.resolve("zarr.json"), "{}");
        assertTrue(Container.isZarrStore(plain));
        assertFalse(Container.isZarrStore(file));
    }
}
