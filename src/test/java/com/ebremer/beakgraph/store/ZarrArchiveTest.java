package com.ebremer.beakgraph.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.cmdline.VerifyCommand;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.BeakGraphFiles;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.io.MemorySegmentBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.io.SegmentedBytes;
import com.ebremer.falcon.zarr.Zarr;
import com.ebremer.falcon.zarr.ZarrArray;
import com.ebremer.falcon.zarr.ZarrGroup;
import com.ebremer.falcon.zarr.ZarrNode;
import com.ebremer.falcon.zarr.datatype.DataType;
import com.ebremer.falcon.zarr.store.ZipStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Zarr container in ONE file (SPECIFICATIONS.md §3a, item 8): a ZIP
 * archive whose entries are the keys of the directory store, none
 * compressed. It holds the same store, an entry is mapped where it lies in
 * the file, and what is not such an archive is named with its rule.
 */
class ZarrArchiveTest {

    @TempDir
    Path dir;

    private Path write(String name, String content) throws IOException {
        Path p = dir.resolve(name);
        Files.writeString(p, content, StandardCharsets.UTF_8);
        return p;
    }

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

    private static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)).substring(0, 16);
    }

    /** Every node of the store in an archive, as {@link ContainerParityTest#describeHdf5} describes one in HDF5: through Falcon. */
    private static SortedMap<String, String> describe(Path archive) throws Exception {
        SortedMap<String, String> out = new TreeMap<>();
        try (ZipStore zip = ZipStore.openReadOnly(archive)) {
            describe(Zarr.openGroup(zip, false), out);
        }
        return out;
    }

    private static void describe(ZarrGroup g, SortedMap<String, String> out) throws Exception {
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
                assertEquals(List.of("bytes"), a.codecNames(), child.path());
                byte[] bytes = a.readRawBytes();
                out.put(child.path(), "array " + bytes.length + " " + sha(bytes) + " " + attributes);
            }
        }
    }

    /** The files of a directory store, by key. */
    private static SortedMap<String, byte[]> files(Path zarr) throws IOException {
        SortedMap<String, byte[]> out = new TreeMap<>();
        try (Stream<Path> walk = Files.walk(zarr)) {
            for (Path p : walk.filter(Files::isRegularFile).toList()) {
                out.put(zarr.relativize(p).toString().replace('\\', '/'), Files.readAllBytes(p));
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
    void everyEngineWritesTheSameStoreIntoAnArchive() throws Exception {
        Path src = write("stable.trig", stableTrig());
        byte[] first = null;
        SortedMap<String, String> inHdf5 = null;
        Path zarr = dir.resolve("reference.zarr");
        for (WriterEngines.Engine engine : WriterEngines.all().toList()) {
            Path archive = dir.resolve(engine.name() + ".zarr.zip");
            engine.buildStore(src.toFile(), archive.toFile());
            assertTrue(Files.isRegularFile(archive), engine.name());
            assertNothingBeside(archive);
            if (first == null) {
                Path h5 = dir.resolve("reference.h5");
                engine.buildStore(src.toFile(), h5.toFile());
                engine.buildStore(src.toFile(), zarr.toFile());
                inHdf5 = ContainerParityTest.describeHdf5(h5);
                first = Files.readAllBytes(archive);
            }
            assertEquals(inHdf5, describe(archive), engine.name() + ": the same groups, arrays, bytes and attributes as in HDF5");
            // One archive whichever engine wrote it, and whenever: the entries are in one order and carry one time.
            assertArrayEquals(first, Files.readAllBytes(archive), engine.name() + ": the archive, byte for byte");
        }
        // The archive is the directory store, an entry a file: the same keys, the same bytes, nothing compressed.
        SortedMap<String, byte[]> onDisk = files(zarr);
        Path archive = dir.resolve(WriterEngines.all().findFirst().orElseThrow().name() + ".zarr.zip");
        List<String> names = new ArrayList<>();
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            for (ZipEntry entry : Collections.list(zip.entries())) {
                names.add(entry.getName());
                assertEquals(ZipEntry.STORED, entry.getMethod(), entry.getName());
                assertEquals(entry.getSize(), entry.getCompressedSize(), entry.getName());
                try (InputStream in = zip.getInputStream(entry)) {
                    assertArrayEquals(onDisk.get(entry.getName()), in.readAllBytes(), entry.getName()); // the JDK checks the CRC-32
                }
            }
        }
        assertEquals("zarr.json", names.get(0), "the document a reader opens the store from, where the archive begins");
        List<String> rest = names.subList(1, names.size());
        List<String> sorted = new ArrayList<>(rest);
        Collections.sort(sorted);
        assertEquals(sorted, rest, "the other keys in order");
        assertEquals(onDisk.keySet(), new java.util.TreeSet<>(names));
        assertTrue(Container.isZipArchive(archive));
        assertFalse(Container.isZipArchive(dir.resolve("reference.h5")));
    }

    @Test
    void theReadersAnswerTheSameFromAnArchive() throws Exception {
        Path src = write("mixed.trig", StoreParity.mixedTrig());
        for (WriterEngines.Engine engine : WriterEngines.all().filter(e -> e.name().startsWith("method0") || e.name().startsWith("method4")).toList()) {
            Path h5 = dir.resolve("q-" + engine.name() + ".h5");
            Path archive = dir.resolve("q-" + engine.name() + ".zarr.zip");
            engine.buildStore(src.toFile(), h5.toFile());
            engine.buildStore(src.toFile(), archive.toFile());
            StoreParity.assertStoresEquivalent(h5, archive,
                    "SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }",
                    "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                    "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o FILTER(?o > 10) }");
            assertEquals(0, new VerifyCommand(archive.toFile(), true).run(), "-verify -deep of the archive");
            try (HDF5Reader inArchive = new HDF5Reader(archive.toFile()); HDF5Reader inHdf5 = new HDF5Reader(h5)) {
                assertEquals(Container.ZARR_ZIP, inArchive.getContainer());
                assertFalse(inArchive.isChannelBacked(), "an archive is memory-mapped");
                assertEquals(inHdf5.getFormatVersion(), inArchive.getFormatVersion());
                assertEquals(inHdf5.getNumQuads(), inArchive.getNumQuads());
                assertEquals(Params.FORMAT_VERSION, inArchive.getFormatVersion());
            }
        }
    }

    @Test
    void anEntryIsReadWhereItLiesInTheArchive() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path zarr = dir.resolve("view.zarr");
        Path archive = dir.resolve("view.zarr.zip");
        WriterEngines.Engine engine = WriterEngines.all().findFirst().orElseThrow();
        engine.buildStore(src.toFile(), zarr.toFile());
        engine.buildStore(src.toFile(), archive.toFile());
        byte[] whole = Files.readAllBytes(archive);
        try (OpenStore open = OpenStore.open(archive)) {
            assertEquals(Container.ZARR_ZIP, open.container());
            assertFalse(open.isChannelBacked());
            for (String name : new String[] {"So", "So_blocks", "Bo", "SBo"}) {
                StoreArray array = open.root().group(".BG").group("GSPO").array(name);
                RandomAccessBytes view = array.bytes();
                // One class in the hottest call site of the read path, whichever container a JVM has open.
                assertInstanceOf(MemorySegmentBytes.class, view, name);
                byte[] expected = Files.readAllBytes(zarr.resolve(".BG/GSPO/" + name + "/c/0"));
                byte[] read = new byte[(int) view.size()];
                view.get(0, read, 0, read.length);
                assertArrayEquals(expected, read, name);
                // ... and those bytes are the archive's own: the entry is not compressed, so it is in the file as it is.
                assertTrue(expected.length < 4 || indexOf(whole, expected) >= 0, name + " lies in the archive as its bytes");
                assertTrue(array.bytes() == view, "an array is mapped once, however often it is asked for");
            }
        }
        // Closed, the archive is free: Windows refuses to delete a file that is mapped or open.
        Files.delete(archive);
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int k = 0; k < needle.length; k++) {
                if (haystack[i + k] != needle[k]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    @Test
    void anArrayCutIntoSeveralChunksReadsTheSameFromAnArchive() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path h5 = dir.resolve("chunks.h5");
        Path archive = dir.resolve("chunks.zarr.zip");
        WriterEngines.Engine inMemory = WriterEngines.all().findFirst().orElseThrow();
        WriterEngines.Engine disk = WriterEngines.all().filter(WriterEngines.Engine::diskBased).findFirst().orElseThrow();
        inMemory.buildStore(src.toFile(), h5.toFile());
        long previous = ZarrStoreWriter.setChunkMax(100);
        try {
            for (WriterEngines.Engine engine : List.of(inMemory, disk)) {
                engine.buildStore(src.toFile(), archive.toFile()); // the second build replaces the first
                assertEquals(ContainerParityTest.describeHdf5(h5), describe(archive), engine.name());
                StoreParity.assertStoresEquivalent(h5, archive, "SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }");
                assertEquals(0, new VerifyCommand(archive.toFile(), true).run(), engine.name());
                try (OpenStore open = OpenStore.open(archive); ZipStore zip = ZipStore.openReadOnly(archive)) {
                    ZarrArray strings = Zarr.openGroup(zip).array(".BG/dictionary/literals/strings/stringbuffer");
                    assertTrue(strings.gridShape()[0] > 1, "cut into chunks");
                    RandomAccessBytes view = open.root().group(".BG").group("dictionary").group("literals").group("strings")
                            .array("stringbuffer").bytes();
                    assertInstanceOf(SegmentedBytes.class, view, "several entries, each mapped, read as one array");
                    byte[] read = new byte[(int) view.size()];
                    view.get(0, read, 0, read.length);
                    assertArrayEquals(strings.readRawBytes(), read);
                    assertThrows(IndexOutOfBoundsException.class, () -> view.get(view.size()));
                }
                assertNothingBeside(archive);
            }
        } finally {
            ZarrStoreWriter.setChunkMax(previous);
        }
    }

    @Test
    void aStoreIsNamedByItsDestinationAndOpenedByWhatItIs() throws Exception {
        assertEquals(Container.ZARR_ZIP, Container.ofName("a.zarr.zip"));
        assertEquals(Container.ZARR_ZIP, Container.ofName("a.ZIP"));
        assertEquals(Container.ZARR, Container.ofName("a.zarr"));
        assertEquals(Container.HDF5, Container.ofName("a.zip.h5"));
        assertEquals("zarr.zip", Container.ZARR_ZIP.extension());
        assertFalse(Container.ZARR_ZIP.isDirectory());
        assertTrue(Container.ZARR.isDirectory());
        assertTrue(BeakGraphFiles.isStoreName("a.zarr.ZIP") && BeakGraphFiles.isZarrArchiveName("a.zarr.zip"));
        assertFalse(BeakGraphFiles.isStoreName("photos.zip"), "a tree holds other archives, and they are not stores");
        assertTrue(BeakGraphFiles.isStoreFileName("out.zip") && BeakGraphFiles.isStoreFileName("out.h5")
                && !BeakGraphFiles.isStoreFileName("out.zarr") && !BeakGraphFiles.isStoreFileName("out"));
        assertEquals("a", BeakGraphFiles.stripExtension("a.zarr.zip"));
        assertEquals("a", BeakGraphFiles.stripExtension("a.zip"));

        // Opened by what is there, whatever it is called: an archive named .h5, an HDF5 file named .zip.
        Path src = write("stable.trig", stableTrig());
        WriterEngines.Engine engine = WriterEngines.all().findFirst().orElseThrow();
        Path archive = dir.resolve("named.zarr.zip");
        Path h5 = dir.resolve("named.h5");
        engine.buildStore(src.toFile(), archive.toFile());
        engine.buildStore(src.toFile(), h5.toFile());
        Path archiveAsH5 = Files.copy(archive, dir.resolve("really-an-archive.h5"));
        Path h5AsZip = Files.copy(h5, dir.resolve("really-hdf5.zip"));
        assertEquals(Container.ZARR_ZIP, Container.ofExisting(archiveAsH5));
        assertEquals(Container.HDF5, Container.ofExisting(h5AsZip));
        String count = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }";
        long expected;
        try (BeakGraph bg = BG.getBeakGraph(h5)) {
            expected = StoreParity.count(bg.getDataset(), count);
        }
        for (Path p : List.of(archive, archiveAsH5, h5AsZip)) {
            try (BeakGraph bg = BG.getBeakGraph(p)) {
                assertEquals(expected, StoreParity.count(bg.getDataset(), count), p.toString());
            }
        }
        assertEquals(List.of(h5, archive, archiveAsH5), BeakGraphFiles.findStores(dir).stream()
                .filter(p -> p.getFileName().toString().startsWith("named") || p.getFileName().toString().startsWith("really")).toList());
    }

    @Test
    void whatIsNotAStoreInAnArchiveIsSaidSo() throws Exception {
        Path src = write("stable.trig", stableTrig());
        Path zarr = dir.resolve("source.zarr");
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), zarr.toFile());
        // The store zipped as most tools zip: its entries deflated. An entry is then not the chunk's bytes.
        Path deflated = dir.resolve("deflated.zarr.zip");
        try (OutputStream out = Files.newOutputStream(deflated); ZipOutputStream zip = new ZipOutputStream(out)) {
            for (Map.Entry<String, byte[]> file : files(zarr).entrySet()) {
                zip.putNextEntry(new ZipEntry(file.getKey()));
                zip.write(file.getValue());
                zip.closeEntry();
            }
        }
        RuntimeException compressed = assertThrows(RuntimeException.class, () -> new HDF5Reader(deflated.toFile()).close());
        String message = rootMessage(compressed);
        assertTrue(message.contains("compressed") && message.contains("§3a, item 8") && message.contains("cannot be memory-mapped"), message);
        assertEquals(2, new VerifyCommand(deflated.toFile(), true).run(), "-verify fails it");
        Files.delete(deflated); // and nothing of it is held

        // An archive of something else: no zarr.json where a store has one.
        Path other = dir.resolve("other.zarr.zip");
        try (OutputStream out = Files.newOutputStream(other); ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.putNextEntry(new ZipEntry("readme.txt"));
            zip.write("not a store".getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        RuntimeException notAStore = assertThrows(RuntimeException.class, () -> new HDF5Reader(other.toFile()).close());
        assertTrue(rootMessage(notAStore).length() > 0 && notAStore.getMessage().contains("other.zarr.zip"), notAStore.getMessage());
        Files.delete(other);

        // A file cut short: the archive's directory is at its end.
        Path archive = dir.resolve("whole.zarr.zip");
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), archive.toFile());
        byte[] bytes = Files.readAllBytes(archive);
        Path cut = dir.resolve("cut.zarr.zip");
        Files.write(cut, java.util.Arrays.copyOf(bytes, bytes.length / 2));
        assertThrows(RuntimeException.class, () -> new HDF5Reader(cut.toFile()).close());
        assertEquals(2, new VerifyCommand(cut.toFile(), true).run());
        Files.delete(cut);
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return String.valueOf(root.getMessage());
    }

    @Test
    void anArchiveIsAtItsPathCompleteOrNotAtAll() throws Exception {
        Path archive = dir.resolve("atomic.zarr.zip");
        // Closed without a commit: nothing at the path, nothing beside it.
        try (WritableStore store = WritableStore.create(archive, Container.ZARR_ZIP)) {
            assertEquals(Container.ZARR_ZIP, store.container());
            store.putGroup(".BG").putDataset("a", new byte[] {1, 2, 3});
        }
        assertFalse(Files.exists(archive));
        assertNothingBeside(archive);
        // An array short of its length: refused, and nothing is at the path.
        try (WritableStore store = WritableStore.create(archive, Container.ZARR_ZIP)) {
            store.putGroup(".BG").createDataset("short", 10).write(new byte[4], 0, 4);
            IOException e = assertThrows(IOException.class, store::commit);
            assertTrue(e.getMessage().contains("4 of 10"), e.getMessage());
        }
        assertFalse(Files.exists(archive));
        assertNothingBeside(archive);
        // Committed: one file, which replaces the one that was there - and a directory store that was.
        for (int round = 0; round < 2; round++) {
            try (WritableStore store = WritableStore.create(archive, Container.ZARR_ZIP)) {
                var bg = store.putGroup(".BG");
                bg.putAttribute("round", round);
                bg.putDataset("a", new byte[] {1, 2, 3, (byte) round});
                bg.createDataset("empty", 0);
                store.commit();
            }
            assertTrue(Files.isRegularFile(archive));
            assertNothingBeside(archive);
            try (OpenStore open = OpenStore.open(archive)) {
                StoreGroup bg = open.root().group(".BG");
                assertEquals(round, bg.attribute("round"));
                assertEquals(round, bg.array("a").bytes().get(3));
                assertEquals(0, bg.array("empty").bytes().size());
            }
        }
        Path wasDirectory = dir.resolve("was.zarr");
        try (WritableStore store = WritableStore.create(wasDirectory, Container.ZARR)) {
            store.putGroup(".BG").putDataset("a", new byte[] {9});
            store.commit();
        }
        assertTrue(Files.isDirectory(wasDirectory));
        try (WritableStore store = WritableStore.create(wasDirectory, Container.ZARR_ZIP)) {
            store.putGroup(".BG").putDataset("a", new byte[] {8});
            store.commit();
        }
        assertTrue(Files.isRegularFile(wasDirectory), "a Zarr store is replaced whole, by the archive too");
        assertNothingBeside(wasDirectory);
        try (OpenStore open = OpenStore.open(wasDirectory)) {
            assertEquals(Container.ZARR_ZIP, open.container());
            assertEquals(8, open.root().group(".BG").array("a").bytes().get(0));
        }
    }
}
