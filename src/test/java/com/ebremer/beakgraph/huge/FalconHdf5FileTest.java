package com.ebremer.beakgraph.huge;

import com.ebremer.falcon.hdf5.Dataset;
import com.ebremer.falcon.hdf5.Hdf5File;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The backend the disk-based writers assemble a store through
 * ({@link FalconHdf5File}), held to the contract of {@link StreamingHdf5File}:
 * what it writes piece by piece is the container profile the readers hold a
 * store to (SPECIFICATIONS.md §3) - checked with jHDF, a reader that is not
 * the one that wrote it, and with Falcon, which must be able to map each
 * dataset where it lies - and a file that is not finished is not published.
 */
class FalconHdf5FileTest {

    @TempDir
    Path dir;

    private static byte[] pattern(int length, int seed) {
        byte[] pattern = new byte[length];
        for (int i = 0; i < length; i++) {
            pattern[i] = (byte) (i * 31 + (i >> 8) + seed);
        }
        return pattern;
    }

    private static List<Path> entries(Path directory) throws IOException {
        try (var s = Files.list(directory)) {
            return s.toList();
        }
    }

    @Test
    void piecesReadBackAsOneContiguousDataset() throws Exception {
        Path h5 = dir.resolve("pieces.h5");
        byte[] offsets = pattern(300_000, 0);
        byte[] strings = pattern(70_001, 7);

        try (StreamingHdf5File f = FalconHdf5File.create(h5)) {
            StreamingHdf5Group bg = f.rootGroup().putGroup(".BG");
            bg.putAttribute("numQuads", 42L);
            bg.putAttribute("formatVersion", 3);
            StreamingHdf5Group dictionary = bg.putGroup("dictionary");
            // Two datasets open at once, their pieces interleaved: each lies whole and in order in the file.
            try (StreamingHdf5Dataset a = dictionary.createByteDataset("offsets", offsets.length);
                 StreamingHdf5Dataset b = dictionary.createByteDataset("stringbuffer", strings.length)) {
                a.write(offsets, 0, 100_000);
                b.write(strings, 0, 70_000);
                a.write(offsets, 100_000, 150_000);         // a piece from the middle of the caller's buffer
                a.write(offsets, 250_000, 0);               // nothing
                b.write(strings, 70_000, 1);
                a.write(offsets, 250_000, 50_000);
                // Attributes follow the data, as the spill buffers write them.
                a.putAttribute("width", 7);
                a.putAttribute("numEntries", 999L);
            }
        }
        assertEquals(List.of(h5), entries(dir), "the file, and nothing beside it");

        try (HdfFile hdf = new HdfFile(h5)) {
            Group bg = (Group) hdf.getChild(".BG");
            assertNotNull(bg, "the .BG group must exist");
            Object numQuads = bg.getAttribute("numQuads").getData();
            assertInstanceOf(Long.class, numQuads, "numQuads is a 64-bit attribute");
            assertEquals(42L, numQuads);
            Object version = bg.getAttribute("formatVersion").getData();
            assertInstanceOf(Integer.class, version, "formatVersion is a 32-bit attribute");
            assertEquals(3, version);

            Group dictionary = (Group) bg.getChild("dictionary");
            ContiguousDataset a = assertInstanceOf(ContiguousDataset.class, dictionary.getChild("offsets"));
            assertEquals(7, a.getAttribute("width").getData(), "width (32-bit)");
            assertEquals(999L, a.getAttribute("numEntries").getData(), "numEntries (64-bit)");
            assertEquals(byte.class, a.getJavaType(), "8-bit elements");
            assertArrayEquals(offsets, bytes(a.getBuffer()));
            ContiguousDataset b = assertInstanceOf(ContiguousDataset.class, dictionary.getChild("stringbuffer"));
            assertArrayEquals(strings, bytes(b.getBuffer()));
        }

        // The readers' own library maps each dataset where it lies: no copy, no filter.
        try (Hdf5File falcon = Hdf5File.open(h5)) {
            Dataset a = falcon.root().dataset(".BG/dictionary/offsets");
            MemorySegment mapped = a.storageSegment().orElseThrow();
            assertArrayEquals(offsets, mapped.toArray(ValueLayout.JAVA_BYTE));
            assertEquals(7, a.attribute("width").orElseThrow().readInt());
            assertEquals(999L, a.attribute("numEntries").orElseThrow().readLong());
            assertArrayEquals(strings, falcon.root().dataset(".BG/dictionary/stringbuffer").storageSegment().orElseThrow()
                    .toArray(ValueLayout.JAVA_BYTE));
        }
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    @Test
    void aFailedCreateNamesTheFullPath() throws Exception {
        try (StreamingHdf5File f = FalconHdf5File.create(dir.resolve("names.h5"))) {
            StreamingHdf5Group a = f.rootGroup().putGroup("a");
            a.putGroup("b");
            IOException root = assertThrows(IOException.class, () -> f.rootGroup().putGroup("a"));
            assertTrue(root.getMessage().startsWith("Cannot create group '/a'"), root.getMessage());
            assertNotNull(root.getCause());
            IOException nested = assertThrows(IOException.class, () -> a.putGroup("b"));
            assertTrue(nested.getMessage().startsWith("Cannot create group '/a/b'"), nested.getMessage());
            IOException taken = assertThrows(IOException.class, () -> a.createByteDataset("b", 4));
            assertTrue(taken.getMessage().startsWith("Cannot create dataset '/a/b'"), taken.getMessage());
            IOException empty = assertThrows(IOException.class, () -> a.createByteDataset("c", 0));
            assertEquals("Refusing to create empty dataset '/a/c'", empty.getMessage());
        }
    }

    @Test
    void aDatasetTakesExactlyItsLength() throws Exception {
        Path sub = Files.createDirectories(dir.resolve("exact"));
        Path h5 = sub.resolve("exact.h5");
        StreamingHdf5File f = FalconHdf5File.create(h5);
        StreamingHdf5Dataset d = f.rootGroup().createByteDataset("d", 10);
        d.write(new byte[4]);
        IOException overflow = assertThrows(IOException.class, () -> d.write(new byte[7]));
        assertEquals("Dataset '/d' overflow: 11 > declared length 10", overflow.getMessage());
        IOException early = assertThrows(IOException.class, d::close);
        assertEquals("Dataset '/d' closed after 4 of 10 bytes - refusing to leave undefined data", early.getMessage());
        d.close();      // a second close says nothing more

        // The file is short of a dataset's bytes: closing it publishes nothing, at the path or beside it.
        IOException unfinished = assertThrows(IOException.class, f::close);
        assertTrue(unfinished.getMessage().contains("dataset '/d' at 4 of 10 bytes"), unfinished.getMessage());
        assertEquals(List.of(), entries(sub), "nothing at the path, and no temporary file beside it");
        f.close();      // idempotent
    }

    @Test
    void aFileAtThePathIsReplacedOnlyByAFinishedOne() throws Exception {
        Path sub = Files.createDirectories(dir.resolve("replace"));
        Path h5 = sub.resolve("store.h5");
        try (StreamingHdf5File f = FalconHdf5File.create(h5)) {
            f.rootGroup().putAttribute("generation", 1);
        }
        // A second file begun at the same path and abandoned short leaves the first as it was.
        StreamingHdf5File second = FalconHdf5File.create(h5);
        second.rootGroup().putAttribute("generation", 2);
        second.rootGroup().createByteDataset("d", 8).write(new byte[3]);
        assertThrows(IOException.class, second::close);
        try (HdfFile hdf = new HdfFile(h5)) {
            assertEquals(1, hdf.getAttribute("generation").getData());
        }
        // A finished one replaces it.
        try (StreamingHdf5File third = FalconHdf5File.create(h5)) {
            third.rootGroup().putAttribute("generation", 3);
        }
        try (HdfFile hdf = new HdfFile(h5)) {
            assertEquals(3, hdf.getAttribute("generation").getData());
        }
        assertEquals(List.of(h5), entries(sub));
    }

    /**
     * BG-419: the native library took a path across JNI as modified UTF-8, so a
     * supplementary-plane character (an emoji, CJK Extension B) made it create a
     * differently named file, and a path past MAX_PATH failed. A path is a
     * {@link Path} all the way down now: any name the file system takes works,
     * under its own name.
     */
    @Test
    void anyPathTheFileSystemTakesWorks() throws Exception {
        String deep = "d".repeat(100);
        for (Path sub : List.of(
                dir.resolve("José Ümläut"),
                dir.resolve("emoji-😀"),
                dir.resolve(deep).resolve(deep).resolve(deep))) {       // past 260 characters
            Files.createDirectories(sub);
            Path h5 = sub.resolve("smile-🐦.h5");
            StreamingHdf5.probeFile(h5);
            assertEquals(List.of(), entries(sub), "the probe removes its file");
            try (StreamingHdf5File f = StreamingHdf5.create(h5)) {
                f.rootGroup().putGroup(".BG").putAttribute("numQuads", 7L);
            }
            assertEquals(List.of(h5), entries(sub), "created under its own name");
            try (Hdf5File falcon = Hdf5File.open(h5)) {
                assertEquals(7L, falcon.root().group(".BG").attribute("numQuads").orElseThrow().readLong());
            }
        }
    }

    @Test
    void theProbeReportsAPathThatCannotBeWritten() throws Exception {
        // A directory where the file should go: no backend can create a file of that name.
        Path taken = Files.createDirectories(dir.resolve("taken.h5"));
        Files.writeString(taken.resolve("inside.txt"), "x");
        IOException e = assertThrows(IOException.class, () -> StreamingHdf5.probeFile(taken));
        assertTrue(e.getMessage().startsWith("The streaming HDF5 backend (Falcon) cannot write a file in " + dir.toAbsolutePath()),
                e.getMessage());
        assertTrue(Files.isDirectory(taken) && Files.exists(taken.resolve("inside.txt")), "what was there is untouched");
        assertEquals(List.of(taken), entries(dir), "and the attempt left nothing beside it");
    }
}
