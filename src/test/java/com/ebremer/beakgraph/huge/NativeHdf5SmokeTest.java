package com.ebremer.beakgraph.huge;

import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The reader-compatibility contract of the native backend, verified against the
 * ACTUAL production reader stack (jHDF): contiguous-layout byte datasets
 * written in several slabs, groups (including the dot-prefixed .BG name), and
 * scalar attributes that come back as exactly Integer / Long - the boxed types
 * the BeakGraph readers cast to.
 */
class NativeHdf5SmokeTest {

    @TempDir
    Path dir;

    @Test
    void aFailedCreateNamesTheFullPath() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        // A group's path is the only thing NativeGroup keeps besides its handle
        // (BG-132); a duplicate name is the one create failure a test can force.
        try (StreamingHdf5File f = NativeHdf5File.create(dir.resolve("dup.h5"))) {
            StreamingHdf5Group a = f.rootGroup().putGroup("a");
            a.putGroup("b");
            java.io.IOException root = org.junit.jupiter.api.Assertions.assertThrows(
                    java.io.IOException.class, () -> f.rootGroup().putGroup("a"));
            assertEquals("H5Gcreate failed for group '/a'", root.getMessage());
            java.io.IOException nested = org.junit.jupiter.api.Assertions.assertThrows(
                    java.io.IOException.class, () -> a.putGroup("b"));
            assertEquals("H5Gcreate failed for group '/a/b'", nested.getMessage());
            java.io.IOException empty = org.junit.jupiter.api.Assertions.assertThrows(
                    java.io.IOException.class, () -> a.createByteDataset("c", 0));
            assertEquals("Refusing to create empty dataset '/a/c'", empty.getMessage());
        }
    }

    @Test
    void nativeWrittenFileReadsBackThroughJhdf() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Path h5 = dir.resolve("smoke.h5");

        int len = 300_000;
        byte[] pattern = new byte[len];
        for (int i = 0; i < len; i++) {
            pattern[i] = (byte) (i * 31 + (i >> 8));
        }

        try (StreamingHdf5File f = NativeHdf5File.create(h5)) {
            StreamingHdf5Group bg = f.rootGroup().putGroup(".BG");
            bg.putAttribute("numQuads", 42L);
            bg.putAttribute("formatVersion", 3);
            StreamingHdf5Group dict = bg.putGroup("dictionary");
            try (StreamingHdf5Dataset ds = dict.createByteDataset("offsets", len)) {
                // three unequal slabs, one with a non-zero source offset
                ds.write(pattern, 0, 100_000);
                ds.write(pattern, 100_000, 150_000);
                ds.write(pattern, 250_000, 50_000);
                ds.putAttribute("width", 7);
                ds.putAttribute("numEntries", 999L);
            }
        }

        try (HdfFile hdf = new HdfFile(h5)) {
            Group bg = (Group) hdf.getChild(".BG");
            assertNotNull(bg, "the .BG group must exist");
            Object numQuads = bg.getAttribute("numQuads").getData();
            assertInstanceOf(Long.class, numQuads, "numQuads must read back as Long");
            assertEquals(42L, numQuads);
            Object version = bg.getAttribute("formatVersion").getData();
            assertInstanceOf(Integer.class, version, "formatVersion must read back as Integer");
            assertEquals(3, version);

            Group dict = (Group) bg.getChild("dictionary");
            ContiguousDataset ds = (ContiguousDataset) dict.getChild("offsets");
            assertEquals(7, ds.getAttribute("width").getData(), "width attribute (Integer)");
            assertEquals(999L, ds.getAttribute("numEntries").getData(), "numEntries attribute (Long)");
            ByteBuffer buf = ds.getBuffer();
            assertEquals(len, buf.remaining(), "dataset length");
            byte[] readBack = new byte[len];
            buf.get(readBack);
            for (int i = 0; i < len; i++) {
                if (readBack[i] != pattern[i]) {
                    assertEquals(pattern[i], readBack[i], "byte mismatch at " + i);
                }
            }
        }
    }
}
