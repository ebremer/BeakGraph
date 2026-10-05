package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.writers.FCDWriter;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Group;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * FCD block decoding after the per-entry copies went away (BG-256): every
 * string comes back exactly, whether ASCII (appended straight from the
 * scratch bytes), multi-byte UTF-8, surrogate pairs, empty, or long enough
 * to be zstd-compressed - across block boundaries and in any access order.
 */
class FCDReaderDecodeTest {

    @TempDir
    Path dir;

    @Test
    void everyKindOfStringRoundTripsThroughTheBlockDecoder() throws Exception {
        List<String> strings = new ArrayList<>();
        strings.add("");
        strings.add("a");
        strings.add("abc");
        strings.add("abd");
        strings.add("abé");                       // shares "ab", suffix is multi-byte
        strings.add("日本語テキスト"); // CJK
        strings.add("emoji 😀 pair");          // surrogate pair
        strings.add("emoji 😀 pair two");      // shares the pair in the prefix
        strings.add("prefix-" + "x".repeat(200));        // compressed suffix (past the 64-byte threshold)
        strings.add("prefix-" + "y".repeat(300) + "é");
        strings.add("z".repeat(64));                     // exactly at the threshold
        for (int i = 0; i < 60; i++) {
            strings.add("item" + String.format("%03d", i));
        }
        strings.add("é".repeat(100));               // compressed AND non-ASCII

        Path f = dir.resolve("fcd.h5");
        try (FCDWriter w = new FCDWriter(Path.of("strings"), Params.FCD_BLOCK_SIZE)) {
            for (String s : strings) w.add(s);
            try (WritableHdfFile out = HdfFile.write(f)) {
                w.add(out.putGroup("dict"));
            }
        }
        try (HdfFile in = new HdfFile(f)) {
            Group g = (Group) ((Group) in.getChild("dict")).getChild("strings");
            FCDReader r = new FCDReader(g);
            assertEquals(strings.size(), r.getNumEntries());
            for (int i = 0; i < strings.size(); i++) {
                assertEquals(strings.get(i), r.get(i), "entry " + i + " in order");
            }
            List<Integer> order = new ArrayList<>();
            for (int i = 0; i < strings.size(); i++) order.add(i);
            Collections.shuffle(order, new Random(256));
            FCDReader fresh = new FCDReader(g); // cold cache, random access
            for (int i : order) {
                assertEquals(strings.get(i), fresh.get(i), "entry " + i + " out of order");
            }
        }
    }
}
