package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-78: the decoded-block cache used to be bounded by block COUNT (4096
 * blocks of 16 strings), so a literals section of large strings - WKT
 * polygons, long text - pinned up to 65,536 fully materialised strings per
 * open store, gigabytes for big geometries. It is weight-bounded now, with
 * the same "1 per string plus 1 per 256 chars" charge the node-table and
 * search caches use, so large-literal sections keep proportionally fewer
 * blocks.
 */
class FCDReaderCacheWeightTest {

    private static final String STRINGS = Params.BG + "/" + Params.DICTIONARY + "/literals/strings";
    /** 200 blocks of 16 strings of ~8 KB: weight 16 * 33 = 528 per block, 105 KB of text per block. */
    private static final int LITERALS = 3200;
    private static final int LITERAL_CHARS = 8192;

    @TempDir
    static Path dir;
    static HdfFile file;
    static FCDReader reader;

    @BeforeAll
    static void build() throws Exception {
        StringBuilder sb = new StringBuilder("@prefix ex: <http://ex.org/> .\n");
        for (int i = 0; i < LITERALS; i++) {
            // Distinct, incompressible-ish prefixes so front-coding shares little
            // and every string materialises at full length.
            String body = Integer.toString(i * 7919, 36).repeat(LITERAL_CHARS);
            sb.append("ex:s").append(i).append(" ex:text \"").append(i).append('-')
                    .append(body, 0, Math.min(body.length(), LITERAL_CHARS)).append("\" .\n");
        }
        File ttl = dir.resolve("big-literals.ttl").toFile();
        Files.writeString(ttl.toPath(), sb.toString(), StandardCharsets.UTF_8);
        File h5 = dir.resolve("big-literals.h5").toFile();
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        file = new HdfFile(h5.toPath());
        reader = new FCDReader((Group) file.getByPath(STRINGS));
    }

    @AfterAll
    static void close() {
        if (file != null) file.close();
    }

    @Test
    void weightChargesLargeStringsPerTwoHundredFiftySixChars() {
        assertEquals(16, FCDReader.weightOf(sixteen("")), "an ordinary block weighs its string count");
        assertEquals(16, FCDReader.weightOf(sixteen("x".repeat(255))));
        assertEquals(32, FCDReader.weightOf(sixteen("x".repeat(256))));
        assertEquals(16 * 33, FCDReader.weightOf(sixteen("x".repeat(8192))));
        assertEquals(16, FCDReader.ORDINARY_BLOCK_WEIGHT);
    }

    private static String[] sixteen(String s) {
        String[] out = new String[16];
        java.util.Arrays.fill(out, s);
        return out;
    }

    @Test
    void largeLiteralSectionKeepsFewerBlocksThanTheCountBound() {
        long n = reader.getNumEntries();
        assertTrue(n >= LITERALS, "fixture: " + n + " literals");
        long blocks = (n + 15) / 16;
        assertTrue(blocks >= 190, "fixture: " + blocks + " blocks");
        for (long i = 0; i < n; i++) {
            assertTrue(reader.get(i).length() > 0);
        }
        long cached = reader.cachedBlocks();
        // Budget 4096 * 16 = 65,536 weight units; a block of sixteen ~8 KB
        // strings weighs ~528, so at most ~124 blocks fit - well below the 200
        // the count bound would have kept in full.
        assertTrue(cached > 0, "some blocks stay cached");
        assertTrue(cached < 140, "weight bound must evict large-literal blocks: " + cached + " of " + blocks + " cached");
        assertTrue(cached < blocks, "not every block of the section may stay resident: " + cached);
    }
}
