package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.DictionarySinks;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * BG-299: the level/padding/dedup rule now lives once, so it gets a direct
 * test on tiny sinks: duplicates are dropped, a skipped L0 id costs one
 * dummy row (id 0, bit 1) per level, {@code finish} pads the tail, and the
 * SB/BB directories are seeded and advanced per block/superblock.
 */
class IndexLevelEmitterTest {

    static final class Longs implements DictionarySinks.LongSink {
        final List<Long> values = new ArrayList<>();
        @Override public void writeInteger(int value) { values.add((long) value); }
        @Override public void writeLong(long value) { values.add(value); }
        @Override public long getNumEntries() { return values.size(); }
    }

    @Test
    void dedupPaddingAndTailFollowBgIndex() {
        Longs S1 = new Longs(), B1 = new Longs(), SB1 = new Longs(), BB1 = new Longs();
        Longs S2 = new Longs(), B2 = new Longs(), SB2 = new Longs(), BB2 = new Longs();
        Longs S3 = new Longs(), B3 = new Longs(), SB3 = new Longs(), BB3 = new Longs();
        IndexLevelEmitter out = new IndexLevelEmitter(S1, B1, SB1, BB1, S2, B2, SB2, BB2, S3, B3, SB3, BB3);
        out.emit(1, 1, 1, 1);
        out.emit(1, 1, 1, 1);   // duplicate: dropped
        out.emit(1, 1, 1, 2);   // same L0/L1/L2, new L3
        out.emit(3, 1, 1, 1);   // L0 id 2 skipped: one dummy row per level
        out.finish(4);          // L0 id 4 never seen: one more dummy row per level

        assertEquals(List.of(1L, 0L, 1L, 0L), S1.values);
        assertEquals(List.of(1L, 1L, 1L, 1L), B1.values);
        assertEquals(List.of(1L, 0L, 1L, 0L), S2.values);
        assertEquals(List.of(1L, 1L, 1L, 1L), B2.values);
        assertEquals(List.of(1L, 2L, 0L, 1L, 0L), S3.values);
        assertEquals(List.of(1L, 0L, 1L, 1L, 1L), B3.values);
        for (Longs dir : List.of(SB1, SB2, SB3, BB1, BB2, BB3)) {
            assertEquals(List.of(0L), dir.values, "seeded with 0 and no boundary crossed");
        }
    }

    @Test
    void directoriesAdvanceAtBlockAndSuperblockBoundaries() {
        Longs S1 = new Longs(), B1 = new Longs(), SB1 = new Longs(), BB1 = new Longs();
        Longs S2 = new Longs(), B2 = new Longs(), SB2 = new Longs(), BB2 = new Longs();
        Longs S3 = new Longs(), B3 = new Longs(), SB3 = new Longs(), BB3 = new Longs();
        IndexLevelEmitter out = new IndexLevelEmitter(S1, B1, SB1, BB1, S2, B2, SB2, BB2, S3, B3, SB3, BB3);
        // One L0 id whose L3 level holds exactly SUPERBLOCKSIZE + 1 rows: the
        // first row sets a bit, every later one is a 0 bit.
        long rows = Params.SUPERBLOCKSIZE + 1;
        for (long o = 1; o <= rows; o++) {
            out.emit(1, 1, 1, o);
        }
        out.finish(1);
        assertEquals(rows, S3.values.size());
        // BB3: seeded 0, then one entry per block boundary inside the superblock
        // (ones so far in that superblock = 1), then the superblock boundary
        // resets the count.
        long blocksPerSuperblock = Params.SUPERBLOCKSIZE / Params.BLOCKSIZE;
        assertEquals(blocksPerSuperblock + 1, BB3.values.size());
        assertEquals(0L, BB3.values.get(0));
        for (int i = 1; i < blocksPerSuperblock; i++) {
            assertEquals(1L, BB3.values.get(i), "block " + i);
        }
        assertEquals(0L, BB3.values.get((int) blocksPerSuperblock), "the superblock boundary reset the block count");
        assertEquals(List.of(0L, 1L), SB3.values, "SB3: seeded 0, then ones before superblock 1");
    }
}
