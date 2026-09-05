package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** BG-75: the export's text memo sizes itself in long arithmetic and clamps an oversized cache setting. */
class IndexExportTextMapTest {

    private static void roundTrips(IndexExport.LongTextMap map) {
        map.put(7L, "seven");
        map.put(1L << 40, "big");
        assertEquals("seven", map.get(7L));
        assertEquals("big", map.get(1L << 40));
        assertNull(map.get(8L));
    }

    @Test
    void oversizedSettingsAreClampedNotCrashed() {
        // The arithmetic, without allocating: these used to be a negative array size (2^30)
        // or a zero-capacity table (Integer.MAX_VALUE) from int overflow.
        assertEquals(1 << 27, IndexExport.LongTextMap.capacityFor(Integer.MAX_VALUE));
        assertEquals(1 << 27, IndexExport.LongTextMap.capacityFor(1 << 30));
        assertEquals(1 << 27, IndexExport.LongTextMap.capacityFor(1 << 29));
        assertEquals(1 << 27, IndexExport.LongTextMap.capacityFor(IndexExport.LongTextMap.MAX_ENTRIES));
        assertEquals(262_144, IndexExport.LongTextMap.capacityFor(100_000), "a power of two, at least twice the entries");
        assertEquals(2048, IndexExport.LongTextMap.capacityFor(1), "below the floor: 1024 entries");
        assertEquals(IndexExport.LongTextMap.MAX_ENTRIES, IndexExport.LongTextMap.entriesFor(Integer.MAX_VALUE));
        for (int n : new int[]{1, 1024, 100_000}) {
            assertTrue(Integer.bitCount(IndexExport.LongTextMap.capacityFor(n)) == 1);
            assertTrue(IndexExport.LongTextMap.capacityFor(n) >= 2 * IndexExport.LongTextMap.entriesFor(n));
        }
        roundTrips(new IndexExport.LongTextMap(1));
        roundTrips(new IndexExport.LongTextMap(100_000));
    }
}
