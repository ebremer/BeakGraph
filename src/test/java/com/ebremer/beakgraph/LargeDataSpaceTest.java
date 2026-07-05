package com.ebremer.beakgraph;

import io.jhdf.Superblock;
import io.jhdf.object.message.DataSpace;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Guards the jHDF large-dataset capability BeakGraph depends on: dataspace dimensions
 * must parse as unsigned longs so a store containing datasets with more than
 * Integer.MAX_VALUE elements - routine for the native-HDF5 writers at PubMed scale
 * (a 74 GB store's dictionary string buffer is ~29.6e9 bytes) - can be OPENED at all.
 * jHDF fixed this in 0.12.0 (long[] dimensions); before that, object-header parsing
 * threw {@code ArithmeticException: integer overflow} from group child loading, before
 * the reader could reach its own FFM large-dataset path (DatasetBytes). If a future
 * jhdf.version bump regresses this (or someone downgrades below 0.12.0), this test
 * fails instead of every multi-GiB store becoming unopenable.
 */
class LargeDataSpaceTest {

    private static final long HUGE = 29_614_122_732L;

    /**
     * A version-1 dataspace message as native HDF5 writes it for a fixed simple dataspace:
     * version(1), rank(1), flags(1), 5 reserved bytes, then each dimension as an 8-byte
     * little-endian length ({@code sizeOfLengths} = 8, the SuperblockV2V3 default).
     */
    private static ByteBuffer v1Message(boolean withMaxSizes, long... dims) {
        ByteBuffer bb = ByteBuffer.allocate(8 + dims.length * 8 * (withMaxSizes ? 2 : 1))
                .order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);                          // version
        bb.put((byte) dims.length);                // rank
        bb.put((byte) (withMaxSizes ? 1 : 0));     // flags: bit 0 = max sizes present
        bb.put(new byte[5]);                       // reserved
        for (long d : dims) bb.putLong(d);
        if (withMaxSizes) {
            for (long d : dims) bb.putLong(d);     // maxdims == dims (fixed dataspace)
        }
        bb.flip();
        return bb;
    }

    private static DataSpace parse(ByteBuffer bb) {
        return DataSpace.readDataSpace(bb, new Superblock.SuperblockV2V3());
    }

    @Test
    void hugeDimensionParses() {
        DataSpace ds = parse(v1Message(false, HUGE));
        assertEquals(HUGE, ds.getTotalLength(),
                "getTotalLength feeds Dataset.getSizeInBytes - it must carry the full long value");
        assertArrayEquals(new long[]{HUGE}, ds.getDimensionsAsLong());
        // maxSizes defaulted from the (long) dimensions, not truncated ints
        assertArrayEquals(new long[]{HUGE}, ds.getMaxSizes());
        // The int[] view cannot represent it; it must fail loudly, not truncate.
        assertThrows(ArithmeticException.class, ds::getDimensions);
    }

    @Test
    void hugeDimensionWithExplicitMaxSizesParses() {
        DataSpace ds = parse(v1Message(true, HUGE));
        assertEquals(HUGE, ds.getTotalLength());
        assertArrayEquals(new long[]{HUGE}, ds.getMaxSizes());
    }

    @Test
    void smallDimensionsBehaveAsBefore() {
        DataSpace ds = parse(v1Message(false, 1024, 3));
        assertEquals(3072, ds.getTotalLength());
        assertArrayEquals(new int[]{1024, 3}, ds.getDimensions());
        assertArrayEquals(new long[]{1024, 3}, ds.getDimensionsAsLong());
        assertArrayEquals(new long[]{1024, 3}, ds.getMaxSizes());
    }

    @Test
    void scalarDataspaceStillParses() {
        // rank 0: no dimension entries at all
        DataSpace ds = parse(v1Message(false));
        assertEquals(1, ds.getTotalLength(), "empty product = 1 (scalar), matching upstream");
        assertArrayEquals(new int[0], ds.getDimensions());
    }
}
