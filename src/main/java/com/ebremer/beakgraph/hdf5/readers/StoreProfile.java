package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.store.StoreArray;
import com.ebremer.beakgraph.store.StoreGroup;

/**
 * How the readers take a store's parts out of its groups (SPECIFICATIONS.md
 * §3): child groups, byte arrays, the bit-packed and block-structured views
 * of them, and integer attributes - whatever container the store is kept in
 * ({@link StoreGroup} is an HDF5 group or a Zarr group). A part that is
 * absent is null; what is there but wrong is an
 * {@link IllegalStateException} that names it and the rule, from the
 * container's implementation (BG-281). The
 * {@code readView(DatasetBytes.of(ds), numEntries, width)} expression this
 * replaces was inlined twelve times (BG-302).
 */
final class StoreProfile {

    private StoreProfile() {}

    /** The child group {@code name} of {@code parent}, or null when absent. */
    static StoreGroup group(StoreGroup parent, String name) {
        return parent.group(name);
    }

    /** The byte array {@code name} of {@code g}, or null when absent. */
    static StoreArray array(StoreGroup g, String name) {
        return g.array(name);
    }

    static StoreArray requireArray(StoreGroup g, String name) {
        StoreArray a = g.array(name);
        if (a == null) {
            throw new IllegalStateException("Missing dataset '" + name + "' in '" + g.path() + "' (SPECIFICATIONS.md §7)");
        }
        return a;
    }

    /** The raw bytes of array {@code name}, or null when absent. */
    static RandomAccessBytes bytes(StoreGroup g, String name) {
        StoreArray a = g.array(name);
        return (a == null) ? null : a.bytes();
    }

    static RandomAccessBytes requireBytes(StoreGroup g, String name) {
        return requireArray(g, name).bytes();
    }

    /** The bit-packed view of array {@code name} ({@code numEntries} x {@code width} bits), or null when absent. */
    static BitPackedUnSignedLongBuffer packed(StoreGroup g, String name) {
        StoreArray a = g.array(name);
        if (a == null) {
            return null;
        }
        RandomAccessBytes data = a.bytes();
        long numEntries = longAttr(a, Params.NUM_ENTRIES);
        int width = intAttr(a, Params.WIDTH);
        if (a.hasAttribute(Params.BLOCK_ENTRIES)) {
            // Block-structured (SPECIFICATIONS.md §5.1a): its directory is the array beside it.
            RandomAccessBytes directory = bytes(g, name + Params.BLOCKS_SUFFIX);
            if (directory == null) {
                throw new IllegalStateException("Missing dataset '" + name + Params.BLOCKS_SUFFIX + "' in '" + g.path()
                        + "': the directory of the block-structured '" + name + "' (SPECIFICATIONS.md §5.1a)");
            }
            return BitPackedUnSignedLongBuffer.readBlocked(data, directory, numEntries, intAttr(a, Params.BLOCK_ENTRIES), width);
        }
        return BitPackedUnSignedLongBuffer.readView(data, numEntries, width);
    }

    static BitPackedUnSignedLongBuffer requirePacked(StoreGroup g, String name) {
        BitPackedUnSignedLongBuffer b = packed(g, name);
        if (b == null) {
            throw new IllegalStateException("Missing dataset '" + name + "' in '" + g.path() + "' (SPECIFICATIONS.md §7)");
        }
        return b;
    }

    /** Integer attribute {@code name} of group {@code on}, whatever its stored width. */
    static long longAttr(StoreGroup on, String name) {
        return on.attribute(name);
    }

    /** Integer attribute {@code name} of array {@code on}, whatever its stored width. */
    static long longAttr(StoreArray on, String name) {
        return on.attribute(name);
    }

    static int intAttr(StoreArray on, String name) {
        long v = on.attribute(name);
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            throw new IllegalStateException("Attribute '" + name + "' on '" + on.path() + "' is out of int range: " + v);
        }
        return (int) v;
    }
}
