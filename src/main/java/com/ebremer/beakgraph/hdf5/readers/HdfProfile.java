package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import io.jhdf.api.Attribute;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.jhdf.api.dataset.ContiguousDataset;

/**
 * The readers' one door into the HDF5 container profile (SPECIFICATIONS.md
 * §3): groups, contiguous datasets and integer attributes. Every reader used
 * to cast the jHDF objects un-defensively - {@code (ContiguousDataset)
 * group.getChild(..)}, {@code (Long) attr.getData()} - so a spec-conformant
 * third-party file with a chunked dataset or a 32-bit {@code numEntries}
 * failed with an opaque ClassCastException; integer attributes of any width
 * are accepted here and every violation names the dataset or attribute and
 * the profile rule (BG-281). The {@code readView(DatasetBytes.of(ds),
 * numEntries, width)} expression this replaces was inlined twelve times
 * (BG-302).
 */
final class HdfProfile {

    private HdfProfile() {}

    /** The child group {@code name} of {@code parent}, or null when absent. */
    static Group group(Group parent, String name) {
        Node n = parent.getChild(name);
        if (n == null) {
            return null;
        }
        if (!(n instanceof Group g)) {
            throw new IllegalStateException("'" + n.getPath() + "' must be a group (SPECIFICATIONS.md §7); found "
                    + kind(n));
        }
        return g;
    }

    /** The dataset {@code name} of {@code g} in contiguous layout, or null when absent. */
    static ContiguousDataset contiguous(Group g, String name) {
        Node n = g.getChild(name);
        if (n == null) {
            return null;
        }
        if (!(n instanceof ContiguousDataset ds)) {
            throw new IllegalStateException("Dataset '" + n.getPath()
                    + "' must use contiguous storage layout (SPECIFICATIONS.md §3, item 2); found " + kind(n));
        }
        return ds;
    }

    static ContiguousDataset requireContiguous(Group g, String name) {
        ContiguousDataset ds = contiguous(g, name);
        if (ds == null) {
            throw new IllegalStateException("Missing dataset '" + name + "' in '" + g.getPath() + "' (SPECIFICATIONS.md §7)");
        }
        return ds;
    }

    /** The raw bytes of dataset {@code name}, or null when absent. */
    static RandomAccessBytes bytes(Group g, String name) {
        ContiguousDataset ds = contiguous(g, name);
        return (ds == null) ? null : DatasetBytes.of(ds);
    }

    /** The bit-packed view of dataset {@code name} ({@code numEntries} x {@code width} bits), or null when absent. */
    static BitPackedUnSignedLongBuffer packed(Group g, String name) {
        ContiguousDataset ds = contiguous(g, name);
        if (ds == null) {
            return null;
        }
        return BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(ds), longAttr(ds, Params.NUM_ENTRIES), intAttr(ds, Params.WIDTH));
    }

    static BitPackedUnSignedLongBuffer requirePacked(Group g, String name) {
        BitPackedUnSignedLongBuffer b = packed(g, name);
        if (b == null) {
            throw new IllegalStateException("Missing dataset '" + name + "' in '" + g.getPath() + "' (SPECIFICATIONS.md §7)");
        }
        return b;
    }

    /** Integer attribute {@code name} of {@code on}, whatever its stored width. */
    static long longAttr(Node on, String name) {
        Attribute a = on.getAttribute(name);
        if (a == null) {
            throw new IllegalStateException("Missing attribute '" + name + "' on '" + on.getPath() + "' (SPECIFICATIONS.md §3, item 4)");
        }
        Object value;
        try {
            value = a.getData();
        } catch (RuntimeException e) {
            throw new IllegalStateException("Attribute '" + name + "' on '" + on.getPath() + "' is unreadable", e);
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        throw new IllegalStateException("Attribute '" + name + "' on '" + on.getPath()
                + "' must be a scalar integer (SPECIFICATIONS.md §3, item 4); found "
                + (value == null ? "nothing" : value.getClass().getSimpleName() + " " + value));
    }

    static int intAttr(Node on, String name) {
        long v = longAttr(on, name);
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            throw new IllegalStateException("Attribute '" + name + "' on '" + on.getPath() + "' is out of int range: " + v);
        }
        return (int) v;
    }

    private static String kind(Node n) {
        return n.getClass().getSimpleName();
    }
}
