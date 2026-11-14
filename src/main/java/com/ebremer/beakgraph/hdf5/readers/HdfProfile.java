package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.falcon.hdf5.Attribute;
import com.ebremer.falcon.hdf5.Dataset;
import com.ebremer.falcon.hdf5.Group;
import com.ebremer.falcon.hdf5.HdfException;
import com.ebremer.falcon.hdf5.Hdf5Object;

/**
 * The readers' one door into the HDF5 container profile (SPECIFICATIONS.md
 * §3): groups, contiguous datasets and integer attributes, read through
 * Falcon (https://github.com/ebremer/falcon). Every reader used to cast the
 * HDF5 library's objects un-defensively, so a spec-conformant third-party
 * file with a chunked dataset or a 32-bit {@code numEntries} failed with an
 * opaque ClassCastException; integer attributes of any width are accepted
 * here and every violation names the dataset or attribute and the profile
 * rule (BG-281). The {@code readView(DatasetBytes.of(ds), numEntries, width)}
 * expression this replaces was inlined twelve times (BG-302).
 */
final class HdfProfile {

    private HdfProfile() {}

    /** The child group {@code name} of {@code parent}, or null when absent. */
    static HdfGroup group(HdfGroup parent, String name) {
        Hdf5Object n = parent.group().child(name).orElse(null);
        if (n == null) {
            return null;
        }
        if (!(n instanceof Group g)) {
            throw new IllegalStateException("'" + n.path() + "' must be a group (SPECIFICATIONS.md §7); found "
                    + kind(n));
        }
        return new HdfGroup(g, parent.channel());
    }

    /** The dataset {@code name} of {@code g} in contiguous layout, or null when absent. */
    static Dataset contiguous(HdfGroup g, String name) {
        Hdf5Object n = g.group().child(name).orElse(null);
        if (n == null) {
            return null;
        }
        if (!(n instanceof Dataset ds) || ds.layout() != Dataset.Layout.CONTIGUOUS) {
            throw new IllegalStateException("Dataset '" + n.path()
                    + "' must use contiguous storage layout (SPECIFICATIONS.md §3, item 2); found " + kind(n));
        }
        return ds;
    }

    static Dataset requireContiguous(HdfGroup g, String name) {
        Dataset ds = contiguous(g, name);
        if (ds == null) {
            throw new IllegalStateException("Missing dataset '" + name + "' in '" + g.path() + "' (SPECIFICATIONS.md §7)");
        }
        return ds;
    }

    /** The raw bytes of dataset {@code name}, or null when absent. */
    static RandomAccessBytes bytes(HdfGroup g, String name) {
        Dataset ds = contiguous(g, name);
        return (ds == null) ? null : DatasetBytes.of(ds, g.channel());
    }

    static RandomAccessBytes requireBytes(HdfGroup g, String name) {
        return DatasetBytes.of(requireContiguous(g, name), g.channel());
    }

    /** The bit-packed view of dataset {@code name} ({@code numEntries} x {@code width} bits), or null when absent. */
    static BitPackedUnSignedLongBuffer packed(HdfGroup g, String name) {
        Dataset ds = contiguous(g, name);
        if (ds == null) {
            return null;
        }
        return BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(ds, g.channel()), longAttr(ds, Params.NUM_ENTRIES), intAttr(ds, Params.WIDTH));
    }

    static BitPackedUnSignedLongBuffer requirePacked(HdfGroup g, String name) {
        BitPackedUnSignedLongBuffer b = packed(g, name);
        if (b == null) {
            throw new IllegalStateException("Missing dataset '" + name + "' in '" + g.path() + "' (SPECIFICATIONS.md §7)");
        }
        return b;
    }

    /** Integer attribute {@code name} of group {@code on}, whatever its stored width. */
    static long longAttr(HdfGroup on, String name) {
        return longAttr(on.group(), name);
    }

    /** Integer attribute {@code name} of {@code on}, whatever its stored width. */
    static long longAttr(Hdf5Object on, String name) {
        Attribute a;
        try {
            a = on.attribute(name).orElse(null);
        } catch (HdfException e) {
            throw new IllegalStateException("Attribute '" + name + "' on '" + on.path() + "' is unreadable", e);
        }
        if (a == null) {
            throw new IllegalStateException("Missing attribute '" + name + "' on '" + on.path() + "' (SPECIFICATIONS.md §3, item 4)");
        }
        try {
            return a.readLong();
        } catch (HdfException e) {
            // Not one element, not an integer type, or a value no long holds. The
            // profile rule is the error to report (and the root cause a caller
            // sees), with the library's reason in it.
            throw new IllegalStateException("Attribute '" + name + "' on '" + on.path()
                    + "' must be a scalar integer (SPECIFICATIONS.md §3, item 4); found " + a + ": " + e.getMessage());
        }
    }

    static int intAttr(Hdf5Object on, String name) {
        long v = longAttr(on, name);
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            throw new IllegalStateException("Attribute '" + name + "' on '" + on.path() + "' is out of int range: " + v);
        }
        return (int) v;
    }

    private static String kind(Hdf5Object n) {
        if (n instanceof Dataset ds) {
            return "a " + ds.layout().name().toLowerCase(java.util.Locale.ROOT) + " dataset";
        }
        return n.isGroup() ? "a group" : n.getClass().getSimpleName();
    }
}
