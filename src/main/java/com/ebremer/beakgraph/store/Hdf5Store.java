package com.ebremer.beakgraph.store;

import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.falcon.hdf5.Attribute;
import com.ebremer.falcon.hdf5.Dataset;
import com.ebremer.falcon.hdf5.ExternalFileAccess;
import com.ebremer.falcon.hdf5.Group;
import com.ebremer.falcon.hdf5.HdfException;
import com.ebremer.falcon.hdf5.Hdf5File;
import com.ebremer.falcon.hdf5.Hdf5Object;
import com.ebremer.falcon.hdf5.OpenOptions;
import com.ebremer.falcon.hdf5.RangeReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Locale;

/**
 * A store kept in one HDF5 file (SPECIFICATIONS.md §3), read through
 * Falcon (https://github.com/ebremer/falcon): groups, contiguous datasets and
 * integer attributes.
 *
 * <p>A store opened from a local file is memory-mapped whole, once, and each
 * dataset is a view of that mapping. A store read through a caller-supplied
 * channel (an HTTP range channel, typically) cannot be mapped: its datasets
 * are read a range at a time, through the same reader the HDF5 metadata came
 * through, so reads of either are serialized together.
 *
 * <p>Every reader used to cast the HDF5 library's objects un-defensively, so
 * a spec-conformant third-party file with a chunked dataset or a 32-bit
 * {@code numEntries} failed with an opaque ClassCastException; integer
 * attributes of any width are accepted here and every violation names the
 * dataset or attribute and the profile rule (BG-281).
 *
 * @author Erich Bremer
 */
public final class Hdf5Store implements OpenStore {

    /**
     * A store is ONE file (SPECIFICATIONS.md §3): the other files an HDF5
     * file can name (external links, external raw data) are never opened, so
     * a store cannot be made to read its neighbours.
     */
    private static final OpenOptions OPTIONS = OpenOptions.defaults().externalFileAccess(ExternalFileAccess.none());

    private final Hdf5File hdf;
    /** What a channel-backed store is read through, else null. */
    private final RangeReader reader;
    /** The channel a channel-backed store is read through (owned: closed by close()), else null. */
    private final SeekableByteChannel channel;

    private Hdf5Store(Hdf5File hdf, RangeReader reader, SeekableByteChannel channel) {
        this.hdf = hdf;
        this.reader = reader;
        this.channel = channel;
    }

    /** Memory-maps the HDF5 file at {@code src}. */
    public static Hdf5Store open(Path src) {
        try {
            return new Hdf5Store(Hdf5File.open(src, OPTIONS), null, null);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open " + src + ": " + e.getMessage(), e);
        }
    }

    /**
     * Reads an HDF5 file through {@code channel}, which the store then owns:
     * it is closed by {@link #close()}, and also released if the open fails.
     */
    public static Hdf5Store open(SeekableByteChannel channel, URI source) {
        // One reader for the metadata and for the datasets: reads of either are serialized together.
        RangeReader reader = RangeReader.of(channel);
        try {
            return new Hdf5Store(Hdf5File.open(reader, OPTIONS), reader, channel);
        } catch (IOException e) {
            try { channel.close(); } catch (Exception ignore) {}
            throw new UncheckedIOException("Cannot open " + source + ": " + e.getMessage(), e);
        } catch (RuntimeException | Error e) {
            // Falcon never closes what it reads through, and a failed open
            // (not an HDF5 file, say) leaves the channel open; the store
            // owns the channel, so release it here.
            try { channel.close(); } catch (Exception ignore) {}
            throw e;
        }
    }

    /**
     * A group of an HDF5 file the caller opened (and closes) itself, memory-mapped:
     * for code that reads one part of a store, and for the tests.
     */
    public static StoreGroup group(Group group) {
        return new Hdf5Group(group, null);
    }

    @Override
    public Container container() {
        return Container.HDF5;
    }

    @Override
    public StoreGroup root() {
        return new Hdf5Group(hdf.root(), reader);
    }

    @Override
    public boolean isChannelBacked() {
        return channel != null;
    }

    /** Unmaps the file (or drops what was cached of a channel-backed one), then closes the channel it was read through. */
    @Override
    public void close() {
        try { hdf.close(); } catch (Exception ignore) {}
        if (channel != null) {
            try { channel.close(); } catch (Exception ignore) {}
        }
    }

    private record Hdf5Group(Group group, RangeReader reader) implements StoreGroup {

        @Override
        public String name() {
            return group.name();
        }

        @Override
        public String path() {
            return group.path();
        }

        @Override
        public StoreGroup group(String name) {
            Hdf5Object n = group.child(name).orElse(null);
            if (n == null) {
                return null;
            }
            if (!(n instanceof Group g)) {
                throw new IllegalStateException("'" + n.path() + "' must be a group (SPECIFICATIONS.md §7); found "
                        + kind(n));
            }
            return new Hdf5Group(g, reader);
        }

        @Override
        public StoreArray array(String name) {
            Hdf5Object n = group.child(name).orElse(null);
            if (n == null) {
                return null;
            }
            if (!(n instanceof Dataset ds) || ds.layout() != Dataset.Layout.CONTIGUOUS) {
                throw new IllegalStateException("Dataset '" + n.path()
                        + "' must use contiguous storage layout (SPECIFICATIONS.md §3, item 2); found " + kind(n));
            }
            return new Hdf5Array(ds, reader);
        }

        @Override
        public boolean hasAttribute(String name) {
            return has(group, name);
        }

        @Override
        public long attribute(String name) {
            return integer(group, name);
        }
    }

    private record Hdf5Array(Dataset dataset, RangeReader reader) implements StoreArray {

        @Override
        public String path() {
            return dataset.path();
        }

        @Override
        public RandomAccessBytes bytes() {
            return DatasetBytes.of(dataset, reader);
        }

        @Override
        public boolean hasAttribute(String name) {
            return has(dataset, name);
        }

        @Override
        public long attribute(String name) {
            return integer(dataset, name);
        }
    }

    private static boolean has(Hdf5Object on, String name) {
        try {
            return on.attribute(name).isPresent();
        } catch (HdfException e) {
            throw new IllegalStateException("Attribute '" + name + "' on '" + on.path() + "' is unreadable", e);
        }
    }

    /** Integer attribute {@code name} of {@code on}, whatever its stored width. */
    private static long integer(Hdf5Object on, String name) {
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

    private static String kind(Hdf5Object n) {
        if (n instanceof Dataset ds) {
            return "a " + ds.layout().name().toLowerCase(Locale.ROOT) + " dataset";
        }
        return n.isGroup() ? "a group" : n.getClass().getSimpleName();
    }
}
