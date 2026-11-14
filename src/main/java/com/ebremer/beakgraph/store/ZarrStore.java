package com.ebremer.beakgraph.store;

import com.ebremer.beakgraph.io.ByteBufferBytes;
import com.ebremer.beakgraph.io.ChannelBytes;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.falcon.hdf5.RangeReader;
import com.ebremer.falcon.zarr.Zarr;
import com.ebremer.falcon.zarr.ZarrArray;
import com.ebremer.falcon.zarr.ZarrException;
import com.ebremer.falcon.zarr.ZarrGroup;
import com.ebremer.falcon.zarr.ZarrNode;
import com.ebremer.falcon.zarr.datatype.DataType;
import com.ebremer.falcon.zarr.json.JsonObject;
import com.ebremer.falcon.zarr.json.JsonValue;
import com.ebremer.falcon.zarr.store.FileSystemStore;
import com.ebremer.falcon.zarr.store.Store;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A store kept in a Zarr hierarchy (SPECIFICATIONS.md §3a), read through
 * Falcon (https://github.com/ebremer/falcon): groups, 1-D arrays of
 * uncompressed bytes, and integer attributes.
 *
 * <p>An array's chunks are stored as their bytes are - the {@code bytes}
 * codec alone - so a chunk's file IS that part of the array. A store opened
 * from a local directory memory-maps an array's chunk, and the array is a
 * view of that mapping: nothing is copied, decoded or read ahead, exactly as
 * with an HDF5 dataset. A reader maps every array of the {@code .BG} group
 * when it opens the store ({@link StoreGroup#load()}), so a store replaced on
 * disk while it is open goes on being read whole. Every mapping ends when
 * the store is closed. An array of one chunk - every array
 * up to 1 GiB, as BeakGraph writes them - is read through the same view an
 * HDF5 dataset is; one cut into several chunks (Falcon holds a chunk of
 * under 2 GiB) is read through its mappings a range at a time, which is
 * slower.
 *
 * <p>A store in any other Falcon {@code Store} - one that cannot map its
 * values: a ZIP archive, HTTP, S3, memory - is read a range at a time from
 * the chunk's key, with no cache of its own.
 *
 * @author Erich Bremer
 */
public final class ZarrStore implements OpenStore {

    private final Store store;
    private final ZarrGroup root;
    private final String source;
    /** Whether the store's values are files to map; else its arrays are read a range at a time. */
    private final boolean mappable;
    /** Owns every mapping made of the store's chunks; closed with the store. */
    private final Arena arena = Arena.ofShared();
    /** An array's bytes, by its path: mapped once however often it is asked for. */
    private final Map<String, RandomAccessBytes> views = new ConcurrentHashMap<>();

    private ZarrStore(Store store, ZarrGroup root, String source) {
        this.store = store;
        this.root = root;
        this.source = source;
        this.mappable = store instanceof FileSystemStore;
    }

    /** Opens the Zarr directory store at {@code dir}; its arrays are memory-mapped as they are asked for. */
    public static ZarrStore open(Path dir) {
        return open(FileSystemStore.openReadOnly(dir), dir.toString());
    }

    /**
     * Opens a Zarr hierarchy in any Falcon store. The store is not closed by
     * {@link #close()}: it is the caller's.
     *
     * @param source names the store in messages
     */
    public static ZarrStore open(Store store, String source) {
        ZarrGroup root;
        try {
            root = Zarr.openGroup(store);
        } catch (ZarrException | IllegalStateException | IllegalArgumentException e) {
            // No zarr.json, a malformed one, or an array where the root group belongs.
            throw new UncheckedIOException("Cannot open " + source + ": " + e.getMessage(), new IOException(e));
        }
        return new ZarrStore(store, root, source);
    }

    @Override
    public Container container() {
        return Container.ZARR;
    }

    @Override
    public StoreGroup root() {
        return new Group(root);
    }

    @Override
    public boolean isChannelBacked() {
        return !mappable;
    }

    /** How many arrays have a view so far: all of a group's once it is {@linkplain StoreGroup#load() loaded} (tests). */
    int viewCount() {
        return views.size();
    }

    /** Unmaps every chunk mapped of the store: its files are free to replace or delete from here on. */
    @Override
    public void close() {
        views.clear();
        try {
            arena.close();
        } catch (RuntimeException ignore) {
            // already closed, or a reader is inside a view at this instant: as with an HDF5 file, never fails
        }
    }

    private static String slashed(ZarrNode node) {
        return "/" + node.path();
    }

    private final class Group implements StoreGroup {

        private final ZarrGroup group;

        Group(ZarrGroup group) {
            this.group = group;
        }

        @Override
        public String name() {
            return group.name();
        }

        @Override
        public String path() {
            return slashed(group);
        }

        private ZarrNode child(String name) {
            try {
                return group.child(name).orElse(null);
            } catch (ZarrException | IllegalArgumentException e) {
                throw new IllegalStateException("'" + path(name) + "' in " + source + " is unreadable: " + e.getMessage(), e);
            }
        }

        private String path(String name) {
            return group.path().isEmpty() ? "/" + name : slashed(group) + "/" + name;
        }

        @Override
        public StoreGroup group(String name) {
            ZarrNode n = child(name);
            if (n == null) {
                return null;
            }
            if (!(n instanceof ZarrGroup g)) {
                throw new IllegalStateException("'" + slashed(n) + "' must be a group (SPECIFICATIONS.md §7); found an array");
            }
            return new Group(g);
        }

        @Override
        public StoreArray array(String name) {
            ZarrNode n = child(name);
            if (n == null) {
                return null;
            }
            if (!(n instanceof ZarrArray a)) {
                throw new IllegalStateException("'" + slashed(n) + "' must be an array (SPECIFICATIONS.md §7); found a group");
            }
            boolean bytes = a.dataType().equals(DataType.UINT8) || a.dataType().equals(DataType.INT8);
            boolean raw;
            try {
                raw = a.rank() == 1 && bytes && !a.isRectilinear() && a.hasRawChunks();
            } catch (ZarrException e) {
                throw new IllegalStateException("Array '" + slashed(a) + "' is unreadable: " + e.getMessage(), e);
            }
            if (!raw) {
                throw new IllegalStateException("Array '" + slashed(a) + "' must be a 1-D array of uint8 in a regular"
                        + " chunk grid, stored with the bytes codec alone (SPECIFICATIONS.md §3a, item 2); found "
                        + a.dataType().name() + " of shape " + Arrays.toString(a.shape()) + " with codecs " + a.codecNames());
            }
            return new Array(a);
        }

        @Override
        public void load() {
            java.util.List<ZarrNode> children;
            try {
                children = group.children();
            } catch (ZarrException | UnsupportedOperationException e) {
                return; // a store that cannot list its keys, and no consolidated metadata: arrays are found as they are asked for
            }
            for (ZarrNode child : children) {
                if (child instanceof ZarrGroup g) {
                    new Group(g).load();
                } else if (child instanceof ZarrArray a && conforms(a)) {
                    views.computeIfAbsent(a.path(), p -> view(a));
                }
            }
        }

        @Override
        public boolean hasAttribute(String name) {
            return group.attributes().has(name);
        }

        @Override
        public long attribute(String name) {
            return integer(group, name);
        }
    }

    /** Whether {@code a} is an array of the profile: 1-D bytes in a regular grid, stored with the bytes codec alone. */
    private static boolean conforms(ZarrArray a) {
        try {
            return a.rank() == 1 && (a.dataType().equals(DataType.UINT8) || a.dataType().equals(DataType.INT8))
                    && !a.isRectilinear() && a.hasRawChunks();
        } catch (ZarrException e) {
            return false;
        }
    }

    private final class Array implements StoreArray {

        private final ZarrArray array;

        Array(ZarrArray array) {
            this.array = array;
        }

        @Override
        public String path() {
            return slashed(array);
        }

        @Override
        public RandomAccessBytes bytes() {
            return views.computeIfAbsent(array.path(), p -> view(array));
        }

        @Override
        public boolean hasAttribute(String name) {
            return array.attributes().has(name);
        }

        @Override
        public long attribute(String name) {
            return integer(array, name);
        }
    }

    /** Integer attribute {@code name} of {@code node}: a JSON number without a fraction. */
    private static long integer(ZarrNode node, String name) {
        JsonObject attributes = node.attributes();
        JsonValue v = attributes.find(name).orElse(null);
        if (v == null) {
            throw new IllegalStateException("Missing attribute '" + name + "' on '" + slashed(node) + "' (SPECIFICATIONS.md §3, item 4)");
        }
        try {
            return v.asNumber().longValue();
        } catch (RuntimeException e) {
            // Not a number, or not one a long holds.
            throw new IllegalStateException("Attribute '" + name + "' on '" + slashed(node)
                    + "' must be a scalar integer (SPECIFICATIONS.md §3, item 4); found " + v.toJson() + ": " + e.getMessage());
        }
    }

    /** The bytes of {@code array}: its chunks mapped, or read a range at a time from a store that maps nothing. */
    private RandomAccessBytes view(ZarrArray array) {
        long size = array.shape()[0];
        if (size == 0) {
            return new ByteBufferBytes(ByteBuffer.allocate(0));
        }
        long chunk = array.chunkShape()[0];
        long count = array.gridShape()[0];
        if (count > Integer.MAX_VALUE - 8) {
            throw new IllegalStateException("Array '" + slashed(array) + "' is cut into " + count + " chunks of " + chunk
                    + " bytes: too many to read (SPECIFICATIONS.md §3a, item 3)");
        }
        String[] keys = new String[(int) count];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = array.chunkKey(i);
        }
        if (!mappable) {
            return new ChannelBytes(new Ranges(array, keys, chunk, size), 0, size);
        }
        MemorySegment[] segments = new MemorySegment[keys.length];
        for (int i = 0; i < segments.length; i++) {
            Optional<MemorySegment> mapped;
            try {
                mapped = array.chunkSegment(arena, i);
            } catch (ZarrException e) {
                throw new IllegalStateException("Array '" + slashed(array) + "' in " + source + ": " + e.getMessage(), e);
            }
            if (mapped.isPresent()) {
                segments[i] = mapped.get();
            } else {
                // A chunk that is not stored reads as the fill value (zeros): another writer may leave one out.
                requireAbsent(array, keys[i]);
                segments[i] = arena.allocate(chunk).asReadOnly();
            }
        }
        if (segments.length == 1) {
            // The one view an HDF5 dataset has too: the chunk is declared at least as long as the array.
            return DatasetBytes.of(segments[0].asSlice(0, size));
        }
        return new ChannelBytes(new Segments(segments, chunk, size), 0, size);
    }

    /** A chunk with nothing to map must be one that is not stored, in an array whose fill value is zero. */
    private void requireAbsent(ZarrArray array, String key) {
        boolean stored;
        try {
            stored = store.exists(key);
        } catch (ZarrException e) {
            throw new IllegalStateException("Array '" + slashed(array) + "' in " + source + ": " + e.getMessage(), e);
        }
        if (stored) {
            throw new IllegalStateException("Chunk '" + key + "' of array '" + slashed(array) + "' in " + source + " cannot be memory-mapped");
        }
        requireZeroFill(array, key);
    }

    private static void requireZeroFill(ZarrArray array, String key) {
        boolean zero;
        try {
            zero = array.fillValue().asNumber().longValue() == 0;
        } catch (RuntimeException e) {
            zero = false;
        }
        if (!zero) {
            throw new IllegalStateException("Chunk '" + key + "' of array '" + slashed(array) + "' is not stored, and the array's"
                    + " fill value is " + array.fillValue().toJson() + ", not 0 (SPECIFICATIONS.md §3a, item 4)");
        }
    }

    /** Positional reads of an array cut into several mapped chunks. */
    private record Segments(MemorySegment[] segments, long chunk, long size) implements RangeReader {

        @Override
        public void read(long position, ByteBuffer destination) throws IOException {
            while (destination.hasRemaining()) {
                if (position < 0 || position >= size) {
                    throw new EOFException("end of array at " + position);
                }
                int i = (int) (position / chunk);
                long at = position - i * chunk;
                int n = (int) Math.min(destination.remaining(), Math.min(chunk - at, size - position));
                if (destination.hasArray()) {
                    MemorySegment.copy(segments[i], ValueLayout.JAVA_BYTE, at, destination.array(),
                            destination.arrayOffset() + destination.position(), n);
                    destination.position(destination.position() + n);
                } else {
                    destination.put(segments[i].asSlice(at, n).asByteBuffer());
                }
                position += n;
            }
        }
    }

    /** Positional reads of an array in a store that maps nothing: a range of a chunk's key at a time. */
    private final class Ranges implements RangeReader {

        private final ZarrArray array;
        private final String[] keys;
        private final long chunk;
        private final long size;

        Ranges(ZarrArray array, String[] keys, long chunk, long size) {
            this.array = array;
            this.keys = keys;
            this.chunk = chunk;
            this.size = size;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public void read(long position, ByteBuffer destination) throws IOException {
            while (destination.hasRemaining()) {
                if (position < 0 || position >= size) {
                    throw new EOFException("end of array at " + position);
                }
                int i = (int) (position / chunk);
                long at = position - i * chunk;
                int n = (int) Math.min(destination.remaining(), Math.min(chunk - at, size - position));
                byte[] range;
                try {
                    range = store.getRange(keys[i], at, n).orElse(null);
                } catch (ZarrException e) {
                    throw new IOException("Cannot read chunk '" + keys[i] + "' of " + source + ": " + e.getMessage(), e);
                }
                if (range == null) {
                    requireZeroFill(array, keys[i]); // not stored: the fill value
                    range = new byte[n];
                } else if (range.length != n) {
                    throw new EOFException("chunk '" + keys[i] + "' of " + source + " ends at " + (at + range.length)
                            + ", short of the " + chunk + " bytes its array declares");
                }
                destination.put(range);
                position += n;
            }
        }
    }
}
