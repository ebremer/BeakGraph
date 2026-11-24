package com.ebremer.beakgraph.store;

import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.io.BlockCache;
import com.ebremer.beakgraph.io.ByteBufferBytes;
import com.ebremer.beakgraph.io.ChannelBytes;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.io.SegmentedBytes;
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
import com.ebremer.falcon.zarr.store.HttpStore;
import com.ebremer.falcon.zarr.store.Store;
import com.ebremer.falcon.zarr.store.ZipStore;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

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
 * under 2 GiB) through {@link SegmentedBytes}, which reads each value
 * straight from the chunk it lies in.
 *
 * <p>A store in ONE file - a ZIP archive whose entries are the store's keys,
 * none compressed ({@link #openZip}) - is read the same way: an entry is the
 * chunk's bytes, mapped where they lie in the archive.
 *
 * <p>A store that maps nothing is read a range at a time, a block at a time,
 * through ONE cache for all its arrays ({@code beakgraph.zarr.cache.block}
 * bytes a block, 128 KiB; {@code beakgraph.zarr.cache.blocks} blocks, 256:
 * 32 MiB a store that is open). Over HTTP ({@link #open(URI)}) each chunk is
 * read through an {@link HTTPSeekableByteChannel} - its retries, its
 * read-ahead, and the validator it pins, so that a chunk replaced on the
 * server fails loudly - and the channels share the cache and one client. A
 * chunk's channel is told its size, which the array declares, so nothing is
 * asked of the server about a chunk but the ranges that are read of it. In
 * any other Falcon {@code Store} (S3, memory) it is the store's own
 * {@code getRange} that is asked.
 *
 * @author Erich Bremer
 */
public final class ZarrStore implements OpenStore {

    /** The decoded-chunk cache of each typed-array view (RDFARRY.md §3.6): a few chunks or shards of rows. */
    static final long TYPED_VIEW_CACHE = 16L << 20;

    /** A store that maps nothing: the bytes fetched at a time, and the blocks kept of all its arrays together. */
    private static volatile int cacheBlock = Integer.getInteger("beakgraph.zarr.cache.block", 128 * 1024);
    private static volatile int cacheBlocks = Integer.getInteger("beakgraph.zarr.cache.blocks", 256);

    private final Store store;
    private final ZarrGroup root;
    private final String source;
    private final Container container;
    /** Whether the store's values are mapped; else its arrays are read a range at a time. */
    private final boolean mappable;
    /** What {@link #close()} also closes: an archive or a client opened here. Never a caller's store. */
    private final AutoCloseable owned;
    /** Where a store over HTTP lies, and the client its chunks are fetched through; else null. */
    private final Remote remote;
    /** The blocks read of a store that maps nothing; else null. */
    private final BlockCache cache;
    /** The channels opened on a remote store's chunks: closed with the store. */
    private final List<HTTPSeekableByteChannel> channels = new ArrayList<>();
    /** Owns every mapping made of the store's chunks; closed with the store. */
    private final Arena arena = Arena.ofShared();
    /** An array's bytes, by its path: mapped once however often it is asked for. */
    private final Map<String, RandomAccessBytes> views = new ConcurrentHashMap<>();

    private ZarrStore(Store store, ZarrGroup root, String source, Container container, AutoCloseable owned, Remote remote) {
        this.store = store;
        this.root = root;
        this.source = source;
        this.container = container;
        this.owned = owned;
        this.remote = remote;
        this.mappable = store instanceof FileSystemStore || store instanceof ZipStore;
        this.cache = mappable ? null : new BlockCache(Math.max(1, cacheBlock), Math.max(2, cacheBlocks));
    }

    /** Overrides the cache of the stores opened from here on (tests); returns the previous block size and block count. */
    static int[] setCache(int block, int blocks) {
        int[] previous = {cacheBlock, cacheBlocks};
        cacheBlock = block;
        cacheBlocks = blocks;
        return previous;
    }

    /** Opens the Zarr directory store at {@code dir}; its arrays are memory-mapped as they are asked for. */
    public static ZarrStore open(Path dir) {
        return open(FileSystemStore.openReadOnly(dir), dir.toString(), Container.ZARR, null, null);
    }

    /**
     * Opens the Zarr store kept in the ZIP archive at {@code archive}
     * (SPECIFICATIONS.md §3a, item 8): each array is mapped where its chunk
     * lies in the file. The archive is held open until {@link #close()}.
     */
    public static ZarrStore openZip(Path archive) {
        ZipStore zip;
        try {
            zip = ZipStore.openReadOnly(archive);
        } catch (ZarrException | IllegalArgumentException e) {
            // No such file, or not a ZIP archive Falcon reads.
            throw new UncheckedIOException("Cannot open " + archive + ": " + e.getMessage(), new IOException(e));
        }
        try {
            return open(zip, archive.toString(), Container.ZARR_ZIP, zip, null);
        } catch (RuntimeException | Error e) {
            zip.close();
            throw e;
        }
    }

    /**
     * Opens the Zarr store at an {@code http} or {@code https} URL: the
     * document that describes it is read now, and each array a block at a
     * time as it is read, through the store's cache. A query in the URL (a
     * presigned or SAS URL) is sent with every request, and left out of
     * every message.
     *
     * @throws IllegalArgumentException if {@code uri} is not an http(s) URL
     */
    public static ZarrStore open(URI uri) {
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            throw new IllegalArgumentException("Not an http(s) URI: " + uri);
        }
        String display = HTTPSeekableByteChannel.redact(uri);
        HttpClient client = HTTPSeekableByteChannel.newClient();
        try {
            return open(HttpStore.openReadOnly(uri.toString()), display, Container.ZARR, client, new Remote(uri, client));
        } catch (RuntimeException | Error e) {
            client.close();
            throw e;
        }
    }

    /**
     * Opens a Zarr hierarchy in any Falcon store. The store is not closed by
     * {@link #close()}: it is the caller's.
     *
     * @param source names the store in messages
     */
    public static ZarrStore open(Store store, String source) {
        return open(store, source, Container.ZARR, null, null);
    }

    private static ZarrStore open(Store store, String source, Container container, AutoCloseable owned, Remote remote) {
        ZarrGroup root;
        try {
            root = Zarr.openGroup(store);
        } catch (ZarrException | IllegalStateException | IllegalArgumentException e) {
            // No zarr.json, a malformed one, or an array where the root group belongs.
            throw new UncheckedIOException("Cannot open " + source + ": " + e.getMessage(), new IOException(e));
        }
        return new ZarrStore(store, root, source, container, owned, remote);
    }

    @Override
    public Container container() {
        return container;
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

    /** How many blocks of a store that maps nothing are held now (tests); 0 for a mapped store. */
    int cachedBlocks() {
        return cache == null ? 0 : cache.size();
    }

    /**
     * Unmaps every chunk mapped of the store, and closes what was opened to
     * read it - an archive, the channels and the client of a store over
     * HTTP: its files are free to replace or delete from here on.
     */
    @Override
    public void close() {
        views.clear();
        try {
            arena.close();
        } catch (RuntimeException ignore) {
            // already closed, or a reader is inside a view at this instant: as with an HDF5 file, never fails
        }
        List<HTTPSeekableByteChannel> open;
        synchronized (channels) {
            open = new ArrayList<>(channels);
            channels.clear();
        }
        for (HTTPSeekableByteChannel channel : open) {
            channel.close();
        }
        if (owned != null) {
            try {
                owned.close();
            } catch (Exception ignore) {
                // nothing more to release
            }
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

        @Override
        public String stringAttribute(String name) {
            JsonValue v = group.attributes().find(name).orElse(null);
            if (v == null) {
                return null;
            }
            try {
                return v.asString();
            } catch (RuntimeException e) {
                throw new IllegalStateException("Attribute '" + name + "' on '" + slashed(group) + "' must be a string", e);
            }
        }

        @Override
        public java.util.List<String> groupNames() {
            java.util.List<String> names = new java.util.ArrayList<>();
            try {
                for (ZarrNode child : group.children()) {
                    if (child instanceof ZarrGroup) {
                        names.add(child.name());
                    }
                }
            } catch (ZarrException e) {
                throw new IllegalStateException("'" + slashed(group) + "' in " + source + " cannot be listed: " + e.getMessage(), e);
            }
            java.util.Collections.sort(names);
            return names;
        }

        @Override
        public TypedArrayView typedArrayView(String name) {
            ZarrNode n = child(name);
            if (n == null) {
                return null;
            }
            if (!(n instanceof ZarrArray plain)) {
                throw new IllegalStateException("'" + slashed(n) + "' must be a typed array; found a group");
            }
            DataType t = plain.dataType();
            TypedArray.Type type;
            if (t.equals(DataType.FLOAT64)) {
                type = TypedArray.Type.F64;
            } else if (t.equals(DataType.FLOAT32)) {
                type = TypedArray.Type.F32;
            } else if (t.equals(DataType.INT64)) {
                type = TypedArray.Type.I64;
            } else if (t.equals(DataType.INT32)) {
                type = TypedArray.Type.I32;
            } else if (t.equals(DataType.UINT16)) {
                type = TypedArray.Type.U16;
            } else if (t.equals(DataType.STRING)) {
                type = TypedArray.Type.STR;
            } else if (t.equals(DataType.BOOL)) {
                type = TypedArray.Type.BOOL;
            } else {
                throw new IllegalStateException("'" + slashed(plain) + "' has data type " + t.name()
                        + ", which no typed array of /.BG/arrays holds");
            }
            // A handle of its own with a decoded-chunk cache: the rows of one
            // sequence after another come out of the same chunk or shard.
            ZarrArray a = plain.withChunkCache(TYPED_VIEW_CACHE);
            long[] shape = a.shape();
            TypedArrayView direct = new TypedArrayView() {
                @Override
                public TypedArray.Type type() {
                    return type;
                }

                @Override
                public long[] shape() {
                    return shape.clone();
                }

                @Override
                public Object readRows(long first, long count) {
                    long[][] box = TypedArrayView.box(shape, first, count);
                    try {
                        com.ebremer.falcon.zarr.Selection s = a.select(box[0], box[1]);
                        return switch (type) {
                            case F64 -> s.readDoubles();
                            case F32 -> s.readFloats();
                            case I64 -> s.readLongs();
                            case I32, U16 -> s.readInts();
                            case STR -> s.readStrings();
                            case BOOL -> {
                                byte[] raw = s.readRawBytes();
                                boolean[] b = new boolean[raw.length];
                                for (int i = 0; i < raw.length; i++) {
                                    b[i] = raw[i] != 0;
                                }
                                yield b;
                            }
                        };
                    } catch (ZarrException e) {
                        throw new IllegalStateException("Array '" + slashed(a) + "' is unreadable: " + e.getMessage(), e);
                    }
                }
            };
            // Falcon caches whole chunks only, not the sub-chunks of a shard: a row
            // read out of a compressed shard would decode its sub-chunk every time.
            // Such an array is read a sub-chunk of rows at a time, the last few kept.
            return a.hasRawChunks() ? direct : new BlockCachedView(direct, Math.max(1, a.innerChunkShape()[0]));
        }

        @Override
        public TypedArray typedArray(String name) {
            ZarrNode n = child(name);
            if (n == null) {
                return null;
            }
            if (!(n instanceof ZarrArray a)) {
                throw new IllegalStateException("'" + slashed(n) + "' must be a typed array; found a group");
            }
            DataType t = a.dataType();
            TypedArray.Type type;
            Object values;
            try {
                if (t.equals(DataType.FLOAT64)) {
                    type = TypedArray.Type.F64;
                    values = a.readDoubles();
                } else if (t.equals(DataType.FLOAT32)) {
                    type = TypedArray.Type.F32;
                    values = a.readFloats();
                } else if (t.equals(DataType.INT64)) {
                    type = TypedArray.Type.I64;
                    values = a.readLongs();
                } else if (t.equals(DataType.INT32)) {
                    type = TypedArray.Type.I32;
                    values = a.readInts();
                } else if (t.equals(DataType.UINT16)) {
                    type = TypedArray.Type.U16;
                    values = a.readInts();
                } else if (t.equals(DataType.STRING)) {
                    type = TypedArray.Type.STR;
                    values = a.readStrings();
                } else if (t.equals(DataType.BOOL)) {
                    type = TypedArray.Type.BOOL;
                    byte[] raw = a.readRawBytes();
                    boolean[] b = new boolean[raw.length];
                    for (int i = 0; i < raw.length; i++) {
                        b[i] = raw[i] != 0;
                    }
                    values = b;
                } else {
                    throw new IllegalStateException("'" + slashed(a) + "' has data type " + t.name()
                            + ", which no typed array of /.BG/arrays holds");
                }
            } catch (ZarrException e) {
                throw new IllegalStateException("Array '" + slashed(a) + "' is unreadable: " + e.getMessage(), e);
            }
            java.util.Map<String, Object> attributes = new java.util.TreeMap<>();
            for (java.util.Map.Entry<String, JsonValue> e : a.attributes().members().entrySet()) {
                JsonValue v = e.getValue();
                attributes.put(e.getKey(), (v instanceof com.ebremer.falcon.zarr.json.JsonString) ? v.asString() : (Object) v.asNumber().longValue());
            }
            java.util.List<String> dims = a.dimensionNames().orElse(null);
            return new TypedArray(type, a.shape(), values, dims, attributes);
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
            return new ChannelBytes(remote != null ? new HttpRanges(array, keys, chunk, size)
                    : new StoreRanges(array, keys, chunk, size), 0, size);
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
        return DatasetBytes.of(segments, chunk, size);
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
            throw new IllegalStateException("Chunk '" + key + "' of array '" + slashed(array) + "' in " + source
                    + " cannot be memory-mapped" + (container == Container.ZARR_ZIP
                            ? ": its entry is compressed or encrypted, and a store in a ZIP archive keeps every entry"
                            + " as it is (SPECIFICATIONS.md §3a, item 8)" : ""));
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

    /** Where a store over HTTP lies, and what its chunks are fetched through. */
    private record Remote(URI base, HttpClient client) {

        /** The URL of {@code key}: the base's path and the key's, each segment percent-encoded, and the base's query as it came. */
        URI uri(String key) {
            String path = base.getRawPath() == null ? "" : base.getRawPath();
            while (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            StringBuilder url = new StringBuilder(base.getScheme()).append("://").append(base.getRawAuthority()).append(path);
            for (String segment : key.split("/")) {
                url.append('/');
                for (byte b : segment.getBytes(StandardCharsets.UTF_8)) {
                    int c = b & 0xFF;
                    if (c < 0x80 && (Character.isLetterOrDigit(c) || c == '-' || c == '.' || c == '_' || c == '~')) {
                        url.append((char) c);
                    } else {
                        url.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16)))
                                .append(Character.toUpperCase(Character.forDigit(c & 0xF, 16)));
                    }
                }
            }
            if (base.getRawQuery() != null) {
                url.append('?').append(base.getRawQuery());
            }
            return URI.create(url.toString());
        }
    }

    /** Reads {@code n} bytes at {@code at} through {@code reader} into {@code destination}, which may have room for more. */
    private static void readInto(RangeReader reader, long at, ByteBuffer destination, int n) throws IOException {
        int limit = destination.limit();
        destination.limit(destination.position() + n);
        try {
            reader.read(at, destination);
        } finally {
            destination.limit(limit);
        }
    }

    /** Positional reads of a chunk that is not stored: the fill value, zero. */
    private record Zeros(long size) implements RangeReader {

        @Override
        public void read(long position, ByteBuffer destination) {
            while (destination.hasRemaining()) {
                destination.put((byte) 0);
            }
        }
    }

    /**
     * Positional reads of an array in a store over HTTP: each chunk through
     * a range channel of its own, made when the chunk is first read. The
     * channels' blocks are in the store's one cache.
     */
    private final class HttpRanges implements RangeReader {

        private final ZarrArray array;
        private final String[] keys;
        private final long chunk;
        private final long size;
        /** By chunk; null until the chunk is read. */
        private final AtomicReferenceArray<RangeReader> readers;
        /** By chunk: whether a range of it was ever answered. One that was is not absent later. */
        private final boolean[] answered;

        HttpRanges(ZarrArray array, String[] keys, long chunk, long size) {
            this.array = array;
            this.keys = keys;
            this.chunk = chunk;
            this.size = size;
            this.readers = new AtomicReferenceArray<>(keys.length);
            this.answered = new boolean[keys.length];
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
                int mark = destination.position();
                try {
                    readInto(reader(i), at, destination, n);
                    if (!answered[i]) {
                        answered[i] = true;
                    }
                } catch (FileNotFoundException absent) {
                    destination.position(mark);
                    readInto(zeros(i, absent), at, destination, n);
                }
                position += n;
            }
        }

        private RangeReader reader(int i) {
            RangeReader reader = readers.get(i);
            if (reader == null) {
                // Told its size - the chunk's, which the array declares - so nothing is asked of the server yet.
                HTTPSeekableByteChannel channel = HTTPSeekableByteChannel.sized(remote.uri(keys[i]), chunk, cache, remote.client());
                reader = RangeReader.of(channel);
                if (readers.compareAndSet(i, null, reader)) {
                    synchronized (channels) {
                        channels.add(channel);
                    }
                } else {
                    channel.close(); // another thread's is the chunk's
                    reader = readers.get(i);
                }
            }
            return reader;
        }

        /** The server has no such chunk: it reads as the fill value, zero, if nothing of it was ever read. */
        private RangeReader zeros(int i, FileNotFoundException absent) throws IOException {
            if (answered[i]) {
                throw new IOException("Chunk '" + keys[i] + "' of " + source + " was read, and is gone: " + absent.getMessage(), absent);
            }
            try {
                requireZeroFill(array, keys[i]);
            } catch (IllegalStateException notZero) {
                throw new IOException(notZero.getMessage(), notZero);
            }
            RangeReader zeros = new Zeros(chunk);
            readers.set(i, zeros);
            return zeros;
        }
    }

    /**
     * Positional reads of an array in a store that maps nothing and is not
     * read over HTTP here (S3, memory, a caller's own): block-aligned ranges
     * of a chunk's key, kept in the store's one cache. A miss right after the
     * block before it is a scan: the next few blocks then come in one range.
     */
    private final class StoreRanges implements RangeReader {

        private final ZarrArray array;
        private final String[] keys;
        private final long chunk;
        private final long size;
        /** By chunk; null until the chunk is read. Guarded by this, as is {@link #lastMiss}. */
        private final BlockCache.Part[] parts;
        private final long[] lastMiss;

        StoreRanges(ZarrArray array, String[] keys, long chunk, long size) {
            this.array = array;
            this.keys = keys;
            this.chunk = chunk;
            this.size = size;
            this.parts = new BlockCache.Part[keys.length];
            this.lastMiss = new long[keys.length];
            Arrays.fill(lastMiss, -1);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public void read(long position, ByteBuffer destination) throws IOException {
            int blockSize = cache.blockSize();
            while (destination.hasRemaining()) {
                if (position < 0 || position >= size) {
                    throw new EOFException("end of array at " + position);
                }
                int i = (int) (position / chunk);
                long at = position - i * chunk;
                long index = at / blockSize;
                int within = (int) (at - index * blockSize);
                byte[] block = block(i, index);
                int n = (int) Math.min(Math.min(destination.remaining(), block.length - within), size - position);
                destination.put(block, within, n);
                position += n;
            }
        }

        /** Block {@code index} of chunk {@code i}: from the cache, or fetched - one fetch at a time an array. */
        private synchronized byte[] block(int i, long index) throws IOException {
            BlockCache.Part part = parts[i];
            if (part == null) {
                part = cache.part();
                parts[i] = part;
            }
            byte[] block = part.get(index);
            if (block != null) {
                return block;
            }
            int blockSize = cache.blockSize();
            boolean scan = lastMiss[i] >= 0 && index == lastMiss[i] + 1;
            long blocks = (chunk + blockSize - 1) / blockSize;
            int ahead = Math.min(HTTPSeekableByteChannel.DEFAULT_READ_AHEAD_BLOCKS, cache.maxBlocks() / 2);
            int count = scan ? (int) Math.max(1, Math.min(ahead, blocks - index)) : 1;
            long start = index * blockSize;
            byte[] fetched = fetch(i, start, (int) Math.min(chunk - start, (long) count * blockSize));
            byte[] asked = null;
            for (int k = 0; k * (long) blockSize < fetched.length; k++) {
                int from = k * blockSize;
                byte[] one = (fetched.length <= blockSize) ? fetched
                        : Arrays.copyOfRange(fetched, from, Math.min(fetched.length, from + blockSize));
                part.put(index + k, one);
                if (k == 0) {
                    asked = one;
                }
            }
            lastMiss[i] = index + count - 1;
            return asked;
        }

        private byte[] fetch(int i, long start, int length) throws IOException {
            byte[] range;
            try {
                range = store.getRange(keys[i], start, length).orElse(null);
            } catch (ZarrException e) {
                throw new IOException("Cannot read chunk '" + keys[i] + "' of " + source + ": " + e.getMessage(), e);
            }
            if (range == null) {
                try {
                    requireZeroFill(array, keys[i]); // not stored: the fill value
                } catch (IllegalStateException notZero) {
                    throw new IOException(notZero.getMessage(), notZero);
                }
                return new byte[length];
            }
            if (range.length != length) {
                throw new EOFException("chunk '" + keys[i] + "' of " + source + " ends at " + (start + range.length)
                        + ", short of the " + chunk + " bytes its array declares");
            }
            return range;
        }
    }
}
