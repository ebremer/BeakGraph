package com.ebremer.beakgraph.store;

import com.ebremer.beakgraph.hdf5.WritableDataset;
import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.falcon.zarr.ArraySpec;
import com.ebremer.falcon.zarr.Zarr;
import com.ebremer.falcon.zarr.ZarrArray;
import com.ebremer.falcon.zarr.ZarrGroup;
import com.ebremer.falcon.zarr.ZarrNode;
import com.ebremer.falcon.zarr.datatype.DataType;
import com.ebremer.falcon.zarr.json.JsonObject;
import com.ebremer.falcon.zarr.store.FileSystemStore;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A store being written into a Zarr v3 directory store, in BeakGraph's Zarr
 * container profile (SPECIFICATIONS.md §3a): a Zarr group per group, a 1-D
 * {@code uint8} array per byte array, stored with the {@code bytes} codec
 * alone, and integer attributes as JSON numbers. The Zarr implementation of
 * {@link WritableStore}, on Falcon (https://github.com/ebremer/falcon).
 *
 * <p>An array of up to 1 GiB is ONE chunk, whose file holds exactly the
 * array's bytes: that file is what a reader maps. A larger array is cut into
 * the fewest equal chunks of at most 1 GiB (Falcon, like most Zarr readers,
 * holds a chunk in one buffer of under 2 GiB), the last one padded with
 * zeros to the chunk's length, as Zarr stores every chunk whole. The node
 * metadata goes through Falcon; a chunk's bytes are written straight to its
 * file a piece at a time, so an array never has to fit the heap.
 *
 * <p>The store is written in a directory beside {@code path}. {@link #commit()}
 * writes each node's attributes, consolidates the metadata of the whole
 * hierarchy into the root's {@code zarr.json} (a reader then opens the store
 * from that one document), forces every file to disk, and moves the directory
 * to {@code path}; a store with an array short of its length is not moved
 * there at all. {@link #close()} without a commit removes the directory.
 *
 * <p>The same store in ONE file ({@link #createZip}, SPECIFICATIONS.md §3a,
 * item 8) is written the same way, and then packed: {@link #commit()} copies
 * each key's file into a ZIP archive beside {@code path} as an entry that is
 * not compressed, removing the file as it goes, forces the archive to disk
 * and moves it to {@code path}. An entry's CRC-32 is taken as the chunk is
 * written, and the JDK's archive writer checks it against what it copied.
 * The archive is the same bytes for the same store: its entries are in key
 * order after the root's {@code zarr.json}, and carry one fixed time.
 *
 * <p>Not thread-safe: a store is assembled from one thread.
 *
 * @author Erich Bremer
 */
public final class ZarrStoreWriter implements WritableStore {

    private static final Logger logger = LoggerFactory.getLogger(ZarrStoreWriter.class);

    /**
     * An array of up to this many bytes is one chunk; a larger one is cut into
     * equal chunks of at most this. 1 GiB, unless the
     * {@code beakgraph.zarr.chunk.max} system property (bytes) says otherwise:
     * for testing the several-chunk form without writing gigabytes. Falcon
     * refuses a chunk of 2 GiB or more.
     */
    private static volatile long chunkMax = Long.getLong("beakgraph.zarr.chunk.max", 1L << 30);

    /**
     * A typed array's raw chunk is at most this (16 MiB) or {@link #chunkMax}:
     * a streaming writer holds one chunk of rows, and a disk engine writes
     * arrays larger than its memory.
     */
    static final long TYPED_CHUNK_MAX = 1L << 24;

    /** The time every entry of an archive carries: the first a ZIP entry can. */
    private static final LocalDateTime ARCHIVE_TIME = LocalDateTime.of(1980, 1, 1, 0, 0);

    private final Path path;
    /** The directory being written, beside {@link #path}. */
    private final Path work;
    /** Whether the store is to be one ZIP archive at {@link #path}, not a directory. */
    private final boolean zip;
    /** The archive being packed, beside {@link #path}; null until {@link #commit()} packs it. */
    private Path packed;
    /** The CRC-32 of each chunk file written, for its entry in the archive. */
    private final Map<Path, Long> checksums = new HashMap<>();
    private final ZarrGroup rootGroup;
    private final Group root;
    private final List<Node> nodes = new ArrayList<>();
    private final List<Dataset> datasets = new ArrayList<>();
    private boolean finished;

    private ZarrStoreWriter(Path path, Path work, ZarrGroup rootGroup, boolean zip) {
        this.path = path;
        this.work = work;
        this.zip = zip;
        this.rootGroup = rootGroup;
        this.root = new Group(rootGroup);
    }

    /** Begins writing the store at {@code path}; what is already there is replaced by {@link #commit()}. */
    public static ZarrStoreWriter create(Path path) {
        return create(path, false);
    }

    /**
     * Begins writing the store as one ZIP archive at {@code path}; what is
     * already there is replaced by {@link #commit()}. While it is packed the
     * store is on disk once, and its largest chunk twice, beside {@code path}.
     */
    public static ZarrStoreWriter createZip(Path path) {
        return create(path, true);
    }

    private static ZarrStoreWriter create(Path path, boolean zip) {
        Path absolute = path.toAbsolutePath();
        if (!Container.isReplaceable(absolute)) {
            throw new IllegalArgumentException("Refusing to write a Zarr store over " + absolute
                    + ": a directory is there that is not a Zarr store (it holds no zarr.json) and is not empty");
        }
        Path work = absolute.resolveSibling(absolute.getFileName() + "."
                + Long.toUnsignedString(System.nanoTime(), 36) + ".partial");
        FileSystemStore store = FileSystemStore.open(work); // makes the directory
        try {
            return new ZarrStoreWriter(absolute, work, Zarr.createGroup(store), zip);
        } catch (RuntimeException | Error e) {
            deleteTree(work);
            throw e;
        }
    }

    /** Overrides the largest single chunk (tests); returns the previous value. */
    static long setChunkMax(long bytes) {
        if (bytes < 1 || bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("chunk max " + bytes);
        }
        long previous = chunkMax;
        chunkMax = bytes;
        return previous;
    }

    @Override
    public Container container() {
        return zip ? Container.ZARR_ZIP : Container.ZARR;
    }

    @Override
    public WritableGroup root() {
        return root;
    }

    @Override
    public void commit() throws IOException {
        if (finished) {
            throw new IllegalStateException("Zarr store " + path + " is already finished");
        }
        finished = true;
        try {
            for (Dataset d : datasets) {
                if (d.written != d.length) {
                    throw new IOException("Zarr store " + path + " closed with array '" + d.name + "' at "
                            + d.written + " of " + d.length + " bytes: nothing was written to the path");
                }
            }
            for (Node n : nodes) {
                n.writeAttributes();
            }
            // One document that describes every node: a reader opens the store with one read of it.
            rootGroup.consolidate();
            if (zip) {
                packed = path.resolveSibling(path.getFileName() + "."
                        + Long.toUnsignedString(System.nanoTime(), 36) + ".zip.partial");
                pack(packed);
                moveIntoPlace(packed);
                deleteTree(work); // what is left of it: its directories
            } else {
                forceTree(work);
                moveIntoPlace(work);
            }
        } catch (IOException | RuntimeException | Error e) {
            discard();
            throw e;
        }
    }

    @Override
    public void close() {
        if (!finished) {
            finished = true;
            discard();
        }
    }

    private void discard() {
        for (Dataset d : datasets) {
            d.closeQuietly();
        }
        deleteTree(work);
        if (packed != null) {
            deleteTree(packed);
        }
    }

    /**
     * Packs the directory being written into the archive {@code target}: an
     * entry a file, named by its key and not compressed, the root's
     * {@code zarr.json} first and the rest in key order. A file is removed
     * once its entry is written, so the store is on disk twice by no more
     * than one chunk. The archive is forced to disk before this returns.
     */
    private void pack(Path target) throws IOException {
        Map<String, Path> files = new TreeMap<>();
        try (Stream<Path> walk = Files.walk(work)) {
            for (Path p : walk.toList()) {
                if (Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
                    files.put(work.relativize(p).toString().replace('\\', '/'), p);
                }
            }
        }
        List<String> keys = new ArrayList<>(files.keySet());
        if (keys.remove("zarr.json")) {
            keys.add(0, "zarr.json"); // the document a reader opens the store from, where the archive begins
        }
        byte[] buffer = new byte[1 << 20];
        try (FileChannel out = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ZipOutputStream archive = new ZipOutputStream(new BufferedOutputStream(Channels.newOutputStream(out), 1 << 16));
            for (String key : keys) {
                Path file = files.get(key);
                long size = Files.size(file);
                Long crc = checksums.get(file);
                if (crc == null) {
                    crc = checksum(file, buffer); // a metadata document, which Falcon wrote
                }
                ZipEntry entry = new ZipEntry(key);
                entry.setMethod(ZipEntry.STORED);
                entry.setSize(size);
                entry.setCompressedSize(size);
                entry.setCrc(crc);
                entry.setTimeLocal(ARCHIVE_TIME);
                archive.putNextEntry(entry);
                try (FileChannel in = FileChannel.open(file, StandardOpenOption.READ)) {
                    ByteBuffer bytes = ByteBuffer.wrap(buffer);
                    for (int n; (n = in.read(bytes.clear())) >= 0; ) {
                        archive.write(buffer, 0, n);
                    }
                }
                archive.closeEntry(); // fails unless the size and the CRC-32 are what was copied
                Files.delete(file);
            }
            archive.finish();
            archive.flush();
            out.force(true);
        }
    }

    private static long checksum(Path file, byte[] buffer) throws IOException {
        CRC32 crc = new CRC32();
        try (FileChannel in = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer bytes = ByteBuffer.wrap(buffer);
            for (int n; (n = in.read(bytes.clear())) >= 0; ) {
                crc.update(buffer, 0, n);
            }
        }
        return crc.getValue();
    }

    /** Moves the finished store - its directory, or its archive - to the path, replacing what is there. */
    private void moveIntoPlace(Path built) throws IOException {
        Path aside = null;
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            // A directory cannot be replaced in one step: set the old one aside, and remove it once the new one is in.
            aside = path.resolveSibling(path.getFileName() + "." + Long.toUnsignedString(System.nanoTime(), 36) + ".old");
            Files.move(path, aside, StandardCopyOption.ATOMIC_MOVE);
        }
        try {
            Files.move(built, path, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            if (aside != null) {
                try {
                    Files.move(aside, path, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException back) {
                    e.addSuppressed(back);
                }
            }
            throw e;
        }
        if (aside != null) {
            deleteTree(aside);
        }
    }

    /** Forces every file of the tree to disk, and (where the OS lets a directory be opened) its directories. */
    private static void forceTree(Path dir) throws IOException {
        List<Path> all;
        try (Stream<Path> walk = Files.walk(dir)) {
            all = walk.toList();
        }
        for (Path p : all) {
            if (Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
                try (FileChannel c = FileChannel.open(p, StandardOpenOption.WRITE)) {
                    c.force(true);
                }
            }
        }
        for (Path p : all) {
            if (Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS)) {
                try (FileChannel c = FileChannel.open(p, StandardOpenOption.READ)) {
                    c.force(true);
                } catch (IOException ignored) {
                    // Windows does not open a directory this way; its entries are journaled by the file system
                }
            }
        }
    }

    /** Removes a directory tree; a failure to do so is logged, never thrown. */
    static void deleteTree(Path dir) {
        if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        } catch (IOException | RuntimeException e) {
            logger.warn("Failed to remove {}", dir, e);
        }
    }

    /** A node whose attributes are written once, when the store is committed. */
    private abstract class Node {

        /** Sorted by name: the same store whatever order a writer sets them in. Integers or strings. */
        private final Map<String, Object> attributes = new TreeMap<>();

        Node() {
            nodes.add(this);
        }

        abstract ZarrNode node();

        final void attribute(String name, Object value) {
            Objects.requireNonNull(name, "name");
            if (finished) {
                throw new IllegalStateException("Zarr store " + path + " is already finished");
            }
            attributes.put(name, value);
        }

        final void writeAttributes() {
            if (attributes.isEmpty()) {
                return;
            }
            node().setAttributes(json(attributes));
        }
    }

    private final class Group extends Node implements WritableGroup {

        private final ZarrGroup group;

        Group(ZarrGroup group) {
            this.group = group;
        }

        @Override
        ZarrNode node() {
            return group;
        }

        @Override
        public WritableGroup putGroup(String name) {
            return new Group(group.createGroup(name));
        }

        @Override
        public WritableDataset putDataset(String name, byte[] data) {
            Dataset d = createDataset(name, data.length);
            d.write(data, 0, data.length);
            return d;
        }

        @Override
        public Dataset createDataset(String name, long length) {
            if (length < 0) {
                throw new IllegalArgumentException("length " + length);
            }
            // The fewest equal chunks of at most chunkMax bytes: one, for nearly every array.
            long max = chunkMax;
            long chunks = Math.max(1, (length + max - 1) / max);
            long chunk = Math.max(1, (length + chunks - 1) / chunks);
            ZarrArray array = group.createArray(name,
                    ArraySpec.builder(new long[]{length}, DataType.UINT8).chunkShape(chunk).build());
            Dataset d = new Dataset(array, length, chunk);
            datasets.add(d);
            return d;
        }

        @Override
        public void putAttribute(String name, int value) {
            attribute(name, (long) value);
        }

        @Override
        public void putAttribute(String name, long value) {
            attribute(name, value);
        }

        @Override
        public void putAttribute(String name, String value) {
            attribute(name, Objects.requireNonNull(value, "value"));
        }

        /**
         * Through Falcon's own array writer, typed and little-endian, the
         * attributes and dimension names in its metadata. Raw: chunks of whole
         * rows of at most {@link #TYPED_CHUNK_MAX} (and {@code chunkMax})
         * bytes, the bytes codec alone. Compressed: shards of whole rows
         * holding sub-chunks of rows, zstd inside them. Rows are written a
         * chunk (a shard) at a time, so a writer holds at most one.
         */
        @Override
        public com.ebremer.beakgraph.store.TypedArrayWriter createTypedArray(String name,
                com.ebremer.beakgraph.store.TypedArray.Type elementType, long[] dims, List<String> dimensionNames,
                Map<String, Object> attributes, boolean compress) {
            long[] shape = dims.clone();
            long rowElements = com.ebremer.beakgraph.store.TypedArray.rowElements(shape);
            if (shape.length == 0 || rowElements == 0) {
                throw new IllegalArgumentException("typed array '" + name + "' of shape " + java.util.Arrays.toString(shape)
                        + ": rows of at least one element");
            }
            long rowBytes = rowElements * elementType.bytes();
            List<String> names = com.ebremer.beakgraph.store.TypedArray.checkedDimensionNames(dimensionNames, shape.length);
            Map<String, Object> attrs = com.ebremer.beakgraph.store.TypedArray.checkedAttributes(attributes);
            DataType type = switch (elementType) {
                case F64 -> DataType.FLOAT64;
                case F32 -> DataType.FLOAT32;
                case I64 -> DataType.INT64;
                case I32 -> DataType.INT32;
                case BOOL -> DataType.BOOL;
                case U16 -> DataType.UINT16;
                case STR -> DataType.STRING;   // the vlen-utf8 codec
            };
            long rows = Math.max(1, shape[0]);
            long[] chunk = new long[shape.length];
            for (int i = 1; i < shape.length; i++) {
                chunk[i] = shape[i];
            }
            ArraySpec.Builder spec = ArraySpec.builder(shape, type);
            if (elementType != com.ebremer.beakgraph.store.TypedArray.Type.STR) {
                spec.endian(java.nio.ByteOrder.LITTLE_ENDIAN);
            }
            if (compress) {
                long sub = Math.max(1, Math.min(rows, (1L << 20) / rowBytes));
                long shard = sub * Math.max(1, Math.min(rows, Math.max(1, (1L << 26) / rowBytes)) / sub);
                chunk[0] = Math.max(sub, shard);
                long[] inner = chunk.clone();
                inner[0] = sub;
                spec.chunkShape(chunk).sharding(inner).zstd();
            } else {
                chunk[0] = Math.max(1, Math.min(rows, Math.min(chunkMax, TYPED_CHUNK_MAX) / rowBytes));
                spec.chunkShape(chunk);
            }
            if (names != null) {
                spec.dimensionNames(names.toArray(String[]::new));
            }
            if (!attrs.isEmpty()) {
                spec.attributes(json(attrs));
            }
            ZarrArray a = group.createArray(name, spec.build());
            return new TypedRows(a, elementType, shape, rowElements, chunk[0]);
        }
    }

    /**
     * Writes a typed array by regions of whole chunks (whole shards): rows
     * gather in a buffer of one block until it fills, and an append that
     * starts on a block boundary writes its whole blocks straight through.
     */
    private static final class TypedRows implements com.ebremer.beakgraph.store.TypedArrayWriter {

        private final ZarrArray array;
        private final com.ebremer.beakgraph.store.TypedArray.Type type;
        private final long[] shape;
        private final long rowElements;
        private final long blockRows;
        /** Rows written to the array, and rows waiting in the buffer. */
        private long flushed;
        private Object buffer;
        private int buffered;   // elements
        private boolean closed;

        TypedRows(ZarrArray array, com.ebremer.beakgraph.store.TypedArray.Type type, long[] shape, long rowElements,
                  long blockRows) {
            this.array = array;
            this.type = type;
            this.shape = shape;
            this.rowElements = rowElements;
            this.blockRows = blockRows;
        }

        /** The rows of the block being filled: a whole block, or what is left of the array. */
        private long blockElements() {
            return Math.min(blockRows, shape[0] - flushed) * rowElements;
        }

        @Override
        public void append(Object values) {
            int n = com.ebremer.beakgraph.store.TypedArray.checkedLength(type, values);
            long queued = flushed * rowElements + buffered;
            if (n % rowElements != 0 || queued + n > shape[0] * rowElements) {
                throw new IllegalArgumentException(n + " elements appended to '" + array.path() + "' of shape "
                        + java.util.Arrays.toString(shape) + " after " + queued / rowElements + " rows");
            }
            int at = 0;
            if (buffered == 0) {
                // Whole blocks, or the array's end, straight from the caller's values.
                long rowsIn = n / rowElements;
                long direct = (flushed + rowsIn == shape[0]) ? rowsIn : (rowsIn / blockRows) * blockRows;
                if (direct > 0) {
                    int len = Math.toIntExact(direct * rowElements);
                    write(flushed, direct, (len == n) ? values : com.ebremer.beakgraph.store.TypedArray.slice(type, values, 0, len));
                    flushed += direct;
                    at = len;
                }
            }
            while (at < n) {
                if (buffer == null) {
                    buffer = com.ebremer.beakgraph.store.TypedArray.newValues(type, Math.toIntExact(blockElements()));
                }
                int room = Math.toIntExact(blockElements()) - buffered;
                int take = Math.min(room, n - at);
                System.arraycopy(values, at, buffer, buffered, take);
                buffered += take;
                at += take;
                if (buffered == blockElements()) {
                    long r = buffered / rowElements;
                    write(flushed, r, buffer);
                    flushed += r;
                    buffer = null;
                    buffered = 0;
                }
            }
        }

        private void write(long firstRow, long rows, Object values) {
            long[] offset = new long[shape.length];
            offset[0] = firstRow;
            long[] count = shape.clone();
            count[0] = rows;
            com.ebremer.falcon.zarr.Selection region = (firstRow == 0 && rows == shape[0])
                    ? array.selectAll() : array.select(offset, count);
            switch (type) {
                case F64 -> region.writeDoubles((double[]) values);
                case F32 -> region.writeFloats((float[]) values);
                case I64 -> region.writeLongs((long[]) values);
                case I32, U16 -> region.writeInts((int[]) values);
                case STR -> region.writeStrings((String[]) values);
                case BOOL -> {
                    boolean[] b = (boolean[]) values;
                    byte[] raw = new byte[b.length];
                    for (int i = 0; i < b.length; i++) {
                        raw[i] = b[i] ? (byte) 1 : 0;
                    }
                    region.writeRawBytes(raw);
                }
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (buffered > 0) {
                throw new IllegalStateException("typed array '" + array.path() + "' ends inside a block: "
                        + (flushed + buffered / rowElements) + " of its " + shape[0] + " rows");
            }
            if (flushed != shape[0]) {
                throw new IllegalStateException("typed array '" + array.path() + "' has " + flushed + " of its "
                        + shape[0] + " rows");
            }
        }
    }

    /** Attributes as JSON: strings as strings, integers as numbers, in name order. */
    private static JsonObject json(Map<String, Object> attributes) {
        JsonObject.Builder json = JsonObject.builder();
        for (Map.Entry<String, Object> e : new TreeMap<>(attributes).entrySet()) {
            if (e.getValue() instanceof String s) {
                json.put(e.getKey(), s);
            } else {
                json.put(e.getKey(), ((Number) e.getValue()).longValue());
            }
        }
        return json.build();
    }

    private final class Dataset extends Node implements WritableDataset {

        private final ZarrArray array;
        private final String name;
        private final long length;
        /** The declared length of each chunk; the last one is padded to it. */
        private final long chunk;
        private long written;
        /** The chunk being written, or null between chunks. */
        private FileChannel channel;
        /** The file of the chunk being written. */
        private Path chunkFile;
        private long inChunk;
        /** The CRC-32 of the chunk being written: an archive's entry needs it before the bytes. Null for a directory store. */
        private final CRC32 crc = zip ? new CRC32() : null;

        Dataset(ZarrArray array, long length, long chunk) {
            this.array = array;
            this.name = "/" + array.path();
            this.length = length;
            this.chunk = chunk;
        }

        @Override
        ZarrNode node() {
            return array;
        }

        @Override
        public void write(byte[] buf, int off, int len) {
            Objects.checkFromIndexSize(off, len, buf.length);
            if (len > length - written) {
                throw new IllegalStateException((written + len) + " bytes written to a dataset of " + length);
            }
            try {
                while (len > 0) {
                    if (channel == null) {
                        channel = openChunk(written / chunk);
                        inChunk = 0;
                    }
                    int n = (int) Math.min(len, chunk - inChunk);
                    put(ByteBuffer.wrap(buf, off, n));
                    off += n;
                    len -= n;
                    inChunk += n;
                    written += n;
                    if (inChunk == chunk) {
                        closeChunk();
                    }
                }
                if (written == length && channel != null) {
                    // The array ends inside its last chunk: Zarr stores the chunk whole.
                    put(ByteBuffer.allocate((int) (chunk - inChunk)));
                    closeChunk();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot write array '" + name + "' of " + path + " at offset " + written
                        + ": " + e.getMessage(), e);
            }
        }

        private FileChannel openChunk(long index) throws IOException {
            // A directory store keeps the value of a key in the file of that relative path.
            Path file = work.resolve(array.chunkKey(index));
            Files.createDirectories(file.getParent());
            chunkFile = file;
            if (crc != null) {
                crc.reset();
            }
            return FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }

        private void put(ByteBuffer bytes) throws IOException {
            if (crc != null) {
                crc.update(bytes.duplicate());
            }
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
        }

        private void closeChunk() throws IOException {
            FileChannel c = channel;
            channel = null;
            c.close();
            if (crc != null) {
                checksums.put(chunkFile, crc.getValue());
            }
        }

        void closeQuietly() {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // the store is being discarded
                }
                channel = null;
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public long written() {
            return written;
        }

        @Override
        public void putAttribute(String name, int value) {
            attribute(name, value);
        }

        @Override
        public void putAttribute(String name, long value) {
            attribute(name, value);
        }
    }
}
