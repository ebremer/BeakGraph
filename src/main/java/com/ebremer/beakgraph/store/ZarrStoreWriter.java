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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;
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

    private final Path path;
    /** The directory being written, beside {@link #path}. */
    private final Path work;
    private final ZarrGroup rootGroup;
    private final Group root;
    private final List<Node> nodes = new ArrayList<>();
    private final List<Dataset> datasets = new ArrayList<>();
    private boolean finished;

    private ZarrStoreWriter(Path path, Path work, ZarrGroup rootGroup) {
        this.path = path;
        this.work = work;
        this.rootGroup = rootGroup;
        this.root = new Group(rootGroup);
    }

    /** Begins writing the store at {@code path}; what is already there is replaced by {@link #commit()}. */
    public static ZarrStoreWriter create(Path path) {
        Path absolute = path.toAbsolutePath();
        if (!Container.isReplaceable(absolute)) {
            throw new IllegalArgumentException("Refusing to write a Zarr store over " + absolute
                    + ": a directory is there that is not a Zarr store (it holds no zarr.json) and is not empty");
        }
        Path work = absolute.resolveSibling(absolute.getFileName() + "."
                + Long.toUnsignedString(System.nanoTime(), 36) + ".partial");
        FileSystemStore store = FileSystemStore.open(work); // makes the directory
        try {
            return new ZarrStoreWriter(absolute, work, Zarr.createGroup(store));
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
        return Container.ZARR;
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
            forceTree(work);
            moveIntoPlace();
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
    }

    /** Moves the finished directory to the path, replacing what is there. */
    private void moveIntoPlace() throws IOException {
        Path aside = null;
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            // A directory cannot be replaced in one step: set the old one aside, and remove it once the new one is in.
            aside = path.resolveSibling(path.getFileName() + "." + Long.toUnsignedString(System.nanoTime(), 36) + ".old");
            Files.move(path, aside, StandardCopyOption.ATOMIC_MOVE);
        }
        try {
            Files.move(work, path, StandardCopyOption.ATOMIC_MOVE);
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

        /** Sorted by name: the same store whatever order a writer sets them in. */
        private final Map<String, Long> attributes = new TreeMap<>();

        Node() {
            nodes.add(this);
        }

        abstract ZarrNode node();

        final void attribute(String name, long value) {
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
            JsonObject.Builder json = JsonObject.builder();
            attributes.forEach(json::put);
            node().setAttributes(json.build());
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
            attribute(name, value);
        }

        @Override
        public void putAttribute(String name, long value) {
            attribute(name, value);
        }
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
        private long inChunk;

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
            return FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }

        private void put(ByteBuffer bytes) throws IOException {
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
        }

        private void closeChunk() throws IOException {
            FileChannel c = channel;
            channel = null;
            c.close();
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
