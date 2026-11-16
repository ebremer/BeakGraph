package com.ebremer.beakgraph.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.io.BlockCache;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import com.ebremer.falcon.zarr.store.MemoryStore;
import com.ebremer.falcon.zarr.store.Store;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A Zarr store read where it lies on a web server: the same answers as from
 * the directory, every array by ranges through one cache of blocks, and a
 * store that changed or does not match what it declares named as that.
 */
class RemoteZarrTest {

    private static final String[] QUERIES = {
        "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }",
        "SELECT ?s ?o WHERE { GRAPH ?g { ?s <http://ex.org/name> ?o } }",
        "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
        "SELECT ?o WHERE { GRAPH <http://ex.org/g7> { <http://ex.org/s507> <http://ex.org/name> ?o } }",
        "SELECT ?s WHERE { GRAPH ?g { ?s <http://ex.org/size> ?n FILTER(?n > 1490) } }",
    };

    /** The bytes of a block in these tests: the store is small, and its largest array is to be several blocks. */
    private static final int BLOCK = 256;

    @TempDir
    static Path dir;
    static Path zarr;
    /** The key of the store's largest chunk: an array of several blocks. */
    static String bigKey;
    private int[] cache;

    @BeforeAll
    static void build() throws Exception {
        StringBuilder nq = new StringBuilder();
        for (int i = 0; i < 1500; i++) {
            String g = "<http://ex.org/g" + (i % 20) + ">";
            String s = "<http://ex.org/s" + i + ">";
            nq.append(s).append(" <http://ex.org/name> \"the name of subject number ").append(i).append("\" ").append(g).append(" .\n");
            nq.append(s).append(" <http://ex.org/size> \"").append(i).append("\"^^<http://www.w3.org/2001/XMLSchema#int> ").append(g).append(" .\n");
            nq.append(s).append(" <http://ex.org/next> <http://ex.org/s").append((i * 7 + 1) % 1500).append("> ").append(g).append(" .\n");
        }
        Path src = dir.resolve("remote.nq");
        Files.writeString(src, nq, StandardCharsets.UTF_8);
        zarr = dir.resolve("remote.zarr");
        WriterEngines.all().findFirst().orElseThrow().buildStore(src.toFile(), zarr.toFile());
        long most = 0;
        try (Stream<Path> walk = Files.walk(zarr)) {
            for (Path p : walk.filter(Files::isRegularFile).toList()) {
                String key = zarr.relativize(p).toString().replace('\\', '/');
                if (key.endsWith("/c/0") && Files.size(p) > most) {
                    most = Files.size(p);
                    bigKey = key;
                }
            }
        }
        assertTrue(most > 10 * BLOCK, "an array of several blocks to read: " + bigKey + ", " + most + " bytes");
    }

    @BeforeEach
    void smallBlocks() {
        cache = ZarrStore.setCache(BLOCK, 4096); // small blocks, and room for every one of the store's
    }

    @AfterEach
    void defaultBlocks() {
        ZarrStore.setCache(cache[0], cache[1]);
    }

    private static List<String> rows(BeakGraph bg, String query) {
        List<String> rows = new ArrayList<>(StoreParity.select(bg.getDataset(), query));
        Collections.sort(rows);
        return rows;
    }

    /** The array whose first chunk is under {@code key} ({@code group/.../array/c/0}). */
    private static StoreArray array(OpenStore store, String key) {
        String[] names = key.split("/");
        StoreGroup group = store.root();
        for (int i = 0; i < names.length - 3; i++) {
            group = group.group(names[i]);
        }
        return group.array(names[names.length - 3]);
    }

    private static byte[] all(RandomAccessBytes bytes) {
        byte[] read = new byte[(int) bytes.size()];
        bytes.get(0, read, 0, read.length);
        return read;
    }

    /** A copy of the store beside it, to change. */
    private static Path copy(String name) throws IOException {
        Path copy = dir.resolve(name);
        try (Stream<Path> walk = Files.walk(zarr)) {
            for (Path p : walk.toList()) {
                Path target = copy.resolve(zarr.relativize(p).toString());
                if (Files.isDirectory(p)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(p, target);
                }
            }
        }
        return copy;
    }

    private static String messages(Throwable t) {
        StringBuilder all = new StringBuilder();
        for (Throwable x = t; x != null; x = x.getCause() == x ? null : x.getCause()) {
            all.append(x).append('\n');
            for (Throwable s : x.getSuppressed()) {
                all.append(s).append('\n');
            }
        }
        return all.toString();
    }

    @Test
    void aStoreOverHttpAnswersAsTheLocalOne() throws Exception {
        try (TreeServer server = new TreeServer(dir)) {
            URI uri = server.uri("/remote.zarr");
            try (BeakGraph remote = BG.getBeakGraph(uri); BeakGraph local = BG.getBeakGraph(zarr)) {
                assertEquals(uri, remote.getURI());
                HDF5Reader reader = (HDF5Reader) remote.getReader();
                assertEquals(Container.ZARR, reader.getContainer());
                assertTrue(reader.isChannelBacked(), "read by ranges: the query engine keeps its reads in order");
                // A store opens from one document; of its arrays, a reader looks at the block directories of the
                // dictionary then, and at no other until it is asked.
                List<String> atOpen = new ArrayList<>(server.requests);
                assertEquals("GET /remote.zarr/zarr.json", atOpen.get(0));
                assertTrue(atOpen.stream().skip(1).allMatch(r -> r.startsWith("GET ") && r.contains("_blocks/c/0")), atOpen.toString());
                assertTrue(atOpen.size() <= 1 + 9, atOpen.toString());
                for (String query : QUERIES) {
                    List<String> expected = rows(local, query);
                    assertFalse(expected.isEmpty(), query);
                    assertEquals(expected, rows(remote, query), query);
                }
            }
            List<String> chunks = server.requests("GET ").stream().filter(r -> r.contains("/c/")).toList();
            assertFalse(chunks.isEmpty());
            assertTrue(chunks.stream().allMatch(r -> r.contains(" bytes=")), "an array is read by ranges only: " + chunks);
            assertEquals(1, server.requests.stream().filter(r -> r.endsWith("zarr.json")).count(),
                    "every node is described by the one document: " + server.requests.stream().filter(r -> r.contains("zarr.json")).toList());
            // Nothing is asked about a chunk but the ranges read of it: its size is the array's to say, and the
            // validator its later ranges are pinned to comes with the first.
            assertEquals(List.of(), server.requests("HEAD "));
            assertEquals(server.requests.size(), server.requests("GET ").size());
            Map<String, Integer> first = new TreeMap<>();
            chunks.forEach(r -> first.merge(r.substring(0, r.indexOf(" bytes=")), 1, Integer::sum));
            assertTrue(first.size() > 20, "the arrays the queries read: " + first.keySet());
        }
    }

    @Test
    void whatWasReadIsNotFetchedAgain() throws Exception {
        try (TreeServer server = new TreeServer(dir)) {
            URI uri = server.uri("/remote.zarr");
            ZarrStore store = ZarrStore.open(uri);
            try (BeakGraph remote = new BeakGraph(new HDF5Reader(store, uri)); BeakGraph local = BG.getBeakGraph(zarr)) {
                for (String query : QUERIES) {
                    assertEquals(rows(local, query), rows(remote, query), query);
                }
                int afterFirst = server.requests.size();
                int held = store.cachedBlocks();
                assertTrue(held > 20, held + " blocks of the store's arrays, in one cache");
                for (String query : QUERIES) {
                    assertEquals(rows(local, query), rows(remote, query), query);
                }
                // What the queries read once is held: asked again, nothing more is asked of the server.
                assertEquals(afterFirst, server.requests.size(),
                        "requests the second time: " + server.requests.subList(afterFirst, server.requests.size()));
                assertEquals(held, store.cachedBlocks());
            }
            assertEquals(0, store.cachedBlocks(), "closed: the blocks are let go");
        }
        // A cache of four blocks for the whole store: the same answers, from what the cache can hold.
        ZarrStore.setCache(BLOCK, 4);
        try (TreeServer server = new TreeServer(dir)) {
            URI uri = server.uri("/remote.zarr");
            ZarrStore store = ZarrStore.open(uri);
            try (BeakGraph remote = new BeakGraph(new HDF5Reader(store, uri)); BeakGraph local = BG.getBeakGraph(zarr)) {
                for (String query : QUERIES) {
                    assertEquals(rows(local, query), rows(remote, query), query);
                    assertTrue(store.cachedBlocks() <= 4, store.cachedBlocks() + " blocks held of a budget of 4");
                }
            }
        }
    }

    @Test
    void aScanFetchesSeveralBlocksAtATime() throws Exception {
        try (TreeServer server = new TreeServer(dir); ZarrStore store = ZarrStore.open(server.uri("/remote.zarr"))) {
            RandomAccessBytes bytes = array(store, bigKey).bytes();
            assertTrue(bytes.isRemote());
            assertArrayEquals(Files.readAllBytes(zarr.resolve(bigKey)), all(bytes));
            long blocks = (bytes.size() + BLOCK - 1) / BLOCK;
            List<String> ranges = server.requests("GET /remote.zarr/" + bigKey);
            assertTrue(ranges.size() >= 2 && ranges.size() <= blocks / 3 + 2,
                    blocks + " blocks read in order came in " + ranges.size() + " requests: " + ranges);
            // Read here and there afterwards: all of it is held.
            int before = server.requests.size();
            assertEquals(Files.readAllBytes(zarr.resolve(bigKey))[(int) bytes.size() - 1], bytes.get(bytes.size() - 1));
            bytes.getLong(BLOCK - 6);
            assertEquals(before, server.requests.size());
            assertThrows(IndexOutOfBoundsException.class, () -> bytes.get(bytes.size()));
        }
    }

    @Test
    void aChunkReplacedOnTheServerFailsLoudly() throws Exception {
        Path replaced = copy("replaced.zarr");
        Path chunk = replaced.resolve(bigKey);
        try (TreeServer server = new TreeServer(dir); ZarrStore store = ZarrStore.open(server.uri("/replaced.zarr"))) {
            RandomAccessBytes bytes = array(store, bigKey).bytes();
            byte first = bytes.get(0); // the first block is held from here on
            // Another build's chunk, of the same length: only its validator says so.
            byte[] other = Files.readAllBytes(chunk);
            for (int i = 0; i < other.length; i++) {
                other[i] ^= 0x5A;
            }
            Files.write(chunk, other);
            Files.setLastModifiedTime(chunk, FileTime.fromMillis(Files.getLastModifiedTime(chunk).toMillis() + 10_000));
            UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> bytes.get(bytes.size() - 1));
            assertTrue(messages(e).contains("changed on the server"), messages(e));
            // Nothing of either version is served from here on: not the block that was held, either.
            UncheckedIOException again = assertThrows(UncheckedIOException.class, () -> bytes.get(0));
            assertTrue(messages(again).contains("changed on the server"), messages(again));
            assertEquals(Files.readAllBytes(zarr.resolve(bigKey))[0], first);
        }
    }

    @Test
    void aChunkThatWasReadAndIsGoneIsNotZeros() throws Exception {
        Path gone = copy("gone.zarr");
        try (TreeServer server = new TreeServer(dir); ZarrStore store = ZarrStore.open(server.uri("/gone.zarr"))) {
            RandomAccessBytes bytes = array(store, bigKey).bytes();
            bytes.get(0);
            Files.delete(gone.resolve(bigKey));
            UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> bytes.get(bytes.size() - 1));
            assertFalse(messages(e).contains("fill value"), messages(e));
            assertTrue(messages(e).contains("HTTP 404"), messages(e));
        }
    }

    private static byte[] readAt(HTTPSeekableByteChannel channel, long position, int n) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(n);
        channel.position(position);
        while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
            // until it is full
        }
        return buffer.array();
    }

    @Test
    void aChannelToldItsSizeAsksOnlyForWhatIsRead() throws Exception {
        byte[] data = new byte[3000];
        new java.util.Random(5).nextBytes(data);
        Files.write(dir.resolve("object.bin"), data);
        try (TreeServer server = new TreeServer(dir); HttpClient client = HTTPSeekableByteChannel.newClient()) {
            BlockCache cache = new BlockCache(BLOCK, 64);
            URI uri = server.uri("/object.bin");
            HTTPSeekableByteChannel channel = HTTPSeekableByteChannel.sized(uri, data.length, cache, client);
            assertEquals(data.length, channel.size());
            assertEquals(List.of(), new ArrayList<>(server.requests), "nothing is asked until something is read");
            assertArrayEquals(Arrays.copyOfRange(data, 1000, 1010), readAt(channel, 1000, 10));
            assertEquals(List.of("GET /object.bin bytes=768-1023"), new ArrayList<>(server.requests), "the block that holds it, and no look at the object first");
            assertArrayEquals(Arrays.copyOfRange(data, 2990, 3000), readAt(channel, 2990, 10));
            assertArrayEquals(Arrays.copyOfRange(data, 1001, 1021), readAt(channel, 1001, 20));
            assertEquals(2, server.requests.size(), server.requests.toString());
            assertEquals(2, channel.getRangeRequestCount());

            // A resource that is not the size it was said to be: longer, shorter, and so short no range of it is there.
            for (long said : new long[] {data.length + 1, data.length - 1, 100_000}) {
                HTTPSeekableByteChannel wrong = HTTPSeekableByteChannel.sized(uri, said, cache, client);
                IOException e = assertThrows(IOException.class, () -> readAt(wrong, Math.min(said - 1, 50_000), 1));
                assertTrue(e.getMessage().contains("3000 bytes, not the " + said) || e.getMessage().contains("shorter than the " + said),
                        e.getMessage());
                wrong.close();
            }
            // One the server does not have: told apart from a failure, for the caller to say what an absent object means.
            HTTPSeekableByteChannel absent = HTTPSeekableByteChannel.sized(server.uri("/none.bin"), 10, cache, client);
            assertThrows(FileNotFoundException.class, () -> readAt(absent, 0, 1));
            absent.close();

            // Its blocks are in the cache it was given, and leave with it; the client is the caller's, and lives on.
            assertTrue(cache.size() >= 2);
            channel.close();
            assertEquals(0, cache.size());
            HTTPSeekableByteChannel again = HTTPSeekableByteChannel.sized(uri, data.length, cache, client);
            assertArrayEquals(Arrays.copyOfRange(data, 0, 5), readAt(again, 0, 5));
            again.close();
            assertThrows(IllegalArgumentException.class, () -> HTTPSeekableByteChannel.sized(uri, -1, cache, client));
            assertThrows(IllegalArgumentException.class, () -> HTTPSeekableByteChannel.sized(URI.create("ftp://x/y"), 1, cache, client));
        }
    }

    @Test
    void aChunkTheServerDoesNotHaveReadsAsZeros() throws Exception {
        Path sparse = copy("sparse.zarr");
        long size = Files.size(sparse.resolve(bigKey));
        Files.delete(sparse.resolve(bigKey)); // as a writer that leaves out a chunk of fill values does
        try (TreeServer server = new TreeServer(dir); ZarrStore store = ZarrStore.open(server.uri("/sparse.zarr"))) {
            RandomAccessBytes bytes = array(store, bigKey).bytes();
            byte[] read = all(bytes);
            assertEquals(size, read.length);
            assertArrayEquals(new byte[read.length], read, "a chunk that is not stored is the fill value, zero");
            // Found absent by the first range asked of it, and never asked for again.
            List<String> asked = server.requests.stream().filter(r -> r.contains(bigKey)).toList();
            assertEquals(1, asked.size(), "nothing to fetch, block after block: " + asked);
            assertArrayEquals(new byte[read.length], all(bytes));
            assertEquals(asked.size(), server.requests.stream().filter(r -> r.contains(bigKey)).count());
        }
    }

    @Test
    void aChunkThatIsNotTheSizeItsArrayDeclaresIsRefused() throws Exception {
        Path cut = copy("cut.zarr");
        Path chunk = cut.resolve(bigKey);
        byte[] whole = Files.readAllBytes(chunk);
        Files.write(chunk, Arrays.copyOf(whole, whole.length - 1));
        try (TreeServer server = new TreeServer(dir); ZarrStore store = ZarrStore.open(server.uri("/cut.zarr"))) {
            RandomAccessBytes bytes = array(store, bigKey).bytes();
            UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> bytes.get(0));
            String said = messages(e);
            assertTrue(said.contains(bigKey) && said.contains((whole.length - 1) + " bytes, not the " + whole.length), said);
        }
    }

    @Test
    void aQueryInTheUrlGoesWithEveryRequestAndIntoNoMessage() throws Exception {
        try (TreeServer server = new TreeServer(dir)) {
            URI signed = server.uri("/remote.zarr?X-Signature=s3cr3t%2Fvalue&expires=1");
            try (BeakGraph remote = BG.getBeakGraph(signed); BeakGraph local = BG.getBeakGraph(zarr)) {
                assertEquals(server.uri("/remote.zarr"), remote.getURI(), "the graph is named without the signature");
                assertEquals(rows(local, QUERIES[1]), rows(remote, QUERIES[1]));
            }
            assertFalse(server.requests.isEmpty());
            for (String request : server.requests) {
                assertTrue(request.contains("?X-Signature=s3cr3t%2Fvalue&expires=1"), "sent as it came: " + request);
            }
            // What cannot be opened, or read, is named without it.
            Throwable missing = assertThrows(Exception.class, () -> BG.getBeakGraph(server.uri("/nothing.zarr?X-Signature=s3cr3t")).close());
            assertFalse(messages(missing).contains("s3cr3t"), messages(missing));
            assertTrue(messages(missing).contains("/nothing.zarr"), messages(missing));
            Path cut = copy("signed-cut.zarr");
            Files.write(cut.resolve(bigKey), new byte[3]);
            try (ZarrStore store = ZarrStore.open(server.uri("/signed-cut.zarr?X-Signature=s3cr3t"))) {
                RandomAccessBytes bytes = array(store, bigKey).bytes();
                UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> bytes.get(0));
                assertFalse(messages(e).contains("s3cr3t"), messages(e));
            }
        }
    }

    @Test
    void whatIsNotAStoreUrlIsSaidSo() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ZarrStore.open(URI.create("ftp://example.org/a.zarr")));
        assertThrows(IllegalArgumentException.class, () -> BG.getBeakGraph(URI.create("s3://bucket/a.zarr")));
        IllegalArgumentException archive = assertThrows(IllegalArgumentException.class,
                () -> BG.getBeakGraph(URI.create("https://example.org/data/a.zarr.zip?sig=s3cr3t")));
        assertTrue(archive.getMessage().contains("read from a local file") && !archive.getMessage().contains("s3cr3t"), archive.getMessage());
        assertEquals(Container.ZARR, Container.ofRemote(URI.create("https://example.org/data/a.zarr/")));
        assertEquals(Container.ZARR, Container.ofRemote(URI.create("https://example.org/data/a.ZARR?x=y.h5")));
        assertEquals(Container.HDF5, Container.ofRemote(URI.create("https://example.org/data.zarr/a.h5")));
        assertEquals(Container.HDF5, Container.ofRemote(URI.create("https://example.org/")));
        // A file: URI is the local store, whichever container.
        try (BeakGraph local = BG.getBeakGraph(zarr.toUri())) {
            assertFalse(rows(local, QUERIES[1]).isEmpty());
        }
    }

    @Test
    void thePoolOpensAStoreOverHttp() throws Exception {
        try (TreeServer server = new TreeServer(dir); BeakGraph local = BG.getBeakGraph(zarr)) {
            URI uri = server.uri("/remote.zarr");
            var pool = BeakGraphPool.getPool();
            try {
                for (int round = 0; round < 2; round++) {
                    BeakGraph remote = pool.borrowObject(uri);
                    try {
                        assertEquals(rows(local, QUERIES[2]), rows(remote, QUERIES[2]));
                    } finally {
                        pool.returnObject(uri, remote);
                    }
                }
                assertEquals(1, server.requests.stream().filter(r -> r.endsWith("zarr.json")).count(), "one reader, kept");
                Exception archive = assertThrows(Exception.class, () -> pool.borrowObject(server.uri("/remote.zarr.zip")));
                assertTrue(messages(archive).contains("read from a local file"), messages(archive));
            } finally {
                pool.clear(uri);
            }
        }
    }

    /** A Falcon store that maps nothing and counts the ranges asked of it. */
    private static final class Counting implements Store {

        private final Store inner;
        final AtomicInteger ranges = new AtomicInteger();

        Counting(Store inner) {
            this.inner = inner;
        }

        @Override public Optional<byte[]> get(String key) { return inner.get(key); }
        @Override public Optional<byte[]> getRange(String key, long offset, long length) {
            ranges.incrementAndGet();
            return inner.getRange(key, offset, length);
        }
        @Override public boolean exists(String key) { return inner.exists(key); }
        @Override public OptionalLong size(String key) { return inner.size(key); }
        @Override public List<String> list() { return inner.list(); }
        @Override public List<String> listPrefix(String prefix) { return inner.listPrefix(prefix); }
        @Override public List<String> listDir(String prefix) { return inner.listDir(prefix); }
        @Override public boolean isWritable() { return false; }
        @Override public void set(String key, byte[] value) { throw new UnsupportedOperationException(); }
        @Override public void delete(String key) { throw new UnsupportedOperationException(); }
    }

    @Test
    void anyFalconStoreIsReadThroughTheCache() throws Exception {
        MemoryStore memory = new MemoryStore();
        try (Stream<Path> walk = Files.walk(zarr)) {
            for (Path p : walk.filter(Files::isRegularFile).toList()) {
                memory.set(zarr.relativize(p).toString().replace('\\', '/'), Files.readAllBytes(p));
            }
        }
        byte[] expected = Files.readAllBytes(zarr.resolve(bigKey));
        long blocks = (expected.length + BLOCK - 1) / BLOCK;
        Counting counting = new Counting(memory);
        try (ZarrStore store = ZarrStore.open(counting, "memory")) {
            assertTrue(store.isChannelBacked());
            RandomAccessBytes bytes = array(store, bigKey).bytes();
            assertArrayEquals(expected, all(bytes));
            int first = counting.ranges.get();
            assertTrue(first >= 2 && first <= blocks / 3 + 2, blocks + " blocks read in order came in " + first + " ranges");
            assertArrayEquals(expected, all(bytes));
            assertEquals(expected[expected.length - 1], bytes.get(expected.length - 1));
            assertEquals(first, counting.ranges.get(), "held: nothing more is asked of the store");
            assertTrue(store.cachedBlocks() >= blocks);
        }
        // A budget of two blocks: read back and forth, the same bytes, and never more than two held.
        ZarrStore.setCache(BLOCK, 2);
        try (ZarrStore store = ZarrStore.open(counting, "memory")) {
            RandomAccessBytes bytes = array(store, bigKey).bytes();
            java.util.Random random = new java.util.Random(3);
            for (int round = 0; round < 300; round++) {
                int at = random.nextInt(expected.length);
                assertEquals(expected[at], bytes.get(at), "at " + at);
                assertTrue(store.cachedBlocks() <= 2);
            }
            assertArrayEquals(expected, all(bytes));
        }
        // The queries, through a store that maps nothing.
        ZarrStore.setCache(BLOCK, 64);
        try (BeakGraph ranged = new BeakGraph(new HDF5Reader(ZarrStore.open(counting, "memory"), URI.create("bg:/memory")));
             BeakGraph local = BG.getBeakGraph(zarr)) {
            for (String query : QUERIES) {
                assertEquals(rows(local, query), rows(ranged, query), query);
            }
        }
    }
}
