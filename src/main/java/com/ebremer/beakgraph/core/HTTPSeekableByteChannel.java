package com.ebremer.beakgraph.core;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only {@link SeekableByteChannel} over an HTTP(S) resource, backed by
 * HTTP range requests - the transport that lets a BeakGraph be queried in
 * place on a web server or object store without downloading it first:
 *
 * <pre>{@code
 * try (BeakGraph bg = BG.getBeakGraph(
 *         new HTTPSeekableByteChannel(URI.create("https://example.org/data.h5")))) {
 *     ...
 * }
 * }</pre>
 *
 * <p>Reads are served from an LRU cache of block-aligned ranges (default
 * 128 KiB blocks, 256 blocks = 32 MiB), so the binary searches the readers
 * issue against dictionaries and indexes touch the network once per block,
 * not once per read. Transient failures (I/O errors, HTTP 429/5xx) are
 * retried with backoff; a server that answers a range request with 200 (no
 * range support) fails loudly rather than silently downloading the whole
 * resource.
 *
 * <p>The size is resolved at construction with a HEAD request, falling back
 * to a 1-byte range probe for deployments that reject HEAD (e.g.
 * method-scoped presigned URLs). The remote resource is assumed immutable
 * while the channel is open - the BeakGraph contract; if it changes
 * server-side, subsequent range reads may fail or return torn data.
 *
 * <p>Write aspects of the interface ({@link #write(ByteBuffer)},
 * {@link #truncate(long)}) throw {@link NonWritableChannelException}. The
 * channel is thread-safe: position, cache and metrics are guarded by one
 * lock, so concurrent readers serialize (network latency, not lock hold
 * time, dominates).
 *
 * @author Erich Bremer
 */
public final class HTTPSeekableByteChannel implements SeekableByteChannel {

    private static final Logger logger = LoggerFactory.getLogger(HTTPSeekableByteChannel.class);

    private static final int DEFAULT_BLOCK_SIZE = 128 * 1024;
    private static final int DEFAULT_MAX_CACHE_BLOCKS = 256; // 32 MiB with default blocks
    private static final int MAX_ATTEMPTS = 3;
    private static final Pattern CONTENT_RANGE = Pattern.compile("\\s*bytes\\s+(\\d+)-(\\d+)/.*");
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);

    private final URI uri;
    private final HttpClient client;
    private final int blockSize;
    private final LruCache cache;
    private final long size;
    private final ReentrantLock lock = new ReentrantLock();

    // guarded by lock
    private long position = 0;
    private long bytesServed = 0;
    private long bytesFetched = 0;
    private long rangeRequests = 0;
    private long cacheHits = 0;
    private long cacheMisses = 0;
    private volatile boolean open = true;

    /** Opens {@code uri} with the default block size and cache capacity. */
    public HTTPSeekableByteChannel(URI uri) throws IOException {
        this(uri, DEFAULT_BLOCK_SIZE, DEFAULT_MAX_CACHE_BLOCKS);
    }

    /**
     * Opens {@code uri} with a specific fetch granularity and cache capacity
     * (memory ceiling is {@code blockSize * maxCacheBlocks}).
     *
     * @throws IOException if the size of the remote resource cannot be determined
     */
    public HTTPSeekableByteChannel(URI uri, int blockSize, int maxCacheBlocks) throws IOException {
        Objects.requireNonNull(uri, "uri");
        String scheme = (uri.getScheme() == null) ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            throw new IllegalArgumentException("Not an http(s) URI: " + uri);
        }
        if (blockSize <= 0 || maxCacheBlocks <= 0) {
            throw new IllegalArgumentException(
                    "blockSize and maxCacheBlocks must be positive: " + blockSize + ", " + maxCacheBlocks);
        }
        this.uri = uri;
        this.blockSize = blockSize;
        this.cache = new LruCache(maxCacheBlocks);
        this.client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
        try {
            this.size = probeSize();
        } catch (IOException | RuntimeException | Error e) {
            client.close();
            throw e;
        }
        logger.debug("Opened {} ({} bytes, {} KiB blocks, {} block cache)",
                uri, size, blockSize / 1024, maxCacheBlocks);
    }

    /** The resource this channel reads. */
    public URI getURI() {
        return uri;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            if (position >= size) {
                return -1;
            }
            int transferred = 0;
            while (dst.hasRemaining() && position < size) {
                long blockIndex = position / blockSize;
                int withinBlock = (int) (position - blockIndex * (long) blockSize);
                byte[] block = block(blockIndex);
                int n = Math.min(dst.remaining(), block.length - withinBlock);
                dst.put(block, withinBlock, n);
                position += n;
                transferred += n;
            }
            bytesServed += transferred;
            return transferred;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int write(ByteBuffer src) {
        throw new NonWritableChannelException();
    }

    @Override
    public long position() throws IOException {
        lock.lock();
        try {
            ensureOpen();
            return position;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0) {
            throw new IllegalArgumentException("Negative position: " + newPosition);
        }
        lock.lock();
        try {
            ensureOpen();
            // beyond-size is legal per the SeekableByteChannel contract; reads there hit EOF
            position = newPosition;
            return this;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long size() throws IOException {
        lock.lock();
        try {
            ensureOpen();
            return size;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (!open) {
                return;
            }
            open = false;
            cache.clear();
            client.close();
            logger.debug("Closed {}: served {} bytes, fetched {} bytes in {} range requests, "
                    + "{} cache hits / {} misses", uri, bytesServed, bytesFetched, rangeRequests,
                    cacheHits, cacheMisses);
        } finally {
            lock.unlock();
        }
    }

    /** Bytes actually transferred over the network so far. */
    public long getBytesFetched() {
        lock.lock();
        try {
            return bytesFetched;
        } finally {
            lock.unlock();
        }
    }

    /** Range requests issued so far (excludes the size probe). */
    public long getRangeRequestCount() {
        lock.lock();
        try {
            return rangeRequests;
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() throws ClosedChannelException {
        if (!open) {
            throw new ClosedChannelException();
        }
    }

    /** Returns the block, consulting the cache first. Caller holds the lock. */
    private byte[] block(long blockIndex) throws IOException {
        byte[] cached = cache.get(blockIndex);
        if (cached != null) {
            cacheHits++;
            return cached;
        }
        cacheMisses++;
        byte[] fetched = fetchBlock(blockIndex);
        cache.put(blockIndex, fetched);
        return fetched;
    }

    /**
     * Fetches one block with a range request, retrying transient failures
     * (connect/read errors, HTTP 429/5xx, truncated bodies) with linear
     * backoff. Permanent conditions - a 200 answer to a ranged request (no
     * range support), or any other status - fail immediately.
     */
    private byte[] fetchBlock(long blockIndex) throws IOException {
        long start = blockIndex * (long) blockSize;
        long end = Math.min(size - 1, start + blockSize - 1);
        int expected = (int) (end - start + 1);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .GET()
                .header("Range", "bytes=" + start + "-" + end)
                .timeout(REQUEST_TIMEOUT)
                .build();
        IOException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (attempt > 1) {
                backoff(attempt);
            }
            HttpResponse<InputStream> response;
            try {
                response = send(request, HttpResponse.BodyHandlers.ofInputStream());
            } catch (IOException e) {
                last = e;
                continue;
            }
            int status = response.statusCode();
            try (InputStream in = response.body()) {
                // A 200 is acceptable only when the whole resource IS the requested
                // block; otherwise the server ignored the Range header and reading
                // on would download the entire resource.
                if (status == 206 || (status == 200 && start == 0 && size <= expected)) {
                    if (status == 206) {
                        // A 206 for a DIFFERENT range than asked would silently
                        // corrupt reads - verify the offset before trusting the body.
                        String contentRange = response.headers().firstValue("Content-Range").orElse(null);
                        if (contentRange != null && !startsAt(contentRange, start)) {
                            throw new IOException("Range bytes=" + start + "-" + end + " of " + uri
                                    + " answered with mismatched Content-Range '" + contentRange + "'");
                        }
                    }
                    try {
                        byte[] block = in.readNBytes(expected);
                        if (block.length != expected) {
                            throw new IOException("Truncated body for bytes " + start + "-" + end
                                    + " of " + uri + ": expected " + expected + ", got " + block.length);
                        }
                        rangeRequests++;
                        bytesFetched += expected;
                        return block;
                    } catch (IOException e) {
                        last = e;
                        continue;
                    }
                }
                if (status == 200) {
                    throw new IOException(uri + " does not support HTTP range requests"
                            + " (Range bytes=" + start + "-" + end + " answered with 200)");
                }
                if (status == 429 || status / 100 == 5) {
                    last = new IOException("HTTP " + status + " fetching bytes "
                            + start + "-" + end + " of " + uri);
                    continue;
                }
                throw new IOException("HTTP " + status + " fetching bytes "
                        + start + "-" + end + " of " + uri);
            }
        }
        throw new IOException("Failed to fetch bytes " + start + "-" + end + " of " + uri
                + " after " + MAX_ATTEMPTS + " attempts", last);
    }

    /**
     * Resolves the remote size: HEAD first, then a 1-byte range GET whose
     * Content-Range carries the total - the working path for servers that
     * reject HEAD (e.g. method-scoped presigned URLs) or omit Content-Length.
     */
    private long probeSize() throws IOException {
        try {
            HttpRequest head = HttpRequest.newBuilder(uri).HEAD().timeout(REQUEST_TIMEOUT).build();
            HttpResponse<Void> response = send(head, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() / 100 == 2) {
                long length = response.headers().firstValueAsLong("Content-Length").orElse(-1L);
                if (length >= 0) {
                    return length;
                }
            }
        } catch (IOException fallThrough) {
            logger.debug("HEAD {} failed ({}); falling back to a range probe",
                    uri, fallThrough.toString());
        }
        HttpRequest probe = HttpRequest.newBuilder(uri)
                .GET()
                .header("Range", "bytes=0-0")
                .timeout(REQUEST_TIMEOUT)
                .build();
        HttpResponse<InputStream> response = send(probe, HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream in = response.body()) {
            int status = response.statusCode();
            if (status == 206) {
                // "bytes 0-0/12345" - the total rides after the slash
                String contentRange = response.headers().firstValue("Content-Range").orElse("");
                int slash = contentRange.lastIndexOf('/');
                if (slash >= 0) {
                    String total = contentRange.substring(slash + 1).trim();
                    if (!total.equals("*")) {
                        try {
                            return Long.parseLong(total);
                        } catch (NumberFormatException unparseable) {
                            // handled below
                        }
                    }
                }
                throw new IOException("Cannot determine size of " + uri
                        + ": unparseable Content-Range '" + contentRange + "'");
            }
            if (status == 200) {
                throw new IOException(uri + " does not support HTTP range requests"
                        + " (Range bytes=0-0 answered with 200)");
            }
            throw new IOException("Cannot determine size of " + uri
                    + ": HEAD and range probe both failed (HTTP " + status + ")");
        }
    }

    /** Whether a Content-Range header ("bytes 0-131071/300000") starts at {@code start}. */
    private static boolean startsAt(String contentRange, long start) {
        Matcher m = CONTENT_RANGE.matcher(contentRange);
        try {
            return m.matches() && Long.parseLong(m.group(1)) == start;
        } catch (NumberFormatException overflows) {
            return false;
        }
    }

    private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
            throws IOException {
        try {
            return client.send(request, handler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while fetching " + uri, e);
        }
    }

    private void backoff(int attempt) throws IOException {
        try {
            Thread.sleep(100L * (attempt - 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while fetching " + uri, e);
        }
    }

    /** Access-ordered LRU of fetched blocks, bounded by block count. */
    private static final class LruCache extends LinkedHashMap<Long, byte[]> {
        private static final long serialVersionUID = 1L;
        private final int maxBlocks;

        LruCache(int maxBlocks) {
            super(16, 0.75f, true);
            this.maxBlocks = maxBlocks;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, byte[]> eldest) {
            return size() > maxBlocks;
        }
    }
}
