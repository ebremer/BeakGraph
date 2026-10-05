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
 * not once per read. Transient failures (I/O errors, HTTP 429/5xx, truncated
 * bodies) are retried with exponential backoff and jitter, honouring a
 * {@code Retry-After} delay when the server sends one (BG-11); a server that
 * answers a range request with 200 (no range support) fails loudly rather
 * than silently downloading the whole resource, and a 206 must carry a
 * Content-Range naming exactly the requested bytes (BG-10).
 *
 * <p>The size is resolved at construction with a HEAD request, falling back
 * to a 1-byte range probe for deployments that reject HEAD (e.g.
 * method-scoped presigned URLs). The remote resource is assumed immutable
 * while the channel is open - the BeakGraph contract. When the server
 * supplies a validator (a strong {@code ETag}, else {@code Last-Modified})
 * it is pinned at open and sent as {@code If-Range} with every range
 * request, so an in-place replacement fails loudly (the server answers 200,
 * or a differing ETag / total size) instead of mixing two versions through
 * cached blocks and captured offsets; without any validator only the
 * Content-Range total is checked (BG-380).
 *
 * <p>Sequential consumers (scans, exports) pay one round trip per block by
 * default; a second consecutive cache miss switches the channel into
 * read-ahead, fetching {@code beakgraph.http.readahead} blocks (default 8,
 * 1 MiB) in ONE range request. Scattered reads (binary searches) keep
 * fetching single blocks (BG-246).
 *
 * <p>Redirects are followed once, at open: the size probe's final URL is
 * the one every block request is sent to, so a redirecting front (a bucket
 * website endpoint, a short link, a re-signing edge) costs one extra round
 * trip per channel instead of one per block; a 403/404 from that URL re-resolves
 * the original once, for redirect targets that expire (BG-387).
 *
 * <p>Messages and log lines name the resource without its query string
 * (presigned URLs carry their signature there); {@link #getURI()} returns
 * the raw URI, {@link #getDisplayURI()} the redacted form (BG-225).
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
    private static final int MAX_ATTEMPTS = 5;
    /** First backoff step (doubled per attempt, capped at {@link #MAX_BACKOFF_MS}); -Dbeakgraph.http.retry.base.ms. */
    private static final long RETRY_BASE_MS = Long.getLong("beakgraph.http.retry.base.ms", 250L);
    private static final long MAX_BACKOFF_MS = 5_000L;
    /** A server's Retry-After is honoured up to this long. */
    private static final long MAX_RETRY_AFTER_MS = 30_000L;
    private static final Pattern CONTENT_RANGE = Pattern.compile("\\s*bytes\\s+(\\d+)-(\\d+)/(\\d+|\\*)\\s*");
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);

    /** Blocks fetched per request once a sequential pattern is seen; 0 disables read-ahead. */
    public static final int DEFAULT_READ_AHEAD_BLOCKS = Integer.getInteger("beakgraph.http.readahead", 8);

    private final URI uri;
    private final String display;
    private final HttpClient client;
    /** Where the size probe ended up after redirects: the URL the block requests go to. */
    private volatile URI effectiveUri;
    private final int blockSize;
    private final int readAheadBlocks;
    private final LruCache cache;
    private final long size;
    /** ETag (strong) or Last-Modified captured with the size; null when the server sent neither. */
    private final String validator;
    private final ReentrantLock lock = new ReentrantLock();

    // guarded by lock
    private long position = 0;
    private long bytesServed = 0;
    private long bytesFetched = 0;
    private long rangeRequests = 0;
    private long cacheHits = 0;
    private long cacheMisses = 0;
    private long lastMissBlock = -1;
    private String changedMessage;
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
        this.display = redact(uri);
        this.blockSize = blockSize;
        this.readAheadBlocks = Math.max(0, Math.min(DEFAULT_READ_AHEAD_BLOCKS, maxCacheBlocks / 2));
        this.cache = new LruCache(maxCacheBlocks);
        this.client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
        try {
            Probe probe = probeSize();
            this.size = probe.size();
            this.validator = probe.validator();
            this.effectiveUri = (probe.resolved() != null) ? probe.resolved() : uri;
            if (!effectiveUri.equals(uri)) {
                logger.info("{} redirects to {}; block requests go there", display, redact(effectiveUri));
            }
        } catch (IOException | RuntimeException | Error e) {
            client.close();
            throw e;
        }
        logger.debug("Opened {} ({} bytes, {} KiB blocks, {} block cache, validator {})",
                display, size, blockSize / 1024, maxCacheBlocks, validator == null ? "none" : "pinned");
    }

    /** The resource this channel reads (raw, query string included). */
    public URI getURI() {
        return uri;
    }

    /**
     * The resource without userinfo, query or fragment - what a graph opened
     * over this channel reports as its identity, so a presigned URL's
     * signature never leaves the channel (messages and logs use the same form
     * plus a {@code ?query-redacted} marker when a query was present).
     */
    public URI getDisplayURI() {
        String marker = "?query-redacted";
        return URI.create(display.endsWith(marker) ? display.substring(0, display.length() - marker.length()) : display);
    }

    /** {@code scheme://host[:port]/path}, plus {@code ?query-redacted} when the URI carried a query. */
    public static String redact(URI u) {
        StringBuilder sb = new StringBuilder();
        sb.append(u.getScheme()).append("://");
        if (u.getHost() != null) {
            sb.append(u.getHost());
            if (u.getPort() >= 0) {
                sb.append(':').append(u.getPort());
            }
        }
        String path = u.getRawPath();
        sb.append(path == null || path.isEmpty() ? "/" : path);
        if (u.getRawQuery() != null) {
            sb.append("?query-redacted");
        }
        return sb.toString();
    }

    /** The size and validator the server reported at open, and the URL the probe was finally answered from. */
    private record Probe(long size, String validator, URI resolved) {}

    /** A usable If-Range validator from a response: a strong ETag, else Last-Modified, else null. */
    private static String validatorOf(HttpResponse<?> response) {
        String etag = response.headers().firstValue("ETag").orElse(null);
        if (etag != null && !etag.startsWith("W/")) {
            return etag;
        }
        return response.headers().firstValue("Last-Modified").orElse(null);
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
                    + "{} cache hits / {} misses", display, bytesServed, bytesFetched, rangeRequests,
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

    /** Range requests issued so far - every attempt that received a response, retried ones included; excludes the size probe. */
    public long getRangeRequestCount() {
        lock.lock();
        try {
            return rangeRequests;
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }
        if (changedMessage != null) {
            throw new IOException(changedMessage);
        }
    }

    /** The resource was replaced under the channel: drop every cached block and refuse further reads. */
    private IOException changed(String detail) {
        cache.clear();
        changedMessage = display + " changed on the server while the channel was open (" + detail
                + "); reopen the channel to read the new version";
        return new IOException(changedMessage);
    }

    /**
     * Returns the block, consulting the cache first. Caller holds the lock.
     * A miss right after the previous miss's block is a sequential pattern:
     * the next {@code readAheadBlocks} blocks are fetched in one request.
     */
    private byte[] block(long blockIndex) throws IOException {
        byte[] cached = cache.get(blockIndex);
        if (cached != null) {
            cacheHits++;
            return cached;
        }
        cacheMisses++;
        boolean sequential = lastMissBlock >= 0 && blockIndex == lastMissBlock + 1;
        lastMissBlock = blockIndex;
        long blocksInResource = (size + blockSize - 1) / blockSize;
        int count = 1;
        if (sequential && readAheadBlocks > 1) {
            count = (int) Math.max(1, Math.min(readAheadBlocks, blocksInResource - blockIndex));
        }
        byte[] fetched = fetchBytes(blockIndex * (long) blockSize,
                Math.min(size - 1, blockIndex * (long) blockSize + (long) count * blockSize - 1));
        for (int i = 0; i < count; i++) {
            int from = i * blockSize;
            if (from >= fetched.length) {
                break;
            }
            int len = Math.min(blockSize, fetched.length - from);
            byte[] one = (count == 1) ? fetched : java.util.Arrays.copyOfRange(fetched, from, from + len);
            cache.put(blockIndex + i, one);
        }
        if (count > 1) {
            lastMissBlock = blockIndex + count - 1;
        }
        return cache.get(blockIndex);
    }

    /**
     * Fetches bytes {@code start..end} with a range request, retrying transient
     * failures (connect/read errors, HTTP 429/5xx, truncated bodies) with
     * exponential backoff, or the server's Retry-After when it sends one.
     * Permanent conditions - a 200 answer to a ranged request (no range
     * support), a 206 without or with a mismatched Content-Range, a changed
     * resource, or any other status - fail immediately.
     */
    private byte[] fetchBytes(long start, long end) throws IOException {
        int expected = (int) (end - start + 1);
        URI target = effectiveUri;
        IOException last = null;
        long retryAfterMs = 0;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (attempt > 1) {
                backoff(attempt, retryAfterMs);
                retryAfterMs = 0;
            }
            HttpResponse<InputStream> response;
            try {
                response = send(rangeRequest(target, start, end), HttpResponse.BodyHandlers.ofInputStream());
                rangeRequests++;
                if ((response.statusCode() == 403 || response.statusCode() == 404) && !target.equals(uri)) {
                    // The redirect target captured at open may have expired (a
                    // presigned URL): re-resolve through the original once.
                    response.body().close();
                    logger.info("{} answered HTTP {} at its redirect target; re-resolving through the original URL",
                            display, response.statusCode());
                    target = uri;
                    response = send(rangeRequest(target, start, end), HttpResponse.BodyHandlers.ofInputStream());
                    rangeRequests++;
                    effectiveUri = (response.uri() != null) ? response.uri() : uri;
                    target = effectiveUri;
                }
            } catch (IOException e) {
                last = e;
                continue;
            }
            int status = response.statusCode();
            try (InputStream in = response.body()) {
                // A 200 is acceptable only when the whole resource IS the requested
                // block; otherwise the server ignored the Range header and reading
                // on would download the entire resource.
                if (status == 206 || (status == 200 && start == 0 && size <= expected && validator == null)) {
                    if (status == 206) {
                        // RFC 9110 requires Content-Range on a single-range 206; without
                        // it nothing says the body starts at `start`, and a server that
                        // answered 206 with the whole resource was read as the block (BG-10).
                        String contentRange = response.headers().firstValue("Content-Range").orElse(null);
                        if (contentRange == null) {
                            throw new IOException("Range bytes=" + start + "-" + end + " of " + display
                                    + " answered with 206 but no Content-Range header");
                        }
                        if (!matchesRange(contentRange, start, end)) {
                            throw new IOException("Range bytes=" + start + "-" + end + " of " + display
                                    + " answered with mismatched Content-Range '" + contentRange + "'");
                        }
                        long declared = response.headers().firstValueAsLong("Content-Length").orElse(-1L);
                        if (declared >= 0 && declared != expected) {
                            throw new IOException("Range bytes=" + start + "-" + end + " of " + display
                                    + " answered with a " + declared + "-byte body (Content-Length) instead of " + expected);
                        }
                        // The representation must still be the one the size and
                        // offsets were taken from: same total, same strong ETag.
                        long total = totalOf(contentRange);
                        if (total >= 0 && total != size) {
                            throw changed("size " + size + " at open, now " + total);
                        }
                        String etag = response.headers().firstValue("ETag").orElse(null);
                        if (validator != null && validator.startsWith("\"") && etag != null
                                && !etag.startsWith("W/") && !etag.equals(validator)) {
                            throw changed("ETag " + validator + " at open, now " + etag);
                        }
                    }
                    try {
                        byte[] block = in.readNBytes(expected);
                        if (block.length != expected) {
                            throw new IOException("Truncated body for bytes " + start + "-" + end
                                    + " of " + display + ": expected " + expected + ", got " + block.length);
                        }
                        bytesFetched += expected;
                        return block;
                    } catch (IOException e) {
                        last = e;
                        continue;
                    }
                }
                if (status == 200 && validator != null) {
                    // Either If-Range did not match (the server sent the whole NEW
                    // representation) or the server ignores ranges altogether: the
                    // response's own validator tells the two apart.
                    String current = validatorOf(response);
                    if (current == null || !current.equals(validator)) {
                        throw changed("If-Range " + validator + " no longer matches");
                    }
                }
                if (status == 200) {
                    throw new IOException(display + " does not support HTTP range requests"
                            + " (Range bytes=" + start + "-" + end + " answered with 200)");
                }
                if (status == 416) {
                    throw changed("range bytes=" + start + "-" + end + " no longer satisfiable");
                }
                if (status == 429 || status / 100 == 5) {
                    retryAfterMs = retryAfterMillis(response);
                    last = new IOException("HTTP " + status + " fetching bytes "
                            + start + "-" + end + " of " + display);
                    continue;
                }
                throw new IOException("HTTP " + status + " fetching bytes "
                        + start + "-" + end + " of " + display);
            }
        }
        throw new IOException("Failed to fetch bytes " + start + "-" + end + " of " + display
                + " after " + MAX_ATTEMPTS + " attempts", last);
    }

    private HttpRequest rangeRequest(URI target, long start, long end) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(target)
                .GET()
                .header("Range", "bytes=" + start + "-" + end)
                .timeout(REQUEST_TIMEOUT);
        if (validator != null) {
            builder.header("If-Range", validator);
        }
        return builder.build();
    }

    /** The server's Retry-After as milliseconds (delay-seconds form only, capped), or 0. */
    private static long retryAfterMillis(HttpResponse<?> response) {
        String value = response.headers().firstValue("Retry-After").orElse(null);
        if (value == null) {
            return 0;
        }
        try {
            long seconds = Long.parseLong(value.trim());
            return Math.max(0, Math.min(seconds, MAX_RETRY_AFTER_MS / 1000)) * 1000;
        } catch (NumberFormatException httpDateOrGarbage) {
            return 0; // an HTTP-date is not worth parsing here: the backoff schedule applies
        }
    }

    /** The total after the '/' of a Content-Range ("bytes 0-1/300000"), or -1 when absent or "*". */
    private static long totalOf(String contentRange) {
        if (contentRange == null) {
            return -1;
        }
        int slash = contentRange.lastIndexOf('/');
        if (slash < 0) {
            return -1;
        }
        String total = contentRange.substring(slash + 1).trim();
        if (total.equals("*")) {
            return -1;
        }
        try {
            return Long.parseLong(total);
        } catch (NumberFormatException unparseable) {
            return -1;
        }
    }

    /**
     * Resolves the remote size: HEAD first, then a 1-byte range GET whose
     * Content-Range carries the total - the working path for servers that
     * reject HEAD (e.g. method-scoped presigned URLs) or omit Content-Length.
     */
    private Probe probeSize() throws IOException {
        try {
            HttpRequest head = HttpRequest.newBuilder(uri).HEAD().timeout(REQUEST_TIMEOUT).build();
            HttpResponse<Void> response = send(head, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() / 100 == 2) {
                long length = response.headers().firstValueAsLong("Content-Length").orElse(-1L);
                // A zero Content-Length on HEAD is what a proxy or handler that
                // short-circuits HEAD answers; trusting it made an empty channel
                // and a misleading jHDF error, while the range probe would have
                // returned the real size (BG-9).
                if (length > 0) {
                    return new Probe(length, validatorOf(response), response.uri());
                }
            }
        } catch (IOException fallThrough) {
            logger.debug("HEAD {} failed ({}); falling back to a range probe",
                    display, fallThrough.toString());
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
                            return new Probe(Long.parseLong(total), validatorOf(response), response.uri());
                        } catch (NumberFormatException unparseable) {
                            // handled below
                        }
                    }
                }
                throw new IOException("Cannot determine size of " + display
                        + ": unparseable Content-Range '" + contentRange + "'");
            }
            if (status == 200) {
                throw new IOException(display + " does not support HTTP range requests"
                        + " (Range bytes=0-0 answered with 200)");
            }
            if (status == 416) {
                throw new IOException(display + " is empty (0 bytes): not a BeakGraph store");
            }
            throw new IOException("Cannot determine size of " + display
                    + ": HEAD and range probe both failed (HTTP " + status + ")");
        }
    }

    /** Whether a Content-Range header ("bytes 0-131071/300000") names exactly {@code start..end}. */
    private static boolean matchesRange(String contentRange, long start, long end) {
        Matcher m = CONTENT_RANGE.matcher(contentRange);
        try {
            return m.matches() && Long.parseLong(m.group(1)) == start && Long.parseLong(m.group(2)) == end;
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
            throw new IOException("Interrupted while fetching " + display, e);
        }
    }

    /**
     * Sleeps before retry {@code attempt} (2..MAX_ATTEMPTS): the server's
     * Retry-After when it sent one, else an exponential step with jitter
     * (base, 2x, 4x ... capped) - the former 100/200 ms gave a throttling
     * object store three hits within 300 ms and then a hard failure (BG-11).
     */
    private void backoff(int attempt, long retryAfterMs) throws IOException {
        long base = Math.min(MAX_BACKOFF_MS, RETRY_BASE_MS << Math.min(attempt - 2, 20));
        long jittered = base / 2 + java.util.concurrent.ThreadLocalRandom.current().nextLong(Math.max(1, base / 2));
        long sleep = Math.max(retryAfterMs, jittered);
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while fetching " + display, e);
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
