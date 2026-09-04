package com.ebremer.beakgraph.lws;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the LWS metadata of a storage directory in step with the files on disk.
 * <p>
 * The metadata model is published as an immutable snapshot that readers fetch
 * through {@link #current()}; a refresh builds a whole new model and swaps the
 * reference, so in-flight requests keep the snapshot they started with and no
 * reader ever observes a half-updated graph. A refresh happens only when the
 * tree's {@link LWSMetadataGenerator#treeSignature fingerprint} differs from
 * the snapshot's, so an idle scan costs one directory walk and nothing else.
 * Three things trigger a scan: {@link #refreshIfChanged()} at startup
 * (validating a cached {@code beakgraph.ttl.gz} that may predate changes made
 * while the server was down), the periodic scan started by {@link #start},
 * and {@link #refreshOnDemand(Path)}, which the servlet calls when a request
 * names a path that exists on disk but not in the snapshot - so a freshly
 * copied file is servable at once. After every regeneration the cache file is
 * rewritten so the next start is fast.
 */
public final class LWSMetadataRefresher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LWSMetadataRefresher.class);

    private final Path root;
    private final Path cache;
    private final AtomicReference<Model> current;
    /** Wall-clock start of the most recent scan; a file older than it was already seen by that scan. */
    private volatile long lastScanMillis;
    private ScheduledExecutorService scheduler;

    public LWSMetadataRefresher(Path root, Model initial) {
        this.root = root;
        this.cache = root.resolve(LWSMetadataGenerator.CACHE_FILE_NAME);
        this.current = new AtomicReference<>(initial);
    }

    /** The metadata snapshot in force right now. Never mutated; replaced wholesale on refresh. */
    public Model current() {
        return current.get();
    }

    /**
     * Compares the tree with the current snapshot and, if they differ,
     * regenerates the metadata, publishes it and rewrites the cache file.
     *
     * @return true when a new snapshot was published
     */
    public synchronized boolean refreshIfChanged() {
        // Stamped BEFORE the walk: a file modified while the walk runs may or may
        // not have been seen, so it must still count as newer than this scan.
        lastScanMillis = System.currentTimeMillis();
        try {
            Set<String> onDisk = LWSMetadataGenerator.treeSignature(root);
            if (onDisk.equals(LWSMetadataGenerator.modelSignature(current.get()))) {
                return false;
            }
            Model fresh = LWSMetadataGenerator.generateLWSModel(root);
            current.set(fresh);
            try {
                LWSMetadataGenerator.writeModelToGZ(fresh, cache);
            } catch (IOException e) {
                logger.warn("LWS metadata for {} regenerated, but the cache {} could not be rewritten: {}",
                        root, cache, e.toString());
            }
            logger.info("LWS metadata for {} refreshed: {} entries", root, onDisk.size());
            return true;
        } catch (IOException | RuntimeException e) {
            logger.warn("LWS metadata refresh for {} failed; the previous metadata stays in service", root, e);
            return false;
        }
    }

    /**
     * Scan for a request-path miss: {@code candidate} exists on disk but the
     * snapshot does not list it. A scan runs only when the file is newer than
     * the last scan - anything older was already examined, so repeated
     * requests for the same path, or a burst of them, cost one walk at most.
     * Only files that really appeared on disk can trigger a walk; requests
     * for paths that do not exist never reach here.
     *
     * @return true when a new snapshot was published
     */
    public synchronized boolean refreshOnDemand(Path candidate) {
        long modified;
        try {
            modified = Files.getLastModifiedTime(candidate).toMillis();
        } catch (IOException e) {
            return false;
        }
        if (modified < lastScanMillis) {
            return false;
        }
        return refreshIfChanged();
    }

    /** Starts the periodic scan on a daemon thread; {@code intervalSeconds <= 0} leaves it off. */
    public synchronized void start(long intervalSeconds) {
        if (scheduler != null || intervalSeconds <= 0) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lws-metadata-refresh");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> refreshIfChanged(), intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        ScheduledExecutorService s;
        synchronized (this) {
            s = scheduler;
            scheduler = null;
        }
        if (s != null) {
            s.shutdownNow();
        }
    }
}
