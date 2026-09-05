package com.ebremer.beakgraph.core;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes a finished build: moves the sibling {@code .tmp} file over the
 * destination atomically, and never destroys the build if that fails.
 * <p>
 * Every writer builds into {@code <dest>.tmp} and renames it into place. That
 * rename can fail for reasons unrelated to the build: on Windows a
 * destination that a reader (the endpoint's pool, HDFView, this JVM) has
 * memory-mapped cannot be replaced - the move is refused with an access
 * error, not {@link AtomicMoveNotSupportedException}. The writers used to
 * handle the publish inside the same catch that cleans up a failed build,
 * so a finished multi-hour store was deleted because its destination was
 * busy. Publishing now retries briefly (readers usually release within
 * seconds), then keeps the build as {@code <dest>.new} and says exactly
 * what to do with it.
 */
public final class AtomicPublish {
    private static final Logger logger = LoggerFactory.getLogger(AtomicPublish.class);
    private static final int ATTEMPTS = Integer.getInteger("beakgraph.publish.attempts", 5);
    private static final long BACKOFF_MS = Long.getLong("beakgraph.publish.backoff.millis", 500L);

    private AtomicPublish() {}

    /**
     * A unique sibling temp path for a build of {@code dest}: two builds of the
     * same destination at once (a.ttl and a.nt both mapping to a.h5 under
     * -threads, or two programmatic writers) used to share one deterministic
     * {@code <dest>.tmp} and truncate each other's work.
     */
    public static Path tempFor(Path dest) {
        return dest.resolveSibling(dest.getFileName() + "." + Long.toUnsignedString(System.nanoTime(), 36)
                + "-" + Long.toUnsignedString(Thread.currentThread().threadId(), 36) + ".tmp");
    }

    /**
     * Moves {@code tmp} over {@code dest}. On failure the finished file is
     * retained (as {@code <dest>.new}, or as {@code tmp} if even that rename
     * fails) and the thrown exception names the retained path.
     *
     * @throws IOException when the destination could not be replaced; the
     *         message names where the finished build was kept
     */
    public static void publish(Path tmp, Path dest) throws IOException {
        IOException last = null;
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            try {
                try {
                    Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
                }
                return;
            } catch (IOException e) {
                last = e;
                if (attempt < ATTEMPTS) {
                    logger.warn("Publishing {} failed (attempt {}/{}): {}; retrying in {} ms", dest, attempt, ATTEMPTS, e, BACKOFF_MS);
                    try {
                        Thread.sleep(BACKOFF_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        Path retained = dest.resolveSibling(dest.getFileName() + ".new");
        try {
            Files.move(tmp, retained, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException renameAside) {
            last.addSuppressed(renameAside);
            retained = tmp;
        }
        throw new IOException("Built store could not be published to " + dest
                + " - is it open or memory-mapped by a reader (an endpoint, HDFView)? The finished build was KEPT at "
                + retained + ": stop the reader, then move it over " + dest + ".", last);
    }
}
