package com.ebremer.beakgraph.pool;

import java.util.Locale;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and validates the pooled {@link BeakGraph} readers, keyed by the
 * store's URI: a {@code file:} URI (any form {@code Path.of(URI)} accepts)
 * opens the local file, an {@code http:} / {@code https:} URI opens the
 * store in place over range requests through {@link HTTPSeekableByteChannel}
 * - the same remote read path {@code BG.getBeakGraph(SeekableByteChannel)}
 * offers; every other scheme is rejected up front with a message naming the
 * accepted ones (the pool's URI key type used to promise more than
 * {@code new File(uri)} delivered, BG-277). Replacement detection (file
 * signature) applies to local files only; remote stores rely on the
 * channel's own validator checks.
 *
 * @author erich
 */
public class BeakGraphPoolFactory extends BaseKeyedPooledObjectFactory<URI, BeakGraph> {
    private static final Logger logger = LoggerFactory.getLogger(BeakGraphPoolFactory.class);

    public BeakGraphPoolFactory() {}

    /**
     * The path a pool key names. {@code Path.of(URI)} accepts every form the
     * keys arrive in: {@code File.toURI()}'s {@code file:///C:/...} and, on
     * Windows, the authority form {@code file://server/share/x.h5} that
     * {@code Path.toUri()} produces for a UNC path - which
     * {@code new File(URI)} rejected outright ("URI has an authority
     * component"), turning every query against a store on a network share
     * into a 500.
     */
    static Path toPath(URI uri) {
        return Path.of(uri);
    }

    /**
     * What identifies the file behind a pooled reader: its file key (inode /
     * file id; may be null on some Windows filesystems), size and
     * modification time as they were when the reader opened it.
     */
    record FileSignature(Object fileKey, long size, long modifiedMillis) {
        static FileSignature of(Path path) throws IOException {
            BasicFileAttributes a = Files.readAttributes(path, BasicFileAttributes.class);
            return new FileSignature(a.fileKey(), a.size(), a.lastModifiedTime().toMillis());
        }

        boolean sameFile(FileSignature live) {
            return Objects.equals(fileKey, live.fileKey) && size == live.size && modifiedMillis == live.modifiedMillis;
        }
    }

    /** A pooled reader plus the signature of the file it opened. */
    static final class StorePooledObject extends DefaultPooledObject<BeakGraph> {
        final FileSignature opened;

        StorePooledObject(BeakGraph bg, FileSignature opened) {
            super(bg);
            this.opened = opened;
        }
    }

    /** Whether a pool key names a local file (the default when the URI carries no scheme). */
    static boolean isLocal(URI uri) {
        return uri.getScheme() == null || uri.getScheme().equalsIgnoreCase("file");
    }

    private static HDF5Reader open(URI uri) throws IOException {
        if (isLocal(uri)) {
            return new HDF5Reader(toPath(uri));
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("http") || scheme.equals("https")) {
            return new HDF5Reader(new HTTPSeekableByteChannel(uri), uri); // the reader owns (and on failure closes) the channel
        }
        throw new IllegalArgumentException("BeakGraphPool supports file: and http(s): store URIs, got: " + uri);
    }

    @Override
    public BeakGraph create(URI uri) throws Exception {
        logger.trace("Creating BeakGraph {}", uri);
        HDF5Reader reader = open(uri);
        try {
            return new BeakGraph(reader, uri);
        } catch (RuntimeException | Error e) {
            // Release the mapped file if wrapping fails - a leaked reader pins it.
            try { reader.close(); } catch (Exception ignore) {}
            throw e;
        }
    }

    @Override
    public PooledObject<BeakGraph> wrap(BeakGraph value) {
        FileSignature opened = null;
        try {
            if (isLocal(value.getURI())) {
                opened = FileSignature.of(toPath(value.getURI()));
            }
        } catch (IOException | RuntimeException e) {
            // A signature is a replacement detector, not a requirement: without
            // one the instance is still validated by the data probe below.
            logger.debug("No file signature for pooled BeakGraph {}", value.getURI(), e);
        }
        return new StorePooledObject(value, opened);
    }

    /**
     * Validation on every borrow ({@code testOnBorrow}). Three things can make
     * a pooled instance unfit, and each needs its own check:
     * <ul>
     *   <li>a CLOSED reader (poisoned by a caller): the open flag;</li>
     *   <li>a file TRUNCATED or damaged in place: the data probe, which reads
     *       a real predicate through the mapping and fails on access;</li>
     *   <li>a file REPLACED underneath - the atomic rename-over pattern the
     *       docs recommend. The mapping keeps the OLD inode alive, so the probe
     *       still passes and stale data was served for as long as the store
     *       kept being queried (idle eviction never reaches a busy instance),
     *       with a second per-key slot answering from the NEW file in between.
     *       The file's live signature (file key, size, mtime) is compared with
     *       the one recorded at open; any difference, or a missing file,
     *       discards the instance so the next borrow opens the replacement.</li>
     * </ul>
     * On Windows a move over a mapped file is refused (access denied) while a
     * reader maps it, though renaming the mapped file away is allowed; the
     * restart-free path there is to rename the old store away first, then
     * move the new one in - the signature check reopens on the next borrow
     * just the same.
     */
    @Override
    public boolean validateObject(URI uri, PooledObject<BeakGraph> p) {
        BeakGraph bg = p.getObject();
        if (!bg.getReader().isOpen()) {
            return false;
        }
        if (p instanceof StorePooledObject sp && sp.opened != null) {
            try {
                FileSignature live = FileSignature.of(toPath(uri));
                if (!sp.opened.sameFile(live)) {
                    logger.info("Store {} was replaced since this reader opened it (was {}, now {}); discarding",
                            uri, sp.opened, live);
                    return false;
                }
            } catch (IOException | RuntimeException e) {
                logger.warn("Store {} is no longer readable ({}); discarding its pooled reader", uri, e.toString());
                return false;
            }
        }
        try {
            var predicates = bg.getReader().getDictionary().getPredicates();
            if (predicates.getNumberOfNodes() > 0) {
                predicates.extract(1);
            }
            return true;
        } catch (RuntimeException | Error e) {
            logger.warn("Pooled BeakGraph for {} failed data-access validation; discarding", uri, e);
            return false;
        }
    }

    @Override
    public void destroyObject(URI uri, PooledObject<BeakGraph> p, DestroyMode mode) throws Exception {
        logger.trace("destroyObject {}", uri);
        p.getObject().close();
        super.destroyObject(uri, p, mode);
    }

}
