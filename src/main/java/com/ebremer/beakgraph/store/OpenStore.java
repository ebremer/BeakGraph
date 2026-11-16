package com.ebremer.beakgraph.store;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

/**
 * A store opened for reading, in whichever container it is kept
 * ({@link Container}): the root of its groups, and the one thing that
 * releases it. The readers are handed its {@link StoreGroup}s and never see
 * the container.
 *
 * <p>Closing it ends every view of its bytes at once - a memory-mapped file
 * is unmapped then and there, on every OS - so a store must not be closed
 * while a query still reads through it; a view read afterwards throws
 * {@link IllegalStateException}.
 *
 * @author Erich Bremer
 */
public interface OpenStore extends AutoCloseable {

    /** The container the store is kept in. */
    Container container();

    /** The root group: the one that holds the {@code .BG} group. */
    StoreGroup root();

    /**
     * Whether the store's bytes are read a range at a time through a channel
     * or a remote store, rather than from local memory mappings. Such reads
     * are serialized and slow, so the query engine keeps them sequential.
     */
    boolean isChannelBacked();

    /** Releases the store: unmaps its files, and closes the channel it owns. Never fails. */
    @Override
    void close();

    /**
     * Opens the store at {@code path}: a directory as a Zarr store, a ZIP
     * archive as the Zarr store it holds, any other file as an HDF5 file,
     * memory-mapped whichever it is.
     *
     * @throws java.io.UncheckedIOException if it cannot be opened
     */
    static OpenStore open(Path path) {
        return switch (Container.ofExisting(path)) {
            case ZARR -> ZarrStore.open(path);
            case ZARR_ZIP -> ZarrStore.openZip(path);
            case HDF5 -> Hdf5Store.open(path);
        };
    }

    /**
     * Opens the Zarr store at an {@code http} or {@code https} URL, read a
     * range at a time through a block cache ({@link ZarrStore#open(URI)}).
     * An HDF5 file over HTTP is opened through its channel instead
     * ({@link #open(SeekableByteChannel, URI)}).
     *
     * @throws java.io.UncheckedIOException if it cannot be opened
     */
    static OpenStore open(URI zarr) {
        return ZarrStore.open(zarr);
    }

    /**
     * Opens an HDF5 store read through {@code channel} (an HTTP range channel,
     * typically). The store takes ownership of the channel: it is closed by
     * {@link #close()}, and also released if opening fails.
     *
     * @param source identifies the data in messages
     */
    static OpenStore open(SeekableByteChannel channel, URI source) {
        return Hdf5Store.open(channel, source);
    }
}
