package com.ebremer.beakgraph.store;

import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.beakgraph.hdf5.WritableHdfFile;
import java.io.IOException;
import java.nio.file.Path;

/**
 * A store being written, in whichever container ({@link Container}): groups,
 * 1-D byte arrays and scalar integer attributes (SPECIFICATIONS.md §3). With
 * {@link WritableGroup} and {@code WritableDataset} it is every writer's one
 * door into the container, as {@link OpenStore} is the readers': the
 * in-memory writers hand each array over whole
 * ({@link WritableGroup#putDataset}), the disk-based ones a piece at a time
 * ({@link WritableGroup#createDataset}). What a writer puts through it is the
 * same call for call whatever the container, so the two hold the same store.
 *
 * <p>Nothing is at {@code path} until {@link #commit()}: the store is
 * written beside it - a temporary file for HDF5, a temporary directory for
 * Zarr (packed into a temporary file for a ZIP archive) - forced to disk,
 * and moved there in one step. A build that fails
 * leaves nothing behind: {@link #close()} without a commit discards what was
 * written. So write inside try-with-resources, and commit as its last
 * statement:
 *
 * <pre>{@code
 * try (WritableStore store = WritableStore.create(tmp, container)) {
 *     WritableGroup bg = store.putGroup(".BG");
 *     ...
 *     store.commit();
 * }
 * }</pre>
 *
 * @author Erich Bremer
 */
public interface WritableStore extends AutoCloseable {

    /** Begins writing a store at {@code path} in {@code container}; what is already there is replaced by {@link #commit()}. */
    static WritableStore create(Path path, Container container) {
        return switch (container) {
            case HDF5 -> WritableHdfFile.write(path);
            case ZARR -> ZarrStoreWriter.create(path);
            case ZARR_ZIP -> ZarrStoreWriter.createZip(path);
        };
    }

    /** The container being written. */
    Container container();

    /** The store's root group. */
    WritableGroup root();

    /** Creates (and returns) a group in the store's root group. */
    default WritableGroup putGroup(String name) {
        return root().putGroup(name);
    }

    /**
     * Completes the store: writes what is still pending and its metadata,
     * forces it to disk, and moves it to its path. A failure discards what
     * was written before it propagates.
     */
    void commit() throws IOException;

    /** Discards the store unless it was {@linkplain #commit() committed}; never fails. */
    @Override
    void close();
}
