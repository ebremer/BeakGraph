package com.ebremer.beakgraph.hdf5;

import com.ebremer.falcon.hdf5.Hdf5Writer;
import java.io.IOException;
import java.nio.file.Path;

/**
 * An HDF5 file being written by an in-memory writer, in BeakGraph's container
 * profile (SPECIFICATIONS.md §3): groups, 1-D datasets of bytes in contiguous
 * layout, and scalar 32-bit / 64-bit integer attributes. With
 * {@link WritableGroup} and {@link WritableDataset} it is the writers' one door
 * into the HDF5 library (Falcon, https://github.com/ebremer/falcon), as
 * {@code HdfProfile} is the readers'.
 *
 * <p>Falcon writes each dataset to a temporary file beside {@code path} once
 * the next object is added, so a dataset's bytes are held only until then, and
 * moves the finished file to {@code path} on {@link #commit()}, forced to disk
 * first. A build that fails leaves nothing behind: {@link #close()} without a
 * commit discards the temporary file. So write inside try-with-resources, and
 * commit as its last statement:
 *
 * <pre>{@code
 * try (WritableHdfFile file = WritableHdfFile.write(tmp)) {
 *     WritableGroup store = file.putGroup(".BG");
 *     ...
 *     file.commit();
 * }
 * }</pre>
 *
 * @author Erich Bremer
 */
public final class WritableHdfFile implements AutoCloseable {

    private final Hdf5Writer writer;
    private boolean finished;

    private WritableHdfFile(Hdf5Writer writer) {
        this.writer = writer;
    }

    /** Begins writing the file at {@code path}; a file already there is replaced by {@link #commit()}. */
    public static WritableHdfFile write(Path path) {
        return new WritableHdfFile(Hdf5Writer.create(path));
    }

    /** Creates (and returns) a group in the file's root group. */
    public WritableGroup putGroup(String name) {
        return new WritableGroup(writer.group(name));
    }

    /**
     * Completes the file: writes what is still pending and the file's
     * metadata, forces it to disk, and moves it to its path. A failure
     * discards the temporary file before it propagates.
     */
    public void commit() throws IOException {
        finished = true;
        try {
            writer.close();
        } catch (IOException | RuntimeException | Error e) {
            try {
                writer.abort();
            } catch (RuntimeException | Error cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    /** Discards the file unless it was {@linkplain #commit() committed}; never fails. */
    @Override
    public void close() {
        if (!finished) {
            finished = true;
            try {
                writer.abort();
            } catch (RuntimeException ignored) {
                // the build has already failed; its own exception is the one to report
            }
        }
    }
}
