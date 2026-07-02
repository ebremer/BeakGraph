package com.ebremer.beakgraph.huge;

import java.io.IOException;

/**
 * A 1-D byte dataset being written incrementally. Bytes are appended strictly
 * sequentially; the dataset must receive exactly the length declared at
 * creation before {@link #close()} (closing an under-filled dataset is an
 * error - it would leave undefined bytes for the readers to map).
 *
 * @author Erich Bremer
 */
public interface StreamingHdf5Dataset extends AutoCloseable {

    /** Appends {@code len} bytes of {@code buf} starting at {@code off}. */
    void write(byte[] buf, int off, int len) throws IOException;

    default void write(byte[] buf) throws IOException {
        write(buf, 0, buf.length);
    }

    /** Writes a scalar 32-bit integer attribute (jHDF reads it as Integer). */
    void putAttribute(String name, int value) throws IOException;

    /** Writes a scalar 64-bit integer attribute (jHDF reads it as Long). */
    void putAttribute(String name, long value) throws IOException;

    @Override
    void close() throws IOException;
}
