package com.ebremer.beakgraph.huge;

import java.io.IOException;

/**
 * A group inside a {@link StreamingHdf5File}.
 *
 * <p>Attribute types are deliberately restricted to the two the BeakGraph
 * format uses: a 32-bit integer (read back by jHDF as {@link Integer}) and a
 * 64-bit integer (read back as {@link Long}). The existing readers cast
 * attribute values to exactly those boxed types, so an implementation must
 * preserve the width faithfully.
 *
 * @author Erich Bremer
 */
public interface StreamingHdf5Group {

    /** Creates (and returns) a child group. */
    StreamingHdf5Group putGroup(String name) throws IOException;

    /** Writes a scalar 32-bit integer attribute (jHDF reads it as Integer). */
    void putAttribute(String name, int value) throws IOException;

    /** Writes a scalar 64-bit integer attribute (jHDF reads it as Long). */
    void putAttribute(String name, long value) throws IOException;

    /**
     * Creates a 1-D dataset of 8-bit elements with the given total length and
     * CONTIGUOUS layout. The returned dataset is populated by sequential
     * {@link StreamingHdf5Dataset#write} calls and must be written completely
     * (exactly {@code length} bytes) before it is closed.
     *
     * @param name   dataset name within this group
     * @param length total number of bytes the dataset will hold; must be &gt; 0
     */
    StreamingHdf5Dataset createByteDataset(String name, long length) throws IOException;
}
