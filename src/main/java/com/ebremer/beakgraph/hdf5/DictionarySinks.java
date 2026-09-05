package com.ebremer.beakgraph.hdf5;

import java.io.IOException;

/**
 * The write-side capabilities a dictionary encoder needs from its buffers,
 * implemented by both buffer families - the in-memory ones
 * ({@link BitPackedUnSignedLongBuffer}, {@code FCDWriter},
 * {@link DataOutputBuffer}) and the disk-backed ones of the huge writer
 * ({@code SpillBitPackedBuffer}, {@code SpillFCDWriter},
 * {@code SpillDataBuffer}). They already exposed identical method
 * signatures without a common type, which is why the node encoding had to
 * be written twice (BG-298).
 *
 * @author Erich Bremer
 */
public final class DictionarySinks {

    private DictionarySinks() {}

    /** A bit-packed unsigned column. */
    public interface LongSink {
        void writeInteger(int value);

        void writeLong(long value);

        long getNumEntries();
    }

    /** A front-coded string dictionary. */
    public interface StringSink {
        void add(String item) throws IOException;

        long getNumEntries();
    }

    /** A raw float / double value store. */
    public interface RealSink {
        void writeFloat(float value) throws IOException;

        void writeDouble(double value) throws IOException;

        long getNumEntries();
    }
}
