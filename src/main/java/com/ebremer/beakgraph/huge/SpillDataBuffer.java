package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.Params;

import com.ebremer.beakgraph.hdf5.DictionarySinks;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Disk-backed twin of {@link com.ebremer.beakgraph.hdf5.DataOutputBuffer}:
 * big-endian primitive stream (via {@link DataOutputStream}, exactly like the
 * RAM twin) accumulated in a temp file, transferred to a dataset with the
 * {@code numEntries} attribute.
 *
 * @author Erich Bremer
 */
final class SpillDataBuffer implements AutoCloseable, DictionarySinks.RealSink {

    private final Path file;
    private final DataOutputStream dos;
    private long numEntries = 0;
    private long bytesWritten = 0;
    private boolean completed = false;

    SpillDataBuffer(Path file) throws IOException {
        this.file = file;
        this.dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file), 1 << 16));
    }

    void writeInt(int v) throws IOException {
        numEntries++;
        bytesWritten += Integer.BYTES;
        dos.writeInt(v);
    }

    void writeLong(long v) throws IOException {
        numEntries++;
        bytesWritten += Long.BYTES;
        dos.writeLong(v);
    }

    public void writeFloat(float v) throws IOException {
        numEntries++;
        bytesWritten += Float.BYTES;
        dos.writeFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        numEntries++;
        bytesWritten += Double.BYTES;
        dos.writeDouble(v);
    }

    public long getNumEntries() {
        return numEntries;
    }

    void complete() throws IOException {
        if (completed) return;
        completed = true;
        dos.close();
    }

    /**
     * Streams the bytes into a dataset named {@code name} with the
     * {@code numEntries} attribute, then deletes the temp file. An empty buffer
     * (never produced by the writer for a dataset that is actually added, same
     * as the RAM path) is skipped.
     */
    void transferTo(StreamingHdf5Group group, String name) throws IOException {
        complete();
        if (bytesWritten == 0) {
            if (numEntries > 0) {
                throw new IllegalStateException(
                    "CRITICAL ERROR: dataset " + name + " has no bytes but numEntries is " + numEntries);
            }
            Files.deleteIfExists(file);
            return;
        }
        try (StreamingHdf5Dataset ds = group.createByteDataset(name, bytesWritten)) {
            HugeIO.copyFileIntoDataset(file, ds);
            ds.putAttribute(Params.NUM_ENTRIES, numEntries);
        }
        Files.deleteIfExists(file);
    }

    @Override
    public void close() throws IOException {
        complete();
        Files.deleteIfExists(file);
    }
}
