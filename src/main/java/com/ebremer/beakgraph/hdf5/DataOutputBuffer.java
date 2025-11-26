package com.ebremer.beakgraph.hdf5;

import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

/**
 *
 * @author Erich Bremer
 */
public class DataOutputBuffer implements HDF5Buffer, AutoCloseable {
    
    private final ByteArrayOutputStream baos;
    private final DataOutputStream dos;
    private final Path path;
    private long numEntries = 0;
    
    public DataOutputBuffer(Path file) throws FileNotFoundException {
        this.path = file;
        baos = new ByteArrayOutputStream();        
        dos = new DataOutputStream(baos);        
    }
    
    public void writeInt(int v) throws IOException {
        numEntries++;
        dos.writeInt(v);
    }

    public void writeLong(long v) throws IOException {
        numEntries++;
        dos.writeLong(v);
    }
    
    public void writeFloat(float v) throws IOException {
        numEntries++;
        dos.writeFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        numEntries++;
        dos.writeDouble(v);
    }    

    @Override
    public void close() throws Exception {
        try (baos) {
            baos.flush();
        }
    }

    @Override
    public Path getName() {
        return path;
    }

    @Override
    public long getNumEntries() {
        return numEntries;
    }

    @Override
    public void Add(WritableGroup group) {
        WritableDataset ds = group.putDataset(path.toString(), baos.toByteArray());
        ds.putAttribute("numEntries", numEntries);
    }   
}
