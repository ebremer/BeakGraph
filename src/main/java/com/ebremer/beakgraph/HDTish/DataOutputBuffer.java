package com.ebremer.beakgraph.HDTish;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

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
    
    @Override
    public Map<String, Object> getProperties() {
        Map<String,Object> meta = new HashMap<>();
        meta.put("numEntries", numEntries);
        return meta;
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
        baos.close();
    }

    @Override
    public Path getName() {
        return path;
    }

    @Override
    public byte[] getBuffer() {
       return baos.toByteArray();
    }
}
