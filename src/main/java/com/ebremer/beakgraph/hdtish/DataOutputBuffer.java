package com.ebremer.beakgraph.hdtish;

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
    
    public DataOutputBuffer(Path file) throws FileNotFoundException {
        this.path = file;
        baos = new ByteArrayOutputStream();
        dos = new DataOutputStream(baos);
    }
    
    public void writeInt(int v) throws IOException {
        dos.writeInt(v);
    }

    public void writeLong(long v) throws IOException {
        dos.writeLong(v);
    }
    
    public void writeFloat(float v) throws IOException {
        dos.writeFloat(v);
    }

    public void writeDouble(double v) throws IOException {
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
