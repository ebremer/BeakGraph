package com.ebremer.beakgraph.hdtish;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 *
 * @author Erich Bremer
 */
public class DataBuffer implements AutoCloseable {
    
    private final BufferedOutputStream baos;
    private final DataOutputStream dos;
    
    public DataBuffer(File file) throws FileNotFoundException {
        baos = new BufferedOutputStream(new FileOutputStream(file));
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
}
