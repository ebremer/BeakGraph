package com.ebremer.beakgraph.hdtish;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

/**
 *
 * @author Erich Bremer
 */
public class DataInputBuffer implements AutoCloseable {
    
    private final BufferedInputStream baos;
    private final DataInputStream dis;
    
    public DataInputBuffer(Path file) throws FileNotFoundException {
        baos = new BufferedInputStream(new FileInputStream(file.toFile()));
        dis = new DataInputStream(baos);
    }
    
    public int readInt() throws IOException {
        return dis.readInt();
    }

    public long readLong() throws IOException {
        return dis.readLong();
    }
    
    public float readFloat(float v) throws IOException {
        return dis.readFloat();
    }

    public double readDouble(double v) throws IOException {
        return dis.readDouble();
    }    

    @Override
    public void close() throws Exception {
        baos.close();
    }
}
