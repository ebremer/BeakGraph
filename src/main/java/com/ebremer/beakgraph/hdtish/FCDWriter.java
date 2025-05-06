package com.ebremer.beakgraph.hdtish;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FCDWriter implements HDF5Buffer, AutoCloseable {
    private final int blockSize;
    private int stringsInCurrentBlock = 0;
    private String previousString = null;
    private ByteArrayOutputStream baos;
    private DataOutputStream dos;
    private Path path;
    private long numBlocks = 0;
    private long numEntries = 0;

    public FCDWriter(Path path, int blockSize) throws FileNotFoundException {
        this.path = path;
        this.blockSize = blockSize;
        this.baos = new ByteArrayOutputStream(); //new BufferedOutputStream(new FileOutputStream(new File("/tcga/strings")), 32768);
        dos = new DataOutputStream(baos); 
    }
    
    @Override
    public Map<String, Object> getProperties() {
        Map<String,Object> meta = new HashMap<>();
        meta.put("blockSize", blockSize);
        meta.put("numBlocks", numBlocks);
        meta.put("numEntries", numEntries);
        return meta;
    }
    
    @Override
    public void close() throws Exception {
        baos.close();
    }

    public void add(String item) throws IOException {
        numEntries++;
        if (stringsInCurrentBlock == 0) {
            dos.writeUTF(item);
            previousString = item;
            stringsInCurrentBlock = 1;
        } else {
            int prefixLength = commonPrefixLength(previousString, item);
            //writeVByte(prefixLength);
            dos.writeUTF(item.substring(prefixLength));
            previousString = item;
            stringsInCurrentBlock++;
            if (stringsInCurrentBlock == blockSize) {
                stringsInCurrentBlock = 0;
            }
        }
    }

    private int commonPrefixLength(String s1, String s2) {
        int minLength = Math.min(s1.length(), s2.length());
        for (int i = 0; i < minLength; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return i;
            }
        }
        return minLength;
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
