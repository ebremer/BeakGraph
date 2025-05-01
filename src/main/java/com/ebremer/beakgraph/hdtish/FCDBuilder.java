package com.ebremer.beakgraph.hdtish;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FCDBuilder implements AutoCloseable {
    private final int blockSize;
    private int stringsInCurrentBlock = 0;
    private String previousString = null;
    private OutputStream baos;
    private DataOutputStream dos;

    public FCDBuilder(int blockSize) throws FileNotFoundException {
        this.blockSize = blockSize;
        baos = new BufferedOutputStream(new FileOutputStream(new File("/tcga/strings")));
        dos = new DataOutputStream(baos); 
    }
    
    @Override
    public void close() throws Exception {
        baos.close();
    }

    public void add(String item) throws IOException {
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
}
