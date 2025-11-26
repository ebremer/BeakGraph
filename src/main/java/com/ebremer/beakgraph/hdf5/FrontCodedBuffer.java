package com.ebremer.beakgraph.hdf5;

import java.util.ArrayList;
import java.util.List;

public class FrontCodedBuffer {

    private static class FrontCodedEntry {
        public final int prefixLength;
        public final String suffix;

        public FrontCodedEntry(int prefixLength, String suffix) {
            this.prefixLength = prefixLength;
            this.suffix = suffix;
        }
    }

    private final List<FrontCodedEntry> buffer = new ArrayList<>();
    private final int blockSize;
    private String currentBlockFirstString;
    private long numEntries = 0;
    private long numBlocks = 0;

    public FrontCodedBuffer(int blockSize) {        
        if (blockSize < 1) {
            throw new IllegalArgumentException("blockSize must be at least 1");
        }
        this.blockSize = blockSize;
        this.currentBlockFirstString = null;
    }

    public void add(String x) {
        numEntries++;
        if (buffer.size() % blockSize == 0) {
            currentBlockFirstString = x;
            buffer.add(new FrontCodedEntry(0, x));
            numBlocks++;
        } else {
            int k = commonPrefixLength(currentBlockFirstString, x);
            buffer.add(new FrontCodedEntry(k, x.substring(k)));
        }
    }

    public long getNumBlocks() {
        return numBlocks;
    }
    
    public long getNumEntries() {
        return numEntries;
    }

    private static int commonPrefixLength(String s1, String s2) {
        int minLen = Math.min(s1.length(), s2.length());
        for (int i = 0; i < minLen; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return i;
            }
        }
        return minLen;
    }
}