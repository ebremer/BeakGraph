package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.hdf5.DataOutputBuffer;
import com.ebremer.beakgraph.hdf5.HDF5Buffer;
import com.ebremer.beakgraph.core.lib.VByte;
import io.jhdf.api.WritableGroup;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class FCDWriter implements HDF5Buffer, AutoCloseable {
    private final int blockSize;
    private int stringsInCurrentBlock = 0;
    private String blockString = null;
    private final ByteArrayOutputStream baos;
    private final Path path;
    private long numBlocks = 0;
    private long numEntries = 0;
    private long position = 0;
    private final DataOutputBuffer offsets;

    public FCDWriter(Path path, int blockSize) throws FileNotFoundException {
        this.path = path;
        this.blockSize = blockSize;
        this.baos = new ByteArrayOutputStream();
        this.offsets = new DataOutputBuffer(Path.of("offsets"));
    }
    
    @Override
    public long getNumEntries() {
        return numEntries;
    }
    
    @Override
    public void close() throws Exception {
       offsets.close();
        try (baos) {
            baos.flush();
        }
    }

    public void add(String item) throws IOException {
        numEntries++;  
        if (stringsInCurrentBlock == 0) {
            byte[] bb = item.getBytes(StandardCharsets.UTF_8);
            offsets.writeLong(position);            
            baos.write(bb);
            position = position + bb.length;            
            baos.write(0);
            position++;
            blockString = item;
            stringsInCurrentBlock = 1;
        } else {
            int prefixLength = commonPrefixLength(blockString, item);
            int cc = VByte.encode(baos, prefixLength);
            position = position + cc;
            byte[] bb = item.substring(prefixLength).getBytes(StandardCharsets.UTF_8);
            baos.write(bb);
            position = position + bb.length;
            baos.write(0);
            position++;
            stringsInCurrentBlock++;
            if (stringsInCurrentBlock == blockSize) {
                stringsInCurrentBlock = 0;
                numBlocks++;                
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
    public String toString() {
        byte[] ha = baos.toByteArray();
        StringBuilder sb = new StringBuilder();
        for (int c=0; c<ha.length;c++) {
            sb.append(ha[c]).append("\n");
        }    
        return sb.toString();
    }

    @Override
    public void Add(WritableGroup group) {
        WritableGroup strings = group.putGroup(path.toString());
        strings.putAttribute("blockSize", blockSize);
        long validBlocks = (stringsInCurrentBlock == 0 && numEntries > 0) ? numBlocks : numBlocks + 1;
        if (numEntries == 0) validBlocks = 0; // Handle empty case
        strings.putAttribute("numBlocks", validBlocks);
        strings.putAttribute("numEntries", numEntries);
        strings.putDataset("stringbuffer", baos.toByteArray());
        offsets.Add(strings);
    }   
}
