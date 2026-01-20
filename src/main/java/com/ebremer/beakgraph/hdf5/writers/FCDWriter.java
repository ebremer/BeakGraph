package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.hdf5.DataOutputBuffer;
import com.ebremer.beakgraph.hdf5.HDF5Buffer;
import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.utils.StringUtils;
import io.jhdf.api.WritableGroup;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;

public class FCDWriter implements HDF5Buffer, AutoCloseable {
    private final int blockSize;
    private static final int COMPRESSION_THRESHOLD = 64;
    private int stringsInCurrentBlock = 0;
    private String prevString = null;
    private final ByteArrayOutputStream baos;
    private final Path path;
    private long numBlocks = 0;
    private long numEntries = 0;
    private long position = 0;
    private final DataOutputBuffer offsets;
    public final String ID = UUID.randomUUID().toString();
    private final BitPackedUnSignedLongBuffer compressed = new BitPackedUnSignedLongBuffer(Path.of("compressed"), null, 0, 1);
    private final StringUtils su = new StringUtils();

    public FCDWriter(Path path, int blockSize) throws FileNotFoundException {
        this.path = path;
        this.blockSize = blockSize;
        this.baos = new ByteArrayOutputStream();
        this.offsets = new DataOutputBuffer(Path.of("offsets"));
    }

    private void writeFragment(byte[] data) throws IOException {
        boolean shouldCompress = data.length >= COMPRESSION_THRESHOLD;
        byte[] finalData;

        if (shouldCompress) {
            // Compress the UTF-8 bytes
            String temp = new String(data, StandardCharsets.UTF_8);
            finalData = su.compress(temp);
            compressed.writeLong(1);
        } else {
            // Keep as raw UTF-8
            finalData = data;
            compressed.writeLong(0);
        }

        // Always write the length of the payload (whether compressed or raw)
        int lenEnc = VByte.encode(baos, finalData.length);
        position += lenEnc;
        baos.write(finalData);
        position += finalData.length;
    }

    public void add(String item) throws IOException {
        numEntries++;
        if (stringsInCurrentBlock == 0) {
            offsets.writeLong(position);
            writeFragment(item.getBytes(StandardCharsets.UTF_8));
            prevString = item;
            stringsInCurrentBlock = 1;
        } else {
            int prefixLength = commonPrefixLength(prevString, item);
            
            // Write Prefix Length
            int cc = VByte.encode(baos, prefixLength);
            position += cc;

            // Write Suffix (Compressed or Raw)
            String suffix = item.substring(prefixLength);
            writeFragment(suffix.getBytes(StandardCharsets.UTF_8));

            prevString = item;
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
            if (s1.charAt(i) != s2.charAt(i)) return i;
        }
        return minLength;
    }

    @Override public long getNumEntries() { return numEntries; }
    @Override public Path getName() { return path; }

    @Override
    public void close() throws Exception {
        offsets.close();
        baos.close();       
    }

    @Override
    public void Add(WritableGroup group) {
        WritableGroup strings = group.putGroup(path.toString());
        strings.putAttribute("blockSize", blockSize);
        long validBlocks = (stringsInCurrentBlock == 0 && numEntries > 0) ? numBlocks : numBlocks + 1;
        strings.putAttribute("numBlocks", (numEntries == 0) ? 0 : validBlocks);
        strings.putAttribute("numEntries", numEntries);
        strings.putAttribute("compression_threshold", COMPRESSION_THRESHOLD);        
        strings.putDataset("stringbuffer", baos.toByteArray());
        offsets.Add(strings);
        compressed.prepareForReading();
        compressed.Add(strings);
    }
}
