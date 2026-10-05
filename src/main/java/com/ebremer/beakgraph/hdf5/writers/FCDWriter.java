package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import static com.ebremer.beakgraph.Params.COMPRESSION_THRESHOLD;
import com.ebremer.beakgraph.hdf5.DataOutputBuffer;
import com.ebremer.beakgraph.hdf5.HDF5Buffer;
import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.DictionarySinks;
import com.ebremer.beakgraph.utils.StringUtils;
import io.jhdf.api.WritableGroup;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class FCDWriter implements HDF5Buffer, AutoCloseable, DictionarySinks.StringSink {
    private final int blockSize;
    private int stringsInCurrentBlock = 0;
    private String prevString = null;
    private final ByteArrayOutputStream baos;
    private final Path path;
    private long numBlocks = 0;
    private long numEntries = 0;
    private long position = 0;
    private final DataOutputBuffer offsets;
    private final BitPackedUnSignedLongBuffer compressed = new BitPackedUnSignedLongBuffer(Path.of("compressed"), 1);
    private final StringUtils su = new StringUtils();

    public FCDWriter(Path path, int blockSize) {
        if (blockSize < 2) {
            // With blockSize 1 the add() block-head branch never closes a block:
            // the offsets dataset would hold one entry total and FCDReader.get()
            // for any later block reads past it. Fail construction loudly rather
            // than write an unreadable dictionary.
            throw new IllegalArgumentException("FCD blockSize must be >= 2, got " + blockSize);
        }
        this.path = path;
        this.blockSize = blockSize;
        this.baos = new ByteArrayOutputStream();
        this.offsets = new DataOutputBuffer(Path.of("offsets"));
    }

    private void writeFragment(byte[] data) throws IOException {
        boolean shouldCompress = data.length >= COMPRESSION_THRESHOLD;
        byte[] finalData;

        if (shouldCompress) {
            // Compress the UTF-8 bytes as they are (BG-105).
            finalData = su.compress(data);
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
        int i = 0;
        while (i < minLength && s1.charAt(i) == s2.charAt(i)) i++;
        // Never split a UTF-16 surrogate pair: the suffix is encoded to UTF-8 on its
        // own, and a suffix starting with an unpaired low surrogate encodes as '?',
        // silently corrupting the stored string. Back off so the whole pair stays
        // in the suffix.
        if (i > 0 && Character.isHighSurrogate(s1.charAt(i - 1))) i--;
        return i;
    }

    @Override public long getNumEntries() { return numEntries; }
    @Override public Path getName() { return path; }

    /** In-memory throughout; closing cannot fail (BG-89). */
    @Override
    public void close() {
        offsets.close();
    }

    @Override
    public void add(WritableGroup group) {
        WritableGroup strings = group.putGroup(path.toString());
        strings.putAttribute(Params.BLOCK_SIZE, blockSize);
        long validBlocks = (stringsInCurrentBlock == 0 && numEntries > 0) ? numBlocks : numBlocks + 1;
        strings.putAttribute(Params.NUM_BLOCKS, (numEntries == 0) ? 0 : validBlocks);
        strings.putAttribute(Params.NUM_ENTRIES, numEntries);
        strings.putAttribute("compression_threshold", COMPRESSION_THRESHOLD);        
        strings.putDataset("stringbuffer", baos.toByteArray());
        offsets.add(strings);
        compressed.prepareForReading();
        compressed.add(strings);
    }
}
