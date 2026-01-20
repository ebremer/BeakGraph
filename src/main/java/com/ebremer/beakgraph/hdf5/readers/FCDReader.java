package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.utils.StringUtils;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class FCDReader {
    private final ByteBuffer buffer;
    private final ByteBuffer offsets;
    private final BitPackedUnSignedLongBuffer compressed;
    private final long blockSize;
    private final long numEntries;
    private final long numBlocks;
    private final StringUtils su = new StringUtils();

    public FCDReader(Group strings) {
        ContiguousDataset stringbuffer = (ContiguousDataset) strings.getChild("stringbuffer");
        ContiguousDataset off = (ContiguousDataset) strings.getChild("offsets");
        ContiguousDataset xcompressed = (ContiguousDataset) strings.getChild("compressed");
        this.buffer = stringbuffer.getBuffer();
        this.offsets = off.getBuffer().order(ByteOrder.BIG_ENDIAN);
        this.compressed = new BitPackedUnSignedLongBuffer(
            null, xcompressed.getBuffer(), 
            (long) xcompressed.getAttribute("numEntries").getData(), 
            (int) xcompressed.getAttribute("width").getData()
        );
        this.blockSize = (int) strings.getAttribute("blockSize").getData();
        this.numEntries = (long) strings.getAttribute("numEntries").getData();
        this.numBlocks = (long) strings.getAttribute("numBlocks").getData();
    }

    private String readFragment(int entryIndex) {
        int dataLen = (int) VByte.decodeSingle(buffer);
        byte[] data = new byte[dataLen];
        buffer.get(data);
        boolean isCompressed = compressed.get(entryIndex) == 1;
        if (isCompressed) {
            return su.decompress(data);
        } else {
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    public String get(long n) {
        if (n < 0 || n >= numEntries) throw new IndexOutOfBoundsException();

        long block = n / blockSize;
        long address = offsets.getLong((int) block * 8);
        buffer.position((int) address);

        // The first string in the block is always at index (block * blockSize)
        String current = readFragment((int) (block * blockSize));
        
        long offsetInBlock = n % blockSize;
        for (long i = 1; i <= offsetInBlock; i++) {
            int prefixLen = (int) VByte.decodeSingle(buffer);
            // Suffix fragment is at index (block * blockSize + i)
            String suffix = readFragment((int) (block * blockSize + i));
            current = current.substring(0, prefixLen) + suffix;
        }
        return current;
    }

    public long locate(String x) {
        if (numEntries == 0) return -1;
        // Binary search for the correct block
        long low = 0;
        long high = numBlocks - 1;
        while (low <= high) {
            long mid = low + (high - low) / 2;
            String midHeader = get(mid * blockSize);
            int cmp = x.compareTo(midHeader);
            if (cmp < 0) high = mid - 1;
            else if (cmp > 0) low = mid + 1;
            else return mid * blockSize;
        }
        long candidateBlock = high;
        if (candidateBlock < 0) return -1;
        // Linear search inside block
        long blockStart = candidateBlock * blockSize;
        long blockEnd = Math.min(blockStart + blockSize, numEntries);
        long address = offsets.getLong((int) candidateBlock * 8);
        buffer.position((int) address);        
        String current = readFragment((int) blockStart);
        if (current.equals(x)) return blockStart;
        for (long i = blockStart + 1; i < blockEnd; i++) {
            int prefixLen = (int) VByte.decodeSingle(buffer);
            String suffix = readFragment((int) i);
            current = current.substring(0, prefixLen) + suffix;
            if (current.equals(x)) return i;
        }
        return -1;
    }
}
