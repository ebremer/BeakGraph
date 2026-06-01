package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.utils.StringUtils;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Reader for a front-coded string dictionary.
 * <p>
 * Safe for concurrent reads: it never mutates the shared {@code stringbuffer}'s position
 * (every read is absolute, threaded through a local cursor), and the Zstd decompressor -
 * which airlift does not allow sharing across threads - is held per thread.
 */
public class FCDReader {
    private final ByteBuffer buffer;
    private final ByteBuffer offsets;
    private final BitPackedUnSignedLongBuffer compressed;
    private final long blockSize;
    private final long numEntries;
    private final long numBlocks;
    private final ThreadLocal<StringUtils> su = ThreadLocal.withInitial(StringUtils::new);

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

    /** A decoded fragment plus the absolute position just past it. */
    private record Fragment(String value, int nextPos) {}

    /**
     * Reads the length-prefixed fragment that starts at absolute byte position {@code pos}.
     * Uses absolute reads only, so it does not disturb the shared buffer's position.
     */
    private Fragment readFragment(int pos, int entryIndex) {
        VByte.DecodeResult lenR = VByte.decodeAt(buffer, pos);
        int dataLen = (int) lenR.value;
        int p = lenR.nextOffset;
        byte[] data = new byte[dataLen];
        buffer.get(p, data); // absolute bulk read; does not move the buffer position
        p += dataLen;
        boolean isCompressed = compressed.get(entryIndex) == 1;
        String value = isCompressed ? su.get().decompress(data) : new String(data, StandardCharsets.UTF_8);
        return new Fragment(value, p);
    }

    public String get(long n) {
        if (n < 0 || n >= numEntries) throw new IndexOutOfBoundsException();

        long block = n / blockSize;
        int pos = (int) offsets.getLong((int) block * 8);

        // The first string in the block is always at index (block * blockSize).
        Fragment frag = readFragment(pos, (int) (block * blockSize));
        String current = frag.value();
        pos = frag.nextPos();

        long offsetInBlock = n % blockSize;
        for (long i = 1; i <= offsetInBlock; i++) {
            VByte.DecodeResult pl = VByte.decodeAt(buffer, pos);
            int prefixLen = (int) pl.value;
            pos = pl.nextOffset;
            // Suffix fragment is at index (block * blockSize + i).
            Fragment suffix = readFragment(pos, (int) (block * blockSize + i));
            current = current.substring(0, prefixLen) + suffix.value();
            pos = suffix.nextPos();
        }
        return current;
    }

    public long locate(String x) {
        if (numEntries == 0) return -1;
        // Binary search for the correct block.
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
        // Linear search inside the block.
        long blockStart = candidateBlock * blockSize;
        long blockEnd = Math.min(blockStart + blockSize, numEntries);
        int pos = (int) offsets.getLong((int) candidateBlock * 8);
        Fragment frag = readFragment(pos, (int) blockStart);
        String current = frag.value();
        pos = frag.nextPos();
        if (current.equals(x)) return blockStart;
        for (long i = blockStart + 1; i < blockEnd; i++) {
            VByte.DecodeResult pl = VByte.decodeAt(buffer, pos);
            int prefixLen = (int) pl.value;
            pos = pl.nextOffset;
            Fragment suffix = readFragment(pos, (int) i);
            current = current.substring(0, prefixLen) + suffix.value();
            pos = suffix.nextPos();
            if (current.equals(x)) return i;
        }
        return -1;
    }
}
