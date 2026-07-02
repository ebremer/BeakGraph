package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.utils.StringUtils;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.charset.StandardCharsets;

/**
 * Reader for a front-coded string dictionary.
 * <p>
 * Safe for concurrent reads: it never shares mutable position state (every read
 * is absolute through {@link RandomAccessBytes}, with long offsets - so string
 * buffers past 2 GiB are addressable), and the Zstd decompressor - which
 * airlift does not allow sharing across threads - is held per thread.
 */
public class FCDReader {
    private final RandomAccessBytes buffer;
    private final RandomAccessBytes offsets;
    private final BitPackedUnSignedLongBuffer compressed;
    private final long blockSize;
    private final long numEntries;
    private final long numBlocks;
    private final ThreadLocal<StringUtils> su = ThreadLocal.withInitial(StringUtils::new);

    public FCDReader(Group strings) {
        ContiguousDataset stringbuffer = (ContiguousDataset) strings.getChild("stringbuffer");
        ContiguousDataset off = (ContiguousDataset) strings.getChild("offsets");
        ContiguousDataset xcompressed = (ContiguousDataset) strings.getChild("compressed");
        this.buffer = DatasetBytes.of(stringbuffer);
        this.offsets = DatasetBytes.of(off);
        this.compressed = BitPackedUnSignedLongBuffer.readView(
            DatasetBytes.of(xcompressed),
            (long) xcompressed.getAttribute("numEntries").getData(),
            (int) xcompressed.getAttribute("width").getData()
        );
        this.blockSize = (int) strings.getAttribute("blockSize").getData();
        this.numEntries = (long) strings.getAttribute("numEntries").getData();
        this.numBlocks = (long) strings.getAttribute("numBlocks").getData();
    }

    /** A decoded fragment plus the absolute position just past it. */
    private record Fragment(String value, long nextPos) {}

    /**
     * Reads the length-prefixed fragment that starts at absolute byte position {@code pos}.
     * Absolute reads only, so concurrent readers never disturb each other.
     */
    private Fragment readFragment(long pos, long entryIndex) {
        VByte.DecodeResult lenR = VByte.decodeAt(buffer, pos);
        int dataLen = (int) lenR.value;
        long p = lenR.nextOffset;
        byte[] data = new byte[dataLen];
        buffer.get(p, data, 0, dataLen); // absolute bulk read
        p += dataLen;
        boolean isCompressed = compressed.get(entryIndex) == 1;
        String value = isCompressed ? su.get().decompress(data) : new String(data, StandardCharsets.UTF_8);
        return new Fragment(value, p);
    }

    public String get(long n) {
        if (n < 0 || n >= numEntries) throw new IndexOutOfBoundsException();

        long block = n / blockSize;
        long pos = offsets.getLong(block * 8L);

        // The first string in the block is always at index (block * blockSize).
        Fragment frag = readFragment(pos, block * blockSize);
        String current = frag.value();
        pos = frag.nextPos();

        long offsetInBlock = n % blockSize;
        for (long i = 1; i <= offsetInBlock; i++) {
            VByte.DecodeResult pl = VByte.decodeAt(buffer, pos);
            int prefixLen = (int) pl.value;
            pos = pl.nextOffset;
            // Suffix fragment is at index (block * blockSize + i).
            Fragment suffix = readFragment(pos, block * blockSize + i);
            current = current.substring(0, prefixLen) + suffix.value();
            pos = suffix.nextPos();
        }
        return current;
    }

    // NOTE: an unused locate(String) lived here that binary-searched blocks by
    // String.compareTo. It was removed in the dead-code sweep - and must not be
    // reintroduced as-was: the value-ordered dictionaries (strings holding typed
    // literals) are NOT ordered by raw string comparison, so its block search was
    // wrong for them. Term lookup goes through MultiTypeDictionaryReader.search,
    // which compares with the same NodeComparator the writer sorted with.
}
