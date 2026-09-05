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
    /**
     * Decoded-block cache capacity (blocks, per FCD section). Front-coding means
     * every {@code get(n)} must decode from its block's head - an average of
     * blockSize/2 fragment decodes (VByte + copy + possible zstd + string build)
     * per lookup - and both binary searches and result materialization revisit
     * the same blocks constantly. Caching the decoded block makes those revisits
     * an array index. Sized via -Dbeakgraph.fcd.cache.blocks.
     */
    private static final long CACHE_BLOCKS = Long.getLong("beakgraph.fcd.cache.blocks", 4096L);

    private final RandomAccessBytes buffer;
    private final RandomAccessBytes offsets;
    private final BitPackedUnSignedLongBuffer compressed;
    private final long blockSize;
    private final long numEntries;
    private final long numBlocks;
    private final ThreadLocal<StringUtils> su = ThreadLocal.withInitial(StringUtils::new);
    private final com.github.benmanes.caffeine.cache.Cache<Long, String[]> blockCache =
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                    .maximumSize(CACHE_BLOCKS)
                    .build();

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
        checkOffset(pos, "fragment", entryIndex);
        VByte.DecodeResult lenR = VByte.decodeAt(buffer, pos);
        long p = lenR.nextOffset;
        // Validate the on-disk length BEFORE allocating from it: a corrupt or
        // hostile file's 64-bit VByte truncated to int gave new byte[negative]
        // or a 2 GB allocation from one dictionary lookup, ahead of the bulk
        // read's own bounds check. The buffer size is the only honest bound.
        if (lenR.value < 0 || lenR.value > buffer.size() - p) {
            throw new IllegalStateException("FCD corrupt: fragment length " + lenR.value
                    + " at stringbuffer offset " + pos + " exceeds the " + buffer.size()
                    + "-byte buffer (entry " + entryIndex + ")");
        }
        int dataLen = (int) lenR.value;
        byte[] data = new byte[dataLen];
        buffer.get(p, data, 0, dataLen); // absolute bulk read
        p += dataLen;
        boolean isCompressed = compressed.get(entryIndex) == 1;
        String value = isCompressed ? su.get().decompress(data) : new String(data, StandardCharsets.UTF_8);
        return new Fragment(value, p);
    }

    /** Number of strings stored (valid {@link #get} indices are {@code 0..n-1}). */
    public long getNumEntries() {
        return numEntries;
    }

    /** Rejects a block or fragment position outside the string buffer with a corruption error, not a raw IOOBE or OOM. */
    private void checkOffset(long pos, String what, long entryIndex) {
        if (pos < 0 || pos >= buffer.size()) {
            throw new IllegalStateException("FCD corrupt: " + what + " offset " + pos
                    + " outside the " + buffer.size() + "-byte buffer (entry " + entryIndex + ")");
        }
    }

    public String get(long n) {
        if (n < 0 || n >= numEntries) throw new IndexOutOfBoundsException();
        long block = n / blockSize;
        return blockCache.get(block, this::decodeBlock)[(int) (n % blockSize)];
    }

    /**
     * Decodes every string of one front-coded block. The running value is built
     * in a reused StringBuilder ({@code setLength(prefixLen)} + append) instead
     * of the former per-entry {@code substring(0, prefixLen) + suffix}, which
     * allocated two intermediate strings per step.
     */
    private String[] decodeBlock(long block) {
        long firstEntry = block * blockSize;
        int entries = (int) Math.min(blockSize, numEntries - firstEntry);
        String[] out = new String[entries];

        long pos = offsets.getLong(block * 8L);
        // The first string in the block is always stored in full.
        Fragment frag = readFragment(pos, firstEntry);
        StringBuilder current = new StringBuilder(frag.value());
        out[0] = frag.value();
        pos = frag.nextPos();

        for (int i = 1; i < entries; i++) {
            checkOffset(pos, "prefix", firstEntry + i);
            VByte.DecodeResult pl = VByte.decodeAt(buffer, pos);
            // A shared prefix can only be as long as the previous string;
            // StringBuilder.setLength would silently NUL-pad a longer value
            // (or throw on a negative one) instead of flagging the corruption.
            if (pl.value < 0 || pl.value > current.length()) {
                throw new IllegalStateException("FCD corrupt: prefix length " + pl.value
                        + " exceeds the previous string's " + current.length()
                        + " chars (entry " + (firstEntry + i) + ")");
            }
            int prefixLen = (int) pl.value;
            pos = pl.nextOffset;
            // Suffix fragment is at index (firstEntry + i).
            Fragment suffix = readFragment(pos, firstEntry + i);
            current.setLength(prefixLen);
            current.append(suffix.value());
            out[i] = current.toString();
            pos = suffix.nextPos();
        }
        return out;
    }

    // NOTE: an unused locate(String) lived here that binary-searched blocks by
    // String.compareTo. It was removed in the dead-code sweep - and must not be
    // reintroduced as-was: the value-ordered dictionaries (strings holding typed
    // literals) are NOT ordered by raw string comparison, so its block search was
    // wrong for them. Term lookup goes through MultiTypeDictionaryReader.search,
    // which compares with the same NodeComparator the writer sorted with.
}
