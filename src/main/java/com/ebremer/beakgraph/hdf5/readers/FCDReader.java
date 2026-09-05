package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.utils.StringUtils;
import io.jhdf.api.Group;
import java.nio.charset.StandardCharsets;

/**
 * Reader for a front-coded string dictionary.
 * <p>
 * Safe for concurrent reads: it never shares mutable position state (every read
 * is absolute through {@link RandomAccessBytes}, with long offsets - so string
 * buffers past 2 GiB are addressable), and the Zstd decompressor - which
 * airlift does not allow sharing across threads - is held per thread, ONCE
 * per JVM thread for every reader (it holds no reader state; a per-reader
 * ThreadLocal materialised one ~140 KiB Java decoder per (reader, thread)
 * pair and kept it alive on the thread after the store closed; BG-169).
 */
public class FCDReader {
    /**
     * Decoded-block cache capacity (per FCD section), in blocks of ordinary
     * strings. Front-coding means every {@code get(n)} must decode from its
     * block's head - an average of blockSize/2 fragment decodes (VByte + copy +
     * possible zstd + string build) per lookup - and both binary searches and
     * result materialization revisit the same blocks constantly. Caching the
     * decoded block makes those revisits an array index. Sized via
     * -Dbeakgraph.fcd.cache.blocks. The bound is by WEIGHT, not count: a
     * string costs 1 plus 1 per 256 chars (as the node-table and search caches
     * charge), so a section of large literals (WKT polygons, long text) keeps
     * proportionally fewer blocks instead of pinning up to 65,536 fully
     * materialised strings per open store (BG-78).
     */
    private static final long CACHE_BLOCKS = Long.getLong("beakgraph.fcd.cache.blocks", 4096L);
    /** Weight of one block of ordinary strings: the cache holds CACHE_BLOCKS of them. */
    static final int ORDINARY_BLOCK_WEIGHT = 16;

    private final RandomAccessBytes buffer;
    private final RandomAccessBytes offsets;
    private final BitPackedUnSignedLongBuffer compressed;
    private final long blockSize;
    private final long numEntries;
    private final long numBlocks;
    private static final ThreadLocal<StringUtils> SU = ThreadLocal.withInitial(StringUtils::new);
    private final com.github.benmanes.caffeine.cache.Cache<Long, String[]> blockCache =
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                    .maximumWeight(CACHE_BLOCKS * ORDINARY_BLOCK_WEIGHT)
                    .weigher((Long block, String[] strs) -> weightOf(strs))
                    .build();

    /** Cache weight of a decoded block: 1 per string plus 1 per 256 chars of it (mirrors SimpleNodeTable.weightOf). */
    static int weightOf(String[] strs) {
        long w = 0;
        for (String s : strs) {
            w += 1 + (s.length() >>> 8);
        }
        return (int) Math.min(w, Integer.MAX_VALUE);
    }

    /** Blocks currently cached, after pending evictions are applied (tests). */
    long cachedBlocks() {
        blockCache.cleanUp();
        return blockCache.estimatedSize();
    }

    public FCDReader(Group strings) {
        this.buffer = DatasetBytes.of(HdfProfile.requireContiguous(strings, "stringbuffer"));
        this.offsets = DatasetBytes.of(HdfProfile.requireContiguous(strings, "offsets"));
        this.compressed = HdfProfile.requirePacked(strings, "compressed");
        this.blockSize = HdfProfile.longAttr(strings, Params.BLOCK_SIZE);
        this.numEntries = HdfProfile.longAttr(strings, Params.NUM_ENTRIES);
        this.numBlocks = HdfProfile.longAttr(strings, Params.NUM_BLOCKS);
        // The attributes must describe each other and the datasets: a file
        // whose blockSize was 0 divided by zero on every lookup, and one whose
        // offsets or flags were short read past their end - after the open had
        // reported success (BG-81).
        String where = "FCD group '" + strings.getPath() + "'";
        if (blockSize < 1) {
            throw new IllegalStateException(where + " declares blockSize " + blockSize + " (must be at least 1)");
        }
        if (numEntries < 0) {
            throw new IllegalStateException(where + " declares a negative numEntries " + numEntries);
        }
        long expectedBlocks = (numEntries + blockSize - 1) / blockSize;
        if (numBlocks != expectedBlocks) {
            throw new IllegalStateException(where + " declares " + numBlocks + " blocks but " + numEntries
                    + " entries in blocks of " + blockSize + " make " + expectedBlocks);
        }
        if (offsets.size() < numBlocks * 8L) {
            throw new IllegalStateException(where + ": the offsets dataset holds " + offsets.size()
                    + " bytes, fewer than the " + (numBlocks * 8L) + " its " + numBlocks + " blocks need");
        }
        if (compressed.getNumEntries() < numEntries) {
            throw new IllegalStateException(where + ": the compressed flags cover " + compressed.getNumEntries()
                    + " of " + numEntries + " entries");
        }
    }

    // Fragment bytes are read into a per-thread scratch array (grown on
    // demand) and appended to the running string straight from it: the former
    // byte[] + String + Fragment per entry cost three copies and four objects
    // for every string of every decoded block (BG-256).
    private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[256]);

    /**
     * Appends the length-prefixed fragment that starts at absolute byte
     * position {@code pos} to {@code into} and returns the position just past
     * it. Absolute reads only, so concurrent readers never disturb each other.
     */
    private long appendFragment(long pos, long entryIndex, StringBuilder into) {
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
        byte[] data = SCRATCH.get();
        if (data.length < dataLen) {
            data = new byte[Math.max(dataLen, Math.min(data.length * 2, Integer.MAX_VALUE - 8))];
            SCRATCH.set(data);
        }
        buffer.get(p, data, 0, dataLen); // absolute bulk read
        if (compressed.get(entryIndex) == 1) {
            into.append(SU.get().decompress(data, 0, dataLen));
        } else {
            appendUtf8(into, data, dataLen);
        }
        return p + dataLen;
    }

    /** ASCII bytes go straight in as chars; anything else decodes as UTF-8 first. */
    private static void appendUtf8(StringBuilder into, byte[] data, int len) {
        int i = 0;
        while (i < len && data[i] >= 0) {
            i++;
        }
        if (i < len) {
            into.append(new String(data, 0, len, StandardCharsets.UTF_8));
            return;
        }
        into.ensureCapacity(into.length() + len);
        for (int k = 0; k < len; k++) {
            into.append((char) data[k]);
        }
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
        StringBuilder current = new StringBuilder();
        pos = appendFragment(pos, firstEntry, current);
        out[0] = current.toString();

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
            current.setLength((int) pl.value);
            // Suffix fragment is at index (firstEntry + i).
            pos = appendFragment(pl.nextOffset, firstEntry + i, current);
            out[i] = current.toString();
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
