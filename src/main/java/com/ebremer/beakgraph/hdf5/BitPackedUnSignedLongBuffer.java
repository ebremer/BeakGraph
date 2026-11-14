package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.io.MemorySegmentBytes;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.io.ByteBufferBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.utils.UTIL;
import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

/**
 * A buffer that supports writing and reading bit-packed unsigned integers/longs.
 *
 * <p>A read view is of one of two forms. The first is one width for the whole
 * sequence ({@link #readView}): bitmaps, rank directories and the dictionary's
 * arrays. The second is block-structured ({@link #readBlocked},
 * SPECIFICATIONS.md §5.1a, written by {@link BlockedEncoder}): the index id
 * sequences since format v6, each block with a base and a width of its own,
 * kept in a directory on the heap. Both answer the same calls, so nothing that
 * reads a sequence needs to know which it holds.
 *
 * <p>All read paths go through {@link RandomAccessBytes} with long offsets, so
 * a read view is no longer capped at 2 GiB by ByteBuffer's int indexing - the
 * backing bytes may be a ByteBuffer view of the mapped store, its FFM segment, or (for
 * the write side after {@link #prepareForReading()}) the internally
 * accumulated bytes.
 */
public class BitPackedUnSignedLongBuffer implements DictionarySinks.LongSink {
    // Write side: the packed bytes once prepareForReading() ran (empty before);
    // null for a read view, whose write API is unavailable.
    private ByteBuffer buffer;
    private RandomAccessBytes data;
    // data.size(), cached: the bounds check in get() is on the same hot path
    // as the word fetch and would otherwise be a second megamorphic call (BG-260).
    private long dataSize;
    private final int bitWidth;

    // Writing State. The packed bytes accumulate in memory (internalStream) and
    // become readable through prepareForReading(); the former "write into a
    // caller-supplied ByteBuffer" mode and the sequential-cursor reads had no
    // callers (BG-310).
    private long writeAccumulator;
    private int writeAccumulatorCount;
    private final ByteArrayOutputStream internalStream;
    // Packed bytes are staged here and handed to the stream a chunk at a time:
    // ByteArrayOutputStream.write(int) is synchronized, and every S/B/SB/BB
    // entry of every index level paid that lock per byte (BG-250).
    private static final int STAGE_BYTES = 1 << 16;
    private final byte[] stage;
    private int staged;

    private final Path path;
    private long numEntries;

    // The directory of a block-structured read view, on the heap; all null (and the
    // shift 0) for a buffer of one width. blockOffset has one entry more than there
    // are blocks: where the data would end.
    private final long[] blockOffset;
    private final long[] blockBase;
    private final byte[] blockWidth;
    private final int blockShift;
    private final long blockMask;

    /**
     * A write-side buffer of {@code bitWidth}-bit values, stored in HDF5 as the
     * dataset named {@code path} by {@link #add}; readable through
     * {@link #get} after {@link #prepareForReading()}.
     */
    public BitPackedUnSignedLongBuffer(Path path, int bitWidth) {
        this.path = path;
        checkWidth(bitWidth);
        this.bitWidth = bitWidth;
        this.internalStream = new ByteArrayOutputStream();
        this.stage = new byte[STAGE_BYTES];
        this.buffer = ByteBuffer.allocate(0);
        this.data = new ByteBufferBytes(this.buffer);
        this.dataSize = 0;
        this.numEntries = 0;
        this.blockOffset = null;
        this.blockBase = null;
        this.blockWidth = null;
        this.blockShift = 0;
        this.blockMask = 0;
    }

    private BitPackedUnSignedLongBuffer(RandomAccessBytes data, long numEntries, int bitWidth) {
        this.path = null;
        checkWidth(bitWidth);
        // The attributes are the file's word, not the truth: a corrupted or
        // foreign numEntries/width used to be stored verbatim and surfaced as
        // a BufferUnderflow/IndexOutOfBounds at query time, after -verify had
        // mapped the dataset and said OK (BG-345). Every reader (indexes,
        // dictionaries, FCD blocks) constructs through here, so the check
        // fails the open instead.
        long needed;
        try {
            needed = (numEntries < 0) ? Long.MAX_VALUE : (Math.multiplyExact(numEntries, (long) bitWidth) + 7) >>> 3;
        } catch (ArithmeticException overflow) {
            needed = Long.MAX_VALUE;
        }
        if (numEntries < 0 || needed > data.size()) {
            throw new IllegalStateException("bit-packed buffer declares " + numEntries + " x " + bitWidth
                    + "-bit entries (" + (numEntries < 0 ? "negative" : needed + " bytes") + ") but only "
                    + data.size() + " bytes are stored");
        }
        this.bitWidth = bitWidth;
        this.buffer = null; // pure read view: the write-side API is unavailable
        this.internalStream = null;
        this.stage = null;
        this.data = data;
        this.dataSize = data.size();
        this.numEntries = numEntries;
        this.blockOffset = null;
        this.blockBase = null;
        this.blockWidth = null;
        this.blockShift = 0;
        this.blockMask = 0;
    }

    /** The block-structured read view: see {@link #readBlocked}. */
    private BitPackedUnSignedLongBuffer(RandomAccessBytes data, RandomAccessBytes directory, long numEntries,
            int blockEntries, int maxWidth) {
        this.path = null;
        this.buffer = null;
        this.internalStream = null;
        this.stage = null;
        // As for a single-width view (BG-345): the attributes and the directory are the
        // file's word, so what they declare is held against the bytes that are there
        // before anything is read through them.
        if (numEntries < 1 || blockEntries < 1 || Integer.bitCount(blockEntries) != 1) {
            throw new IllegalStateException("block-structured buffer declares " + numEntries + " entries in blocks of "
                    + blockEntries + " (at least one entry, and a power of two per block)");
        }
        long blocks = (numEntries - 1) / blockEntries + 1;
        if (blocks > Integer.MAX_VALUE - 8 || directory.size() != blocks * BlockedEncoder.DIRECTORY_ENTRY_BYTES) {
            throw new IllegalStateException("block-structured buffer declares " + numEntries + " entries in blocks of "
                    + blockEntries + " (" + blocks + " directory entries of " + BlockedEncoder.DIRECTORY_ENTRY_BYTES
                    + " bytes) but only " + directory.size() + " bytes are stored in its directory");
        }
        int n = (int) blocks;
        byte[] raw = new byte[n * BlockedEncoder.DIRECTORY_ENTRY_BYTES];
        directory.get(0, raw, 0, raw.length);
        long[] offset = new long[n + 1];
        long[] base = new long[n];
        byte[] width = new byte[n];
        long at = 0;
        long remaining = numEntries;
        for (int b = 0; b < n; b++) {
            int e = b * BlockedEncoder.DIRECTORY_ENTRY_BYTES;
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (raw[e + i] & 0xFFL);
            }
            int w = raw[e + 8] & 0xFF;
            if ((w > BlockedEncoder.MAX_PACKED_WIDTH && w != 64) || w > maxWidth) {
                throw new IllegalStateException("block-structured buffer: block " + b + " declares a width of " + w
                        + " bits (0.." + BlockedEncoder.MAX_PACKED_WIDTH + " or 64, and at most the buffer's " + maxWidth + ")");
            }
            long count = Math.min(blockEntries, remaining);
            offset[b] = at;
            base[b] = v;
            width[b] = (byte) w;
            at += BlockedEncoder.blockBytes(count, w);
            remaining -= count;
        }
        offset[n] = at;
        if (at + BlockedEncoder.TAIL_BYTES > data.size()) {
            throw new IllegalStateException("block-structured buffer declares " + numEntries + " entries in " + n
                    + " blocks (" + (at + BlockedEncoder.TAIL_BYTES) + " bytes) but only " + data.size() + " bytes are stored");
        }
        this.bitWidth = maxWidth;
        this.data = data;
        this.dataSize = data.size();
        this.numEntries = numEntries;
        this.blockOffset = offset;
        this.blockBase = base;
        this.blockWidth = width;
        this.blockShift = Integer.numberOfTrailingZeros(blockEntries);
        this.blockMask = blockEntries - 1L;
    }

    /**
     * A read-only view over already-packed bytes: how the HDF5 readers (via
     * {@code DatasetBytes.of}) and the spill readers construct buffers, with
     * no 2 GiB ceiling. A static factory so the read side stays visibly
     * distinct from the write-side constructor.
     */
    /** Whether the backing bytes are read from a remote store (see {@link RandomAccessBytes#isRemote()}). */
    public boolean isRemote() {
        return data != null && data.isRemote();
    }

    public static BitPackedUnSignedLongBuffer readView(RandomAccessBytes data, long numEntries, int bitWidth) {
        return new BitPackedUnSignedLongBuffer(data, numEntries, bitWidth);
    }

    /**
     * A read-only view over a block-structured sequence (SPECIFICATIONS.md §5.1a):
     * {@code data} holds the blocks, {@code directory} each block's base and width.
     * The directory is read whole, now, and kept on the heap - 17 bytes a block -
     * so that finding an entry's block, and narrowing a search in a sorted run to
     * one block, never touch the file.
     *
     * @param blockEntries entries per block, a power of two
     * @param maxWidth     the widest block's width, as the dataset declares it
     */
    public static BitPackedUnSignedLongBuffer readBlocked(RandomAccessBytes data, RandomAccessBytes directory,
            long numEntries, int blockEntries, int maxWidth) {
        return new BitPackedUnSignedLongBuffer(data, directory, numEntries, blockEntries, maxWidth);
    }

    /** True for a block-structured view ({@link #readBlocked}). */
    public boolean isBlocked() {
        return blockBase != null;
    }

    private static void checkWidth(int bitWidth) {
        // The pack/unpack accumulators (putValue/get/stream) hold a value together with its
        // <=7-bit sub-byte offset in a single 64-bit long. That fits only for width <= 57 (7 + 57 = 64);
        // width 64 is also safe because it is byte-aligned (offset always 0). Widths 58..63 would
        // silently drop high bits on both read and write, so reject them up front rather than corrupt
        // data. Unreachable in practice: width is MinBits(id count) and 57 bits already addresses
        // > 1.4e17 ids.
        boolean supported = (bitWidth >= 1 && bitWidth <= 57) || bitWidth == 64;
        if (!supported) {
            throw new IllegalArgumentException(
                "Unsupported bit width: " + bitWidth + ". Supported: 1..57, or 64 (byte-aligned).");
        }
    }

    // The one RandomAccessBytes call site every id fetch, bitmap probe and
    // binary-search step lands on. Its receiver-type profile is process-wide:
    // a JVM that has read ByteBuffer-backed (ByteBufferBytes), FFM-mapped
    // (MemorySegmentBytes) and remote (ChannelBytes) datasets turns the plain
    // interface call megamorphic, measured at +57% on randomGet and +42% on
    // binarySearch (benchmarks/BitPackedBufferBench, profile=mixed vs mono).
    // RandomAccessBytes is sealed, so each branch is a klass compare plus a
    // direct, inlinable call whatever the site has seen (BG-260).
    private long readLong(long offset) {
        if (data instanceof ByteBufferBytes b) return b.getLong(offset);
        if (data instanceof MemorySegmentBytes m) return m.getLong(offset);
        return data.getLong(offset);
    }

    /** The unsigned byte at {@code offset}; same dispatch as {@link #readLong}. */
    private int readByte(long offset) {
        if (data instanceof ByteBufferBytes b) return b.get(offset) & 0xFF;
        if (data instanceof MemorySegmentBytes m) return m.get(offset) & 0xFF;
        return data.get(offset) & 0xFF;
    }

    // --- QUERY METHODS ---

    public long select1(long rank) {
        // select1 is 1-based: rank 1 = first set bit. rank < 1 (including 0) is not a valid query,
        // so return -1 instead of a misleading index 0 (matches the rank<1 guard in the iterators).
        if (rank < 1) return -1;
        if (bitWidth != 1 || blockBase != null) throw new UnsupportedOperationException("select1 only supported for 1-bit bitmaps");
        long currentRank = 0;
        long maxIndex = numEntries;
        // Clamp fast-path byte range to the bytes that actually hold live bits (numEntries/64 full words),
        // so trailing padding / arena tail bytes can't inflate popcount and skew the rank.
        long fullWords = maxIndex / 64;
        long fullWordBytes = fullWords * 8;
        long safeLimit = Math.min(fullWordBytes, dataSize) - 8;
        long bufferOffset = 0;
        long i = 0;
        // FAST PATH: Iterate over full 64-bit words directly from the backing bytes
        while (bufferOffset <= safeLimit && i < maxIndex) {
            long word = readLong(bufferOffset);
            int pop = Long.bitCount(word);
            if (currentRank + pop >= rank) {
                // The target bit is in this word.
                long needed = rank - currentRank;
                return i + selectInWordSafe(word, needed);
            }
            currentRank += pop;
            i += 64;
            bufferOffset += 8;
        }
        // TAIL PATH: Handle the remaining bits (if any) safely
        // This handles cases where numEntries isn't a multiple of 64
        for (; i < maxIndex; i += 64) {
             long word = getWord64SafeTail(i); // Use existing safe method for the edge
             int pop = Long.bitCount(word);

             if (currentRank + pop >= rank) {
                long needed = rank - currentRank;
                long resultIndex = i + selectInWordSafe(word, needed);
                return (resultIndex < maxIndex) ? resultIndex : -1;
             }
             currentRank += pop;
        }
        return -1;
    }

    /**
     * Finds the index (0-63, from the MSB) of the k-th set bit in a word.
     * Delegates to the shared broadword implementation so this linear select1
     * and HDTBitmapDirectory's accelerated select1 can never disagree.
     */
    private int selectInWordSafe(long word, long k) {
        return UTIL.selectInWord(word, k);
    }

    /** Returned by {@link #nextSetBit} when the word budget ran out before a set bit. */
    public static final long SCAN_EXHAUSTED = -2;

    /**
     * Index of the first set bit at or after {@code fromIndex}, scanning at most
     * {@code maxWords} 64-bit words: -1 when no set bit remains, or
     * {@link #SCAN_EXHAUSTED} when the budget ran out (the block is long - the
     * caller should answer with an O(log n) directory select instead). This lets
     * "where does the next block start" be answered with a couple of word reads
     * for the short blocks that dominate real data, without ever degrading to a
     * linear scan on a multi-million-bit block.
     */
    public long nextSetBit(long fromIndex, long maxWords) {
        if (bitWidth != 1 || blockBase != null) throw new UnsupportedOperationException("nextSetBit only supported for 1-bit bitmaps");
        if (fromIndex >= numEntries) return -1;
        long w = fromIndex >>> 6;
        long lastWord = (numEntries - 1) >>> 6;
        // Saturating: an unbounded budget (Long.MAX_VALUE) must not wrap negative
        // and turn every word crossing into SCAN_EXHAUSTED.
        long budgetLast = (maxWords >= Long.MAX_VALUE - w) ? Long.MAX_VALUE : w + maxWords - 1;
        // Bits are MSB-first within getWord64's view; shift out the bits before fromIndex.
        long word = getWord64(w << 6) << (fromIndex & 63);
        if (word != 0) {
            long r = fromIndex + Long.numberOfLeadingZeros(word);
            return (r < numEntries) ? r : -1;
        }
        while (true) {
            w++;
            if (w > lastWord) return -1;
            if (w > budgetLast) return SCAN_EXHAUSTED;
            word = getWord64(w << 6);
            if (word != 0) {
                long r = (w << 6) + Long.numberOfLeadingZeros(word);
                return (r < numEntries) ? r : -1;
            }
        }
    }

    /**
     * A word-caching single-bit reader for hot loops that probe a 1-bit bitmap at
     * (mostly) monotonically advancing positions: one {@link #getWord64} read
     * serves up to 64 probes. Seeking backwards or jumping is fine - it just
     * refreshes the cached word. NOT thread-safe; use one per iterator.
     */
    public final class BitReader {
        private long wordIdx = -1;
        private long word;

        public boolean bit(long index) {
            long w = index >>> 6;
            if (w != wordIdx) {
                word = getWord64(w << 6);
                wordIdx = w;
            }
            return (word << (index & 63)) < 0;
        }
    }

    /** A {@link BitReader} over this 1-bit bitmap. */
    public BitReader bitReader() {
        if (bitWidth != 1 || blockBase != null) throw new UnsupportedOperationException("bitReader only supported for 1-bit bitmaps");
        return new BitReader();
    }

    // --- WRITE METHODS ---

    public void writeInteger(int value) {
        // Reject values whose bit pattern would not survive the width mask - silent
        // truncation here corrupts the dictionary far from the cause. Widths 32 and
        // 64 are exempt for negatives: the full two's-complement pattern round-trips
        // (the reader casts back to int/long).
        if (bitWidth != 32 && bitWidth != 64 && (value < 0 || value > ((1L << bitWidth) - 1))) {
            throw new IllegalArgumentException(
                "Value " + value + " does not fit in " + bitWidth + " bits");
        }
        putValue(value & ((bitWidth == 64) ? -1L : (1L << bitWidth) - 1));
        numEntries++;
    }

    public void writeLong(long value) {
        // See writeInteger: only width 64 carries a negative long's full pattern.
        if (bitWidth != 64 && (value < 0 || value > ((1L << bitWidth) - 1))) {
            throw new IllegalArgumentException(
                "Value " + value + " does not fit in " + bitWidth + " bits");
        }
        putValue(value & ((bitWidth == 64) ? -1L : (1L << bitWidth) - 1));
        numEntries++;
    }

    public long getBitWidth() {
        return bitWidth;
    }

    public long getNumEntries() {
        return numEntries;
    }

    private void putValue(long valToPack) {
        if (internalStream == null) {
            throw new IllegalStateException("This buffer is a read view; the write API is unavailable");
        }
        writeAccumulator = (writeAccumulator << bitWidth) | valToPack;
        writeAccumulatorCount += bitWidth;

        while (writeAccumulatorCount >= 8) {
            int shift = writeAccumulatorCount - 8;
            stageByte((byte) (writeAccumulator >>> shift));
            writeAccumulator &= (1L << shift) - 1;
            writeAccumulatorCount -= 8;
        }
    }

    /** Pads the last partial byte and hands every staged byte to the stream. A no-op on a read view. */
    public void complete() {
        if (writeAccumulatorCount > 0) {
            stageByte((byte) (writeAccumulator << (8 - writeAccumulatorCount)));
            writeAccumulator = 0;
            writeAccumulatorCount = 0;
        }
        flushStage();
    }

    private void stageByte(byte b) {
        stage[staged++] = b;
        if (staged == stage.length) {
            flushStage();
        }
    }

    private void flushStage() {
        if (staged > 0) {
            internalStream.write(stage, 0, staged);
            staged = 0;
        }
    }

    // --- READ METHODS ---

    /** Makes everything written so far readable through {@link #get}; a no-op on a read view. */
    public void prepareForReading() {
        if (internalStream == null) {
            return;
        }
        complete();
        // Big endian, matching the stream's byte order.
        buffer = ByteBuffer.wrap(internalStream.toByteArray()).order(ByteOrder.BIG_ENDIAN);
        data = new ByteBufferBytes(buffer);
        dataSize = data.size();
    }

    public long get(long index) {
        if (index < 0 || index >= numEntries) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds [0, " + numEntries + ")");
        }
        if (blockBase != null) {
            return getBlocked(index);
        }
        long totalBitOffset = index * bitWidth;
        long startByteIndex = totalBitOffset >>> 3;
        int bitOffsetInFirstByte = (int) (totalBitOffset & 7);
        // FAST PATH: one unaligned big-endian 64-bit read covers the value whenever
        // 8 bytes are available - the sub-byte offset (<= 7) plus any supported
        // width (<= 57) fits in 64 bits, and width 64 is byte-aligned (offset 0).
        // This is the innermost primitive of the whole read path (every id fetch,
        // bitmap probe, and binary-search step lands here), and the former
        // byte-at-a-time accumulation loop dominated its cost.
        if (startByteIndex + 8 <= dataSize) {
            long word = readLong(startByteIndex);
            if (bitWidth == 64) {
                return word;
            }
            return (word >>> (64 - bitOffsetInFirstByte - bitWidth)) & ((1L << bitWidth) - 1);
        }
        // TAIL: fewer than 8 bytes remain before the buffer end; collect bytes
        // individually exactly as before.
        long acc = 0;
        int bitsCollected = 0;
        long currentByteIndex = startByteIndex;
        while (bitsCollected < bitOffsetInFirstByte + bitWidth) {
            if (currentByteIndex >= dataSize) {
                 throw new BufferUnderflowException();
            }
            acc = (acc << 8) | readByte(currentByteIndex);
            currentByteIndex++;
            bitsCollected += 8;
        }
        int rightShift = bitsCollected - (bitOffsetInFirstByte + bitWidth);
        long val = acc >>> rightShift;
        long mask = (bitWidth == 64) ? -1L : (1L << bitWidth) - 1;
        return val & mask;
    }

    /**
     * An entry of a block-structured view: its block's base and width come from the
     * heap, its bits from one 8-byte read. The read never runs past the data: a
     * width is at most 57 bits beside a sub-byte offset of at most 7 (or 64,
     * byte-aligned), and the data ends with {@link BlockedEncoder#TAIL_BYTES} of
     * padding, which the constructor has checked.
     */
    private long getBlocked(long index) {
        int b = (int) (index >>> blockShift);
        int w = blockWidth[b];
        if (w == 0) {
            return blockBase[b];
        }
        long bit = (index & blockMask) * w;
        long word = readLong(blockOffset[b] + (bit >>> 3));
        if (w == 64) {
            return word;
        }
        return blockBase[b] + ((word >>> (64 - (int) (bit & 7) - w)) & ((1L << w) - 1));
    }

    /**
     * Decodes {@code count} entries from {@code index} into {@code dst}: for a
     * block-structured view a block at a time, from whole words, without a call per
     * entry into the bytes behind it.
     */
    public void get(long index, long[] dst, int dstOffset, int count) {
        if (index < 0 || count < 0 || index + count > numEntries) {
            throw new IndexOutOfBoundsException("Entries [" + index + ", " + (index + count) + ") out of bounds [0, " + numEntries + ")");
        }
        if (blockBase == null) {
            for (int i = 0; i < count; i++) {
                dst[dstOffset + i] = get(index + i);
            }
            return;
        }
        while (count > 0) {
            int b = (int) (index >>> blockShift);
            int first = (int) (index & blockMask);
            int n = (int) Math.min(count, blockMask + 1 - first);
            int w = blockWidth[b];
            long base = blockBase[b];
            if (w == 0) {
                java.util.Arrays.fill(dst, dstOffset, dstOffset + n, base);
            } else {
                long bit = (long) first * w;
                long wordAt = blockOffset[b] + ((bit >>> 6) << 3);
                int offset = (int) (bit & 63);
                long mask = (w == 64) ? -1L : (1L << w) - 1;
                long add = (w == 64) ? 0 : base;
                long word = readLong(wordAt);
                for (int i = 0; i < n; i++) {
                    long v;
                    if (offset + w <= 64) {
                        v = (word >>> (64 - offset - w)) & mask;
                        offset += w;
                        if (offset == 64 && i + 1 < n) {
                            wordAt += 8;
                            word = readLong(wordAt);
                            offset = 0;
                        }
                    } else {
                        int over = offset + w - 64;
                        long high = word << over;
                        wordAt += 8;
                        word = readLong(wordAt);
                        v = (high | (word >>> (64 - over))) & mask;
                        offset = over;
                    }
                    dst[dstOffset + i] = add + v;
                }
            }
            index += n;
            dstOffset += n;
            count -= n;
        }
    }

    public long getWord64(long bitIndex) {
        long byteIndex = bitIndex / 8;
        int bitOffset = (int) (bitIndex % 8);
        if (bitIndex + 64 > numEntries) {
            return getWord64SafeTail(bitIndex);
        }
        long raw;
        try {
            raw = readLong(byteIndex);
        } catch (IndexOutOfBoundsException | BufferUnderflowException e) {
            return getWord64SafeTail(bitIndex);
        }
        if (bitOffset == 0) {
            return raw;
        }
        if (byteIndex + 8 >= dataSize) {
             return getWord64SafeTail(bitIndex);
        }
        long nextByte = readByte(byteIndex + 8);
        return (raw << bitOffset) | (nextByte >>> (8 - bitOffset));
    }

    /**
     * The 64-bit MSB-first word at {@code bitIndex} of a bitmap whose end lies
     * within the word (bits past {@code numEntries} read as 0). Assembled from
     * the at most nine bytes that hold it; the former 64 bounds-checked
     * {@link #get} calls per tail word were paid by every block-end lookup on
     * the last graphs/subjects of a level and, on a channel-backed store, by
     * 64 locked channel reads (BG-83).
     */
    private long getWord64SafeTail(long bitIndex) {
        if (bitWidth != 1) {
            long acc = 0;
            for (int i = 0; i < 64; i++) {
                acc <<= 1;
                long entryIdx = bitIndex + i;
                if (entryIdx < numEntries) {
                    acc |= get(entryIdx);
                }
            }
            return acc;
        }
        if (bitIndex < 0 || bitIndex >= numEntries) {
            return 0L;
        }
        long byteIndex = bitIndex >>> 3;
        int bitOffset = (int) (bitIndex & 7);
        long size = dataSize;
        int avail = (int) Math.min(8, size - byteIndex);
        long word = 0;
        for (int k = 0; k < avail; k++) {
            word = (word << 8) | readByte(byteIndex + k);
        }
        word <<= (8 - avail) * 8; // left-align: bit 0 of the word is the MSB
        if (bitOffset != 0) {
            word <<= bitOffset;
            if (byteIndex + 8 < size) {
                word |= ((long) readByte(byteIndex + 8)) >>> (8 - bitOffset);
            }
        }
        long remaining = numEntries - bitIndex;
        return (remaining < 64) ? word & (-1L << (64 - remaining)) : word;
    }

    public Path getName() {
        return path;
    }

    public void add(WritableGroup group) {
        if (buffer == null) {
            throw new IllegalStateException("This buffer is a read view; the write API is unavailable");
        }
        ByteBuffer dup = buffer.duplicate();
        dup.rewind();
        byte[] data = new byte[dup.remaining()];
        dup.get(data);

        if (data.length > 0) {
            WritableDataset ds = group.putDataset(path.toString(), data);
            ds.putAttribute(Params.WIDTH, bitWidth);
            ds.putAttribute(Params.NUM_ENTRIES, numEntries);
        }
    }

    /**
     * As {@link #add}, in the block-structured form (SPECIFICATIONS.md §5.1a): how
     * the index id sequences are stored. The dataset is {@code name}, its directory
     * {@code name + "_blocks"}.
     */
    public void addBlocked(WritableGroup group) {
        if (buffer == null) {
            throw new IllegalStateException("This buffer is a read view; the write API is unavailable");
        }
        prepareForReading();
        BlockedEncoder.add(group, path.toString(), numEntries, this::get);
    }

    private static final int WHOLE_RANGE = Integer.MIN_VALUE;

    /**
     * For a search of the sorted run {@code [low, high]} of a block-structured view:
     * the one block {@code value} can lie in, found without reading the data, or
     * {@link #WHOLE_RANGE} when the run does not span whole blocks (or the view is
     * of one width). A block that lies wholly inside an ascending run starts with
     * its smallest entry, which is its base in the directory; so the last such
     * block whose base is at most {@code value} is found on the heap. The run's
     * first block starts before the run and its last may end after it, so their
     * bases say nothing: the first is the answer when no whole block qualifies, and
     * the last is asked for its first entry, one read.
     */
    private int blockOf(long low, long high, long value) {
        if (blockBase == null) {
            return WHOLE_RANGE;
        }
        int first = (int) (low >>> blockShift);
        int last = (int) (high >>> blockShift);
        if (last - first < 2) {
            return WHOLE_RANGE;
        }
        int lo = first + 1;
        int hi = last - 1;
        int found = first;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Long.compareUnsigned(blockBase[mid], value) <= 0) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (found == last - 1 && Long.compareUnsigned(getBlocked((long) last << blockShift), value) <= 0) {
            found = last;
        }
        return found;
    }

    public long binarySearch(long start, long end, long value) {
        long low = start;
        long high = end;
        int block = blockOf(low, high, value);
        if (block != WHOLE_RANGE) {
            // Every entry before the block is smaller and every one after it larger, so
            // the insertion point of a value that is absent lies in the block too.
            low = Math.max(low, (long) block << blockShift);
            high = Math.min(high, (((long) block + 1) << blockShift) - 1);
        }
        while (low <= high) {
            long mid = (low + high) >>> 1;
            long midVal = get(mid); // Internal get is bit-unpacked
            // Unsigned comparison, matching lowerBound/upperBound: the stored
            // values are unsigned bit patterns, and mixing signed search with
            // unsigned bounds on the same buffer invites subtle disagreement.
            int cmp = Long.compareUnsigned(midVal, value);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // Value found
            }
        }
        return -(low + 1); // Value not found, returns insertion point
    }

/**
     * Finds the first index in the range [start, end] where the value is
     * greater than or equal to the target. (Unsigned)
     * @param start
     * @param end
     * @param value
     * @return
     */
    public long lowerBound(long start, long end, long value) {
        long low = start;
        long high = end;
        long result = -1;
        int block = blockOf(low, high, value);
        if (block != WHOLE_RANGE) {
            low = Math.max(low, (long) block << blockShift);
            high = Math.min(high, (((long) block + 1) << blockShift) - 1);
            // No entry of the block reaches the value: the first that does opens the next block.
            if (high < end) {
                result = high + 1;
            }
        }

        while (low <= high) {
            long mid = (low + high) >>> 1;
            long midVal = get(mid);
            if (Long.compareUnsigned(midVal, value) >= 0) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
    }

    /**
     * Finds the last index in the range [start, end] where the value is
     * less than or equal to the target. (Unsigned)
     * @param start
     * @param end
     * @param value
     * @return
     */
    public long upperBound(long start, long end, long value) {
        long low = start;
        long high = end;
        long result = -1;
        int block = blockOf(low, high, value);
        if (block != WHOLE_RANGE) {
            low = Math.max(low, (long) block << blockShift);
            high = Math.min(high, (((long) block + 1) << blockShift) - 1);
            // Every entry of the block is above the value: the last at or below it closes the block before.
            if (low > start) {
                result = low - 1;
            }
        }

        while (low <= high) {
            long mid = (low + high) >>> 1;
            long midVal = get(mid);
            if (Long.compareUnsigned(midVal, value) <= 0) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }

    /**
     * Returns a sequential LongStream of all entries in the buffer.
     * Note: the stream reads through the shared read view with its own cursor,
     * so it never disturbs this buffer's sequential read position.
     * @return
     */
    public LongStream stream() {
        if (blockBase != null) {
            return LongStream.range(0, numEntries).map(this::get);
        }
        return StreamSupport.longStream(new BitPackedSpliterator(data, numEntries, bitWidth), false);
    }

    private static class BitPackedSpliterator extends Spliterators.AbstractLongSpliterator {
        private final RandomAccessBytes bytes;
        private final long byteSize;
        private final int bitWidth;
        private final long totalEntries;
        private long entriesRead = 0;
        private long pos = 0;

        private long acc = 0L;
        private int accCount = 0;

        BitPackedSpliterator(RandomAccessBytes bytes, long totalEntries, int bitWidth) {
            super(totalEntries, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.SIZED | Spliterator.NONNULL);
            this.bytes = bytes;
            this.byteSize = bytes.size();
            this.totalEntries = totalEntries;
            this.bitWidth = bitWidth;
        }

        @Override
        public boolean tryAdvance(java.util.function.LongConsumer action) {
            if (entriesRead >= totalEntries) {
                return false;
            }

            while (accCount < bitWidth) {
                if (pos >= byteSize) {
                    // A buffer too short for its declared entry count is corrupt.
                    // Fail loudly like the sequential reader does - the old break
                    // left accCount < bitWidth, making the shift below negative
                    // (mod-64) and emitting silent garbage values.
                    throw new BufferUnderflowException();
                }
                acc = (acc << 8) | (bytes.get(pos++) & 0xFFL);
                accCount += 8;
            }

            int shift = accCount - bitWidth;
            long value = acc >>> shift;

            acc &= (1L << shift) - 1;
            accCount -= bitWidth;

            action.accept(value);
            entriesRead++;
            return true;
        }
    }
}
