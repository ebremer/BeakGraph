package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.Params;

import com.ebremer.beakgraph.io.ByteBufferBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.utils.UTIL;
import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import java.io.ByteArrayOutputStream;
import java.nio.BufferOverflowException;
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
 * <p>All read paths go through {@link RandomAccessBytes} with long offsets, so
 * a read view is no longer capped at 2 GiB by ByteBuffer's int indexing - the
 * backing bytes may be a jHDF-mapped buffer, an FFM-mapped segment, or (for
 * the write side after {@link #prepareForReading()}) the internally
 * accumulated bytes.
 */
public class BitPackedUnSignedLongBuffer implements DictionarySinks.LongSink {
    private ByteBuffer buffer;
    private RandomAccessBytes data;
    private final int bitWidth;

    // Writing State
    private long writeAccumulator;
    private int writeAccumulatorCount;
    private final boolean usesInternalStream;
    private ByteArrayOutputStream internalStream;
    // Packed bytes are staged here and handed to the stream a chunk at a time:
    // ByteArrayOutputStream.write(int) is synchronized, and every S/B/SB/BB
    // entry of every index level paid that lock per byte (BG-250).
    private static final int STAGE_BYTES = 1 << 16;
    private byte[] stage;
    private int staged;

    // Reading State (Sequential)
    private long readAccumulator;
    private int readAccumulatorCount;
    private long readPos;

    private final Path path;
    private long numEntries;

    public BitPackedUnSignedLongBuffer(Path path, ByteBuffer buffer, long numEntries, int bitWidth) {
        this.path = path;
        checkWidth(bitWidth);
        this.bitWidth = bitWidth;
        if (buffer == null) {
            this.internalStream = new ByteArrayOutputStream();
            this.stage = new byte[STAGE_BYTES];
            this.usesInternalStream = true;
            this.buffer = ByteBuffer.allocate(0);
            this.data = new ByteBufferBytes(this.buffer);
            this.numEntries = 0;
        } else {
            this.buffer = buffer;
            // Enforce Big Endian so getLong() matches the stream byte order
            this.buffer.order(ByteOrder.BIG_ENDIAN);
            this.data = new ByteBufferBytes(this.buffer);
            this.usesInternalStream = false;
            this.numEntries = numEntries;
        }
        resetState();
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
        this.data = data;
        this.usesInternalStream = false;
        this.numEntries = numEntries;
        resetState();
    }

    /**
     * A read-only view over already-packed bytes. This is how the HDF5 readers
     * construct buffers (via {@code DatasetBytes.of}); unlike the ByteBuffer
     * constructor it carries no 2 GiB ceiling. A static factory rather than a
     * constructor overload: the writers construct with a null ByteBuffer
     * literal, which an overload would make ambiguous.
     */
    /** Whether the backing bytes are read from a remote store (see {@link RandomAccessBytes#isRemote()}). */
    public boolean isRemote() {
        return data != null && data.isRemote();
    }

    public static BitPackedUnSignedLongBuffer readView(RandomAccessBytes data, long numEntries, int bitWidth) {
        return new BitPackedUnSignedLongBuffer(data, numEntries, bitWidth);
    }

    private static void checkWidth(int bitWidth) {
        // The pack/unpack accumulators (putValue/getValue/get/stream) hold a value together with its
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

    private void resetState() {
        this.writeAccumulator = 0L;
        this.writeAccumulatorCount = 0;
        this.readAccumulator = 0L;
        this.readAccumulatorCount = 0;
        this.readPos = 0L;
    }

    // --- QUERY METHODS ---

    public long select1(long rank) {
        // select1 is 1-based: rank 1 = first set bit. rank < 1 (including 0) is not a valid query,
        // so return -1 instead of a misleading index 0 (matches the rank<1 guard in the iterators).
        if (rank < 1) return -1;
        if (bitWidth != 1) throw new UnsupportedOperationException("select1 only supported for 1-bit bitmaps");
        long currentRank = 0;
        long maxIndex = numEntries;
        // Clamp fast-path byte range to the bytes that actually hold live bits (numEntries/64 full words),
        // so trailing padding / arena tail bytes can't inflate popcount and skew the rank.
        long fullWords = maxIndex / 64;
        long fullWordBytes = fullWords * 8;
        long safeLimit = Math.min(fullWordBytes, data.size()) - 8;
        long bufferOffset = 0;
        long i = 0;
        // FAST PATH: Iterate over full 64-bit words directly from the backing bytes
        while (bufferOffset <= safeLimit && i < maxIndex) {
            long word = data.getLong(bufferOffset);
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
        if (bitWidth != 1) throw new UnsupportedOperationException("nextSetBit only supported for 1-bit bitmaps");
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
        if (bitWidth != 1) throw new UnsupportedOperationException("bitReader only supported for 1-bit bitmaps");
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
        if (buffer == null) {
            throw new IllegalStateException("This buffer is a read view; the write API is unavailable");
        }
        writeAccumulator = (writeAccumulator << bitWidth) | valToPack;
        writeAccumulatorCount += bitWidth;

        while (writeAccumulatorCount >= 8) {
            int shift = writeAccumulatorCount - 8;
            byte b = (byte) (writeAccumulator >>> shift);

            if (usesInternalStream) {
                stageByte(b);
            } else {
                if (!buffer.hasRemaining()) {
                    throw new BufferOverflowException();
                }
                buffer.put(b);
            }

            writeAccumulator &= (1L << shift) - 1;
            writeAccumulatorCount -= 8;
        }
    }

    public void complete() {
        if (writeAccumulatorCount > 0) {
            byte b = (byte) (writeAccumulator << (8 - writeAccumulatorCount));

            if (usesInternalStream) {
                stageByte(b);
            } else {
                if (buffer.hasRemaining()) {
                    buffer.put(b);
                } else {
                    throw new BufferOverflowException();
                }
            }
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

    public void prepareForReading() {
        complete();
        if (usesInternalStream) {
            byte[] bytes = internalStream.toByteArray();
            buffer = ByteBuffer.wrap(bytes);
            // Enforce Big Endian for internal buffers too
            buffer.order(ByteOrder.BIG_ENDIAN);
        } else {
            buffer.flip();
            // Enforce Big Endian
            buffer.order(ByteOrder.BIG_ENDIAN);
        }
        // Refresh the read view: the flip/wrap above changed the readable window.
        data = new ByteBufferBytes(buffer);
        readAccumulator = 0L;
        readAccumulatorCount = 0;
        readPos = 0L;
    }

    public long get(long index) {
        if (index < 0 || index >= numEntries) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds [0, " + numEntries + ")");
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
        if (startByteIndex + 8 <= data.size()) {
            long word = data.getLong(startByteIndex);
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
            if (currentByteIndex >= data.size()) {
                 throw new BufferUnderflowException();
            }
            acc = (acc << 8) | (data.get(currentByteIndex) & 0xFFL);
            currentByteIndex++;
            bitsCollected += 8;
        }
        int rightShift = bitsCollected - (bitOffsetInFirstByte + bitWidth);
        long val = acc >>> rightShift;
        long mask = (bitWidth == 64) ? -1L : (1L << bitWidth) - 1;
        return val & mask;
    }

    public long getWord64(long bitIndex) {
        long byteIndex = bitIndex / 8;
        int bitOffset = (int) (bitIndex % 8);
        if (bitIndex + 64 > numEntries) {
            return getWord64SafeTail(bitIndex);
        }
        long raw;
        try {
            raw = data.getLong(byteIndex);
        } catch (IndexOutOfBoundsException | BufferUnderflowException e) {
            return getWord64SafeTail(bitIndex);
        }
        if (bitOffset == 0) {
            return raw;
        }
        if (byteIndex + 8 >= data.size()) {
             return getWord64SafeTail(bitIndex);
        }
        long nextByte = data.get(byteIndex + 8) & 0xFFL;
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
        long size = data.size();
        int avail = (int) Math.min(8, size - byteIndex);
        long word = 0;
        for (int k = 0; k < avail; k++) {
            word = (word << 8) | (data.get(byteIndex + k) & 0xFFL);
        }
        word <<= (8 - avail) * 8; // left-align: bit 0 of the word is the MSB
        if (bitOffset != 0) {
            word <<= bitOffset;
            if (byteIndex + 8 < size) {
                word |= (data.get(byteIndex + 8) & 0xFFL) >>> (8 - bitOffset);
            }
        }
        long remaining = numEntries - bitIndex;
        return (remaining < 64) ? word & (-1L << (64 - remaining)) : word;
    }

    public int get() {
        return (int) getValue();
    }

    public long getLong() {
        return getValue();
    }

    private long getValue() {
        while (readAccumulatorCount < bitWidth) {
            if (readPos >= data.size()) {
                throw new BufferUnderflowException();
            }
            readAccumulator = (readAccumulator << 8) | (data.get(readPos++) & 0xFFL);
            readAccumulatorCount += 8;
        }
        int shift = readAccumulatorCount - bitWidth;
        long value = readAccumulator >>> shift;

        readAccumulator &= (1L << shift) - 1;
        readAccumulatorCount -= bitWidth;
        return value;
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

    public long binarySearch(long start, long end, long value) {
        long low = start;
        long high = end;
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
