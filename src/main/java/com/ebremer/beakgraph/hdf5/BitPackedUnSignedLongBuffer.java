package com.ebremer.beakgraph.hdf5;

import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import java.io.ByteArrayOutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

/**
 * A buffer that supports writing and reading bit-packed unsigned integers/longs.
 */
public class BitPackedUnSignedLongBuffer {
    private ByteBuffer buffer;
    private final int bitWidth;
    
    // Writing State
    private long writeAccumulator;
    private int writeAccumulatorCount;
    private final boolean usesInternalStream;
    private ByteArrayOutputStream internalStream;
    
    // Reading State (Sequential)
    private long readAccumulator;
    private int readAccumulatorCount;
    
    private final Path path;
    private long numEntries;

    public BitPackedUnSignedLongBuffer(Path path, ByteBuffer buffer, long numEntries, int bitWidth) {
        this.path = path;
        if (bitWidth <= 0 || bitWidth > 64) {
            throw new IllegalArgumentException("Bit width must be between 1 and 64. Got: " + bitWidth);
        }
        this.bitWidth = bitWidth;
        
        if (buffer == null) {
            this.internalStream = new ByteArrayOutputStream();
            this.usesInternalStream = true;
            this.buffer = ByteBuffer.allocate(0); 
            this.numEntries = 0;
        } else {
            this.buffer = buffer;
            // FIX: Enforce Big Endian so getLong() matches the stream byte order
            this.buffer.order(ByteOrder.BIG_ENDIAN);
            this.usesInternalStream = false;
            this.numEntries = numEntries;
        }
        
        resetState();
    }

    private void resetState() {
        this.writeAccumulator = 0L;
        this.writeAccumulatorCount = 0;
        this.readAccumulator = 0L;
        this.readAccumulatorCount = 0;
    }

    // --- QUERY METHODS ---
/*
    public long select1(long rank) {
        if (rank <= 0) return -1;
        if (bitWidth != 1) throw new UnsupportedOperationException("select1 only supported for 1-bit bitmaps");

        long currentRank = 0;
        long maxIndex = numEntries;
        
        // Optimized scan
        for (long i = 0; i < maxIndex; i += 64) {
            long word = getWord64(i);
            int pop = Long.bitCount(word);
            
            if (currentRank + pop >= rank) {
                for (int b = 0; b < 64; b++) {
                    if (i + b >= maxIndex) return -1;
                    long bit = (word >>> (63 - b)) & 1L;
                    if (bit == 1) {
                        currentRank++;
                        if (currentRank == rank) {
                            return i + b;
                        }
                    }
                }
            }
            currentRank += pop;
        }
        return -1;
    }*/
        
    public long select1(long rank) {
        if (rank <= 0) return -1;
        if (bitWidth != 1) throw new UnsupportedOperationException("select1 only supported for 1-bit bitmaps");
        long currentRank = 0;
        long maxIndex = numEntries;        
        // Optimization: Read directly from buffer limit to avoid Boundary Checks in the loop
        // We stop 8 bytes before the end to safely use getLong() without BufferUnderflow
        int safeLimit = buffer.limit() - 8; 
        int bufferOffset = 0;
        long i = 0;
        // FAST PATH: Iterate over full 64-bit words directly from buffer
        // This eliminates the overhead of getWord64()
        while (bufferOffset <= safeLimit && i < maxIndex) {
            long word = buffer.getLong(bufferOffset);
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
     * Finds the index (0-63) of the k-th set bit in a 64-bit word using Broadword Selection.
     * This is strictly O(1) -- exactly 6 checks, no loops.
     * * @param word The 64-bit word (Big Endian context).
     * @param k The rank to find (1-based).
     * @return The 0-based index of the k-th set bit (from MSB).
     */
    private int selectInWordSafe(long word, long k) {
        int result = 0;
        int cnt;
        // Check top 32 bits
        // shift right to isolate the top 32 bits.
        cnt = Long.bitCount(word >>> 32);
        if (k > cnt) {
            // The target is NOT in the top 32. It's in the lower 32.
            // Move the lower 32 bits up, add 32 to our result index, and subtract the count we skipped.
            word <<= 32;
            result += 32;
            k -= cnt;
        }
        // Check top 16 bits (of the remaining word)
        cnt = Long.bitCount(word >>> 48);
        if (k > cnt) {
            word <<= 16;
            result += 16;
            k -= cnt;
        }
        // Check top 8 bits
        cnt = Long.bitCount(word >>> 56);
        if (k > cnt) {
            word <<= 8;
            result += 8;
            k -= cnt;
        }
        // Check top 4 bits
        cnt = Long.bitCount(word >>> 60);
        if (k > cnt) {
            word <<= 4;
            result += 4;
            k -= cnt;
        }
        // Check top 2 bits
        cnt = Long.bitCount(word >>> 62);
        if (k > cnt) {
            word <<= 2;
            result += 2;
            k -= cnt;
        }
        // Check top 1 bit
        // If k > cnt (where cnt is 0 or 1), it means the target is the 2nd bit of this pair.
        cnt = Long.bitCount(word >>> 63);
        if (k > cnt) {
            result += 1;
        }
        return result;
    }

    /**
     * Finds the index (0-63) of the k-th set bit in a 64-bit word.
     * Uses CPU intrinsics (numberOfLeadingZeros) which is safer and faster than manual loops.
     */
    private int selectInWordSafe2(long word, long k) {
        // Loop finding the next set bit until we find the k-th one.
        // Since max k is 64, this is extremely fast.
        while (k > 0) {
            // Find position of the first set bit (MSB 0-indexed)
            int lz = Long.numberOfLeadingZeros(word);        
            if (k == 1) {
                return lz;
            }            
            // Clear the bit we just found so we can find the next one
            // (1L << (63 - lz)) creates a mask with only that bit set.
            word &= ~(1L << (63 - lz));
            k--;
        }
        return -1; // Should not happen given the logic in select1
    }
    
    // --- WRITE METHODS ---

    public void writeInteger(int value) {
        putValue(value & ((bitWidth == 64) ? -1L : (1L << bitWidth) - 1));
        numEntries++;
    }

    public void writeLong(long value) {
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
        writeAccumulator = (writeAccumulator << bitWidth) | valToPack;
        writeAccumulatorCount += bitWidth;

        while (writeAccumulatorCount >= 8) {
            int shift = writeAccumulatorCount - 8;
            byte b = (byte) (writeAccumulator >>> shift);
            
            if (usesInternalStream) {
                internalStream.write(b);
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
                internalStream.write(b);
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
    }

    // --- READ METHODS ---

    public void prepareForReading() {
        complete();
        if (usesInternalStream) {
            byte[] data = internalStream.toByteArray();
            buffer = ByteBuffer.wrap(data);
            // FIX: Enforce Big Endian for internal buffers too
            buffer.order(ByteOrder.BIG_ENDIAN);
        } else {
            buffer.flip();
            // FIX: Enforce Big Endian
            buffer.order(ByteOrder.BIG_ENDIAN);
        }
        readAccumulator = 0L;
        readAccumulatorCount = 0;
    }

    public long get(long index) {
        if (index < 0 || index >= numEntries) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds [0, " + numEntries + ")");
        }

        long totalBitOffset = index * bitWidth;
        int startByteIndex = Math.toIntExact(totalBitOffset / 8);
        int bitOffsetInFirstByte = (int) (totalBitOffset % 8);
        
        long acc = 0;
        int bitsCollected = 0;
        int currentByteIndex = startByteIndex;

        while (bitsCollected < bitOffsetInFirstByte + bitWidth) {
            if (currentByteIndex >= buffer.limit()) {
                 throw new BufferUnderflowException();
            }
            acc = (acc << 8) | (buffer.get(currentByteIndex) & 0xFFL);
            currentByteIndex++;
            bitsCollected += 8;
        }

        int rightShift = bitsCollected - (bitOffsetInFirstByte + bitWidth);
        long val = acc >>> rightShift;

        long mask = (bitWidth == 64) ? -1L : (1L << bitWidth) - 1;
        return val & mask;
    }

    public long getWord64(long bitIndex) {
        int byteIndex = Math.toIntExact(bitIndex / 8);
        int bitOffset = (int) (bitIndex % 8);

        if (bitIndex + 64 > numEntries) {
            return getWord64SafeTail(bitIndex);
        }

        long raw;
        try {
            raw = buffer.getLong(byteIndex);
        } catch (IndexOutOfBoundsException | BufferUnderflowException e) {
            return getWord64SafeTail(bitIndex);
        }

        if (bitOffset == 0) {
            return raw;
        }

        if (byteIndex + 8 >= buffer.limit()) {
             return getWord64SafeTail(bitIndex);
        }

        long nextByte = buffer.get(byteIndex + 8) & 0xFFL;
        return (raw << bitOffset) | (nextByte >>> (8 - bitOffset));
    }

    private long getWord64SafeTail(long bitIndex) {
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

    public int get() { return (int) getValue(); }
    public long getLong() { return getValue(); }

    private long getValue() {
        while (readAccumulatorCount < bitWidth) {
            if (!buffer.hasRemaining()) {
                throw new BufferUnderflowException();
            }
            readAccumulator = (readAccumulator << 8) | (buffer.get() & 0xFFL);
            readAccumulatorCount += 8;
        }
        int shift = readAccumulatorCount - bitWidth;
        long value = readAccumulator >>> shift;
        
        readAccumulator &= (1L << shift) - 1;
        readAccumulatorCount -= bitWidth;
        return value;
    }

    public Path getName() { return path; }

    public void Add(WritableGroup group) {
        ByteBuffer dup = buffer.duplicate();
        dup.rewind();
        byte[] data = new byte[dup.remaining()];
        dup.get(data);

        if (data.length > 0) {
            WritableDataset ds = group.putDataset(path.toString(), data);
            ds.putAttribute("width", bitWidth);
            ds.putAttribute("numEntries", numEntries);
        }
    }
}