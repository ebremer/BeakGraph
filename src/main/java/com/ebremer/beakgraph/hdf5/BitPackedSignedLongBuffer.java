package com.ebremer.beakgraph.hdf5;

import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.file.Path;

/**
 * A buffer for storing integers or longs packed into a fixed number of bits.
 * This class allows for efficient storage when numbers do not require their full native bit size.
 * It correctly handles positive and negative numbers using two's complement representation for the specified bitWidth.
 * The bitWidth can range from 1 to 64.
 *
 * If a null ByteBuffer is provided to the constructor, an internal ByteArrayOutputStream is used,
 * allowing the buffer to expand dynamically during writing.
 *
 * <p><b>Usage:</b></p>
 * <ol>
 * <li>Create an instance with a {@link ByteBuffer} (can be null for auto-expansion) and the desired bit width (1-64).</li>
 * <li>To write:
 * <ul>
 * <li>Optionally call {@link #prepareForWriting()} if reusing the buffer.</li>
 * <li>Call {@link #put(int)} or {@link #putLong(long)} for each number.</li>
 * <li>Call {@link #prepareForReading()} (which includes a flush) when done writing. This finalizes the buffer if an internal stream was used.</li>
 * </ul>
 * </li>
 * <li>To read:
 * <ul>
 * <li>Call {@link #get()} or {@link #getLong()} to retrieve numbers.</li>
 * </ul>
 * </li>
 * </ol>
 */
public class BitPackedSignedLongBuffer implements HDF5Buffer {
    private ByteBuffer buffer; // Can be the user-provided buffer or one wrapped from internalByteStream
    private final int bitWidth;
    private long writeAccumulator;
    private int writeAccumulatorCount;
    private long readAccumulator;
    private int readAccumulatorCount;
    private long totalValidBitsWritten;
    private long totalValidBitsRead;
    // For internal dynamic buffer
    private ByteArrayOutputStream internalByteStream;
    private final boolean usesInternalStream;
    private Path path;
    private long numEntries = 0;

    /**
     * Constructs a BitPackedIntBuffer.
     *
     * @param path
     * @param buffer The ByteBuffer to use for storing packed numbers. If null, an internal,
     * dynamically expanding buffer (ByteArrayOutputStream) will be used.
     * @param bitWidth The number of bits for each number (must be between 1 and 64, inclusive).
     * @throws IllegalArgumentException if bitWidth is not in the range [1, 64].
     */
    public BitPackedSignedLongBuffer(Path path, ByteBuffer buffer, int bitWidth) {
        this.path = path;
        if (bitWidth <= 0 || bitWidth > 64) {
            throw new IllegalArgumentException("Bit width must be between 1 and 64, inclusive. Got: " + bitWidth);
        }
        this.bitWidth = bitWidth;
        if (buffer == null) {
            this.internalByteStream = new ByteArrayOutputStream();
            this.usesInternalStream = true;
            this.buffer = ByteBuffer.allocate(0); // Dummy buffer, actual data in internalByteStream until prepareForReading
        } else {
            this.buffer = buffer;
            this.usesInternalStream = false;
            this.internalByteStream = null; // Not used
        }
        this.writeAccumulator = 0L;
        this.writeAccumulatorCount = 0;
        this.readAccumulator = 0L;
        this.readAccumulatorCount = 0;
        this.totalValidBitsWritten = 0L;
        this.totalValidBitsRead = 0L;
    }

    /**
     * Puts an integer into the buffer.
     * @param value The integer to put.
     * @throws BufferOverflowException if an external, fixed-size ByteBuffer is used and it does not have enough space.
     */
    public void writeInteger(int value) {
        numEntries++;
        long mask;
        if (this.bitWidth == 64) {
            mask = -1L;
        } else {
            mask = (1L << this.bitWidth) - 1;
        }
        long valToPack = value & mask;
        putValue(valToPack);
    }

    /**
     * Puts a long into the buffer.
     * @param value The long to put.
     * @throws BufferOverflowException if an external, fixed-size ByteBuffer is used and it does not have enough space.
     */
    public void writeLong(long value) {
        numEntries++;
        long mask;
        if (this.bitWidth == 64) {
            mask = -1L;
        } else {
            mask = (1L << this.bitWidth) - 1;
        }
        long valToPack = value & mask;
        putValue(valToPack);
    }

    private void putValue(long valToPack) {
        writeAccumulator = (writeAccumulator << this.bitWidth) | valToPack;
        writeAccumulatorCount += (int)this.bitWidth;
        this.totalValidBitsWritten += this.bitWidth;
        while (writeAccumulatorCount >= 8) {
            if (!usesInternalStream && !buffer.hasRemaining()) {
                throw new BufferOverflowException();
            }
            int remainingBitsInAccumulator = writeAccumulatorCount - 8;
            byte byteToWrite = (byte) (writeAccumulator >>> remainingBitsInAccumulator);

            if (usesInternalStream) {
                internalByteStream.write(byteToWrite);
            } else {
                buffer.put(byteToWrite);
            }
            long lsbMask = (remainingBitsInAccumulator == 64) ? -1L : (1L << remainingBitsInAccumulator) - 1;
            writeAccumulator &= lsbMask;
            writeAccumulatorCount -= 8;
        }
    }

    public int get() {
        long rawValue = getValue();
        if (this.bitWidth < 32) {
            int signBitPositionInValue = (int)this.bitWidth - 1;
            boolean isNegative = ((rawValue >> signBitPositionInValue) & 1) == 1;
            if (isNegative) {
                rawValue |= (-1L << this.bitWidth);
            }
        }
        return (int) rawValue;
    }

    /**
     * Gets a long from the buffer.
     * @return The unpacked long.
     * @throws BufferUnderflowException if not enough valid bits are available.
     */
    public long getLong() {
        long rawValue = getValue();
        if (this.bitWidth < 64) { 
             int signBitPositionInValue = (int)this.bitWidth - 1;
             boolean isNegative = ((rawValue >> signBitPositionInValue) & 1) == 1;
             if (isNegative) {
                 rawValue |= (-1L << this.bitWidth);
             }
        }
        return rawValue;
    }
    
    private long getValue() {
        if ((totalValidBitsWritten - totalValidBitsRead) < this.bitWidth) {
            throw new BufferUnderflowException();
        }
        // Ensure buffer is ready for reading, especially if it was just created from internalByteStream
        if (usesInternalStream && (this.buffer == null || this.buffer.capacity() == 0) && internalByteStream.size() > 0) {
            // This case should ideally be handled by ensuring prepareForReading was called.
            // If prepareForReading wasn't called, this.buffer might be the dummy one.
            // For robustness, one might re-wrap here, but it's better to enforce prepareForReading.
             System.err.println("Warning: Reading from BitPackedIntBuffer that might not have been prepared for reading after using internal stream.");
        }
        while (readAccumulatorCount < this.bitWidth) {
            if (!buffer.hasRemaining()) { // Check the current this.buffer
                throw new BufferUnderflowException();
            }
            byte byteRead = buffer.get();
            readAccumulator = (readAccumulator << 8) | (byteRead & 0xFFL);
            readAccumulatorCount += 8;
        }
        int remainingBitsInAccumulator = readAccumulatorCount - (int)this.bitWidth;
        long result = readAccumulator >>> remainingBitsInAccumulator;
        long lsbMask = (remainingBitsInAccumulator == 64) ? -1L : (1L << remainingBitsInAccumulator) - 1;
        readAccumulator &= lsbMask;
        readAccumulatorCount -= (int)this.bitWidth;
        this.totalValidBitsRead += this.bitWidth;
        return result;
    }
    
    public long get(long n) {
        if (usesInternalStream && this.buffer.capacity() == 0 && totalValidBitsWritten > 0 && internalByteStream.size() > 0) {
            throw new IllegalStateException(
                """
                Buffer was initialized with an internal stream and has data,
                but prepareForReading() has not been called to finalize it for reading.
                """);
        }
        long numValuesAvailable = 8*totalValidBitsWritten / this.bitWidth;
        /*
        if (n < 0 || n >= numValuesAvailable) {
            throw new IndexOutOfBoundsException(
                "Index " + n + " is out of bounds for " + numValuesAvailable + " values. " +
                "Total valid bits written: " + totalValidBitsWritten + ", bitWidth: " + this.bitWidth
            );
        }*/
        long targetBitOffset = n * this.bitWidth;
        long currentBytePosLong = targetBitOffset / 8;
        int bitsToSkipInFirstByte = (int) (targetBitOffset % 8);
        int originalBufferPos = this.buffer.position(); // Save original position
        try {
            if (currentBytePosLong > Integer.MAX_VALUE) {
                 throw new IndexOutOfBoundsException("Byte position for index " + n + " exceeds ByteBuffer capacity.");
            }
            this.buffer.position((int)currentBytePosLong);
            long localAccumulator = 0L;
            int bitsInLocalAccumulator = 0;
            int bitsNeededInAccumulator = bitsToSkipInFirstByte + (int) this.bitWidth;
            while (bitsInLocalAccumulator < bitsNeededInAccumulator) {
                if (!this.buffer.hasRemaining()) {
                    // This should ideally not be reached if IndexOutOfBounds check is correct
                    throw new BufferUnderflowException();
                }
                localAccumulator = (localAccumulator << 8) | (this.buffer.get() & 0xFFL);
                bitsInLocalAccumulator += 8;
            }
            
            // localAccumulator now contains a window of bits.
            // The desired value is at the MSB end of the (bitsToSkipInFirstByte + bitWidth) segment.
            // We need to shift right to discard bits that are beyond our target value within the accumulator.
            int bitsToShiftRight = bitsInLocalAccumulator - bitsNeededInAccumulator;
            long shiftedValue = localAccumulator >>> bitsToShiftRight;

            // Now shiftedValue has (bitsToSkipInFirstByte + bitWidth) bits at its LSB end.
            // We need to mask out the leading bitsToSkipInFirstByte.
            long valueMaskForBitWidth = (this.bitWidth == 64) ? -1L : (1L << this.bitWidth) - 1;
            long rawValue = shiftedValue & valueMaskForBitWidth;

            // Sign extension
            if (this.bitWidth < 64) {
                int signBitPosition = (int) this.bitWidth - 1;
                boolean isNegative = ((rawValue >> signBitPosition) & 1) == 1;
                if (isNegative) {
                    rawValue |= (-1L << this.bitWidth);
                }
            }
            return rawValue;
        } finally {
            this.buffer.position(originalBufferPos); // Restore original position
        }
    }

    public void flush() {
        if (writeAccumulatorCount > 0) {
            if (!usesInternalStream && !buffer.hasRemaining()) {
                throw new BufferOverflowException();
            }
            byte lastByte = (byte) (writeAccumulator << (8 - writeAccumulatorCount));
            if (usesInternalStream) {
                internalByteStream.write(lastByte);
            } else {
                buffer.put(lastByte);
            }
            writeAccumulator = 0L;
            writeAccumulatorCount = 0;
        }
    }

    public void prepareForReading() {
        flush();
        if (usesInternalStream) {
            byte[] packedBytes = internalByteStream.toByteArray();
            this.buffer = ByteBuffer.wrap(packedBytes); // Position 0, limit = capacity
        } else {
            this.buffer.flip(); // For external buffer
        }
        readAccumulator = 0L;
        readAccumulatorCount = 0;
        totalValidBitsRead = 0L; 
    }

    public void prepareForWriting() {
        if (usesInternalStream) {
            internalByteStream.reset();
            this.buffer = ByteBuffer.allocate(0); // Reset to dummy buffer
        } else {
            this.buffer.clear();
        }
        writeAccumulator = 0L;
        writeAccumulatorCount = 0;
        readAccumulator = 0L;
        readAccumulatorCount = 0;
        totalValidBitsWritten = 0L;
        totalValidBitsRead = 0L;
    }

    @Override
    public long getNumEntries() {
        if (totalValidBitsWritten < totalValidBitsRead) { 
            return 0;
        }
        return (long) ((totalValidBitsWritten - totalValidBitsRead) / this.bitWidth);
    }

    public long getBitWidth() {
        return this.bitWidth;
    }
    
    @Override
    public Path getName() {
        return path;
    }

    @Override
    public void Add(WritableGroup group) {
        byte[] ho = buffer.array();
        if (ho.length>0) {
            WritableDataset ds = group.putDataset(path.toString(), ho);
            ds.putAttribute("width", bitWidth);
            ds.putAttribute("numEntries", getNumEntries());
        }
    }
}
