package com.ebremer.beakgraph.hdtish;

import java.io.IOException;
import java.nio.ByteBuffer;

// Interface to abstract byte writing, allowing flexibility for files or ByteBuffers
interface ByteWriter {
    void writeByte(byte b) throws IOException;
}

// BitPackedWriter class to pack bits into bytes and write to an output
public class BitPackedWriter {
    private final ByteWriter byteWriter;
    private long bitBuffer = 0; // Accumulates bits
    private int bitCount = 0;  // Number of bits currently in the buffer

    // Constructor accepting a ByteWriter
    public BitPackedWriter(ByteWriter byteWriter) {
        this.byteWriter = byteWriter;
    }

    // Write the least significant n bits of the integer to the buffer
    public void writeInteger(int value, int n) throws IOException {
        if (n < 0 || n > 32) {
            throw new IllegalArgumentException("n must be between 0 and 32");
        }
        // Extract the least significant n bits
        long bits = value & ((1L << n) - 1);
        // Shift existing bits left and append new bits
        bitBuffer = (bitBuffer << n) | bits;
        bitCount += n;
        // Write full bytes when possible
        while (bitCount >= 8) {
            int shift = bitCount - 8;
            byte b = (byte) (bitBuffer >>> shift);
            byteWriter.writeByte(b);
            // Keep the remaining bits
            bitBuffer = bitBuffer & ((1L << shift) - 1);
            bitCount -= 8;
        }
    }

    // Flush remaining bits as a padded byte and close
    public void close() throws IOException {
        if (bitCount > 0) {
            // Pad the remaining bits with zeros on the right
            byte b = (byte) (bitBuffer << (8 - bitCount));
            byteWriter.writeByte(b);
        }
    }

    // Static factory method to create a BitPackedWriter for a file
    public static BitPackedWriter forFile(String filePath) throws IOException {
        java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath);
        ByteWriter fileWriter = new ByteWriter() {
            @Override
            public void writeByte(byte b) throws IOException {
                fos.write(b);
            }
        };
        return new BitPackedWriter(fileWriter);
    }

    // Static factory method to create a BitPackedWriter for a ByteBuffer
    public static BitPackedWriter forByteBuffer(ByteBuffer buffer) {
        ByteWriter bufferWriter = new ByteWriter() {
            @Override
            public void writeByte(byte b) throws IOException {
                if (!buffer.hasRemaining()) {
                    throw new IOException("ByteBuffer capacity exceeded");
                }
                buffer.put(b);
            }
        };
        return new BitPackedWriter(bufferWriter);
    }

    public static void main(String[] args) throws IOException {
        // Example 1: Writing to a file
        BitPackedWriter fileWriter = BitPackedWriter.forFile("output.dat");
        fileWriter.writeInteger(5, 3); // 101
        fileWriter.writeInteger(3, 3); // 011
        fileWriter.writeInteger(7, 3); // 111
        fileWriter.writeInteger(2, 3); // 111
        fileWriter.writeInteger(1, 3); // 111
        fileWriter.writeInteger(1, 3); // 111
        fileWriter.writeInteger(4, 3); // 111
        fileWriter.close();

        // Example 2: Writing to a ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(10);
        BitPackedWriter bufferWriter = BitPackedWriter.forByteBuffer(buffer);
        bufferWriter.writeInteger(5, 3);
        bufferWriter.writeInteger(3, 3);
        bufferWriter.writeInteger(7, 3);
        bufferWriter.close();

        // Access the packed bytes
        buffer.flip();
        byte[] packedBytes = new byte[buffer.remaining()];
        buffer.get(packedBytes);
    }
}
