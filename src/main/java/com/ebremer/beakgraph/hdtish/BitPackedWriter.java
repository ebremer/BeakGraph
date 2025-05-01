package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

// Interface to abstract byte writing, allowing flexibility for files or ByteBuffers
interface ByteWriter {
    void writeByte(byte b) throws IOException;
}

// BitPackedWriter class to pack bits into bytes and write to an output
public class BitPackedWriter {
    private final ByteWriter byteWriter;
    private final int width;
    private long bitBuffer = 0; // Accumulates bits
    private int bitCount = 0;  // Number of bits currently in the buffer

    // Constructor accepting a ByteWriter
    public BitPackedWriter(ByteWriter byteWriter, int width) {
        this.byteWriter = byteWriter;
        this.width = width;
    }

    // Write the least significant n bits of the integer to the buffer
    public void writeInteger(int value) throws IOException {
        if (width < 0 || width > 32) {
            throw new IllegalArgumentException("n must be between 0 and 32");
        }
        // Extract the least significant n bits
        long bits = value & ((1L << width) - 1);
        // Shift existing bits left and append new bits
        bitBuffer = (bitBuffer << width) | bits;
        bitCount += width;
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
    
    public void writeLong(long value) throws IOException {
        if (width < 0 || width > 64) {
            throw new IllegalArgumentException("width must be between 0 and 64");
        }
        long bits = value & ((1L << width) - 1);
        bitBuffer = (bitBuffer << width) | bits;
        bitCount += width;
        while (bitCount >= 8) {
            int shift = bitCount - 8;
            byte b = (byte) (bitBuffer >>> shift);
            byteWriter.writeByte(b);
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
    public static BitPackedWriter forFile(File file, int width) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        ByteWriter fileWriter = new ByteWriter() {
            @Override
            public void writeByte(byte b) throws IOException {
                fos.write(b);
            }
        };
        return new BitPackedWriter(fileWriter, width);
    }
    
    public static String toBinaryString(ByteBuffer buffer, String delimiter) {
        buffer.mark();
        StringBuilder binary = new StringBuilder();
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            String binaryByte = String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            binary.append(binaryByte);
            if (buffer.hasRemaining()) {
                binary.append(delimiter);
            }
        }
        buffer.reset();
        return binary.toString();
    }

    // Static factory method to create a BitPackedWriter for a ByteBuffer
    public static BitPackedWriter forByteBuffer(ByteBuffer buffer, int width) {
        ByteWriter bufferWriter = new ByteWriter() {
            @Override
            public void writeByte(byte b) throws IOException {
                if (!buffer.hasRemaining()) {
                    throw new IOException("ByteBuffer capacity exceeded");
                }
                buffer.put(b);
            }
        };
        return new BitPackedWriter(bufferWriter, width);
    }

    public static void main(String[] args) throws IOException {
        // Example 1: Writing to a file
        BitPackedWriter fileWriter = BitPackedWriter.forFile(new File("output.dat"), 3);
        fileWriter.writeInteger(5); // 101
        fileWriter.writeInteger(3); // 011
        fileWriter.writeInteger(7); // 111
        fileWriter.writeInteger(2); // 111
        fileWriter.writeInteger(1); // 111
        fileWriter.writeInteger(1); // 111
        fileWriter.writeInteger(4); // 111
        fileWriter.close();

        // Example 2: Writing to a ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(10);
        BitPackedWriter bufferWriter = BitPackedWriter.forByteBuffer(buffer,3);
        bufferWriter.writeInteger(5);
        bufferWriter.writeInteger(3);
        bufferWriter.writeInteger(7);
        bufferWriter.close();

        buffer.rewind();
        System.out.println(toBinaryString(buffer, " "));
    }
}
