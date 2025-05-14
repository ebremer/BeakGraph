package com.ebremer.beakgraph.HDTish;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

interface ByteWriter {
    void writeByte(byte b) throws IOException;
}

public class BitPackedWriter implements HDF5Buffer, AutoCloseable {
    private final ByteWriter byteWriter;
    private final long width;
    private long bitBuffer = 0;
    private int bitCount = 0;
    private ByteArrayOutputStream os;
    private Path path;
    private long entries = 0;

    private BitPackedWriter(Path path, ByteWriter byteWriter, long width, ByteArrayOutputStream os) {
        this.path = path;
        this.byteWriter = byteWriter;
        this.width = width;
        this.os = os;
    }
    
    @Override
    public Map<String, Object> getProperties() {
        Map<String,Object> meta = new HashMap<>();
        meta.put("width", width);
        meta.put("numEntries", entries);
        return meta;
    }
    
    @Override
    public Path getName() {
        return path;
    }

    @Override
    public byte[] getBuffer() {
        return os.toByteArray();
    }

    // Write the least significant n bits of the integer to the buffer
    public void writeInteger(int value) throws IOException {        
        //if (width < 0 || width > 32) {
          //  throw new IllegalArgumentException("n must be between 0 and 32 ---> "+value);
       // }
        entries++;
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
        //if (width < 0 || width > 64) {
          //  throw new IllegalArgumentException("width must be between 0 and 64");
       // }
        entries++;
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

    @Override
    public void close() throws IOException {
        if (bitCount > 0) {
            // Pad the remaining bits with zeros on the right
            byte b = (byte) (bitBuffer << (8 - bitCount));
            byteWriter.writeByte(b);
        }
        os.flush();
        os.close();
    }

    public static BitPackedWriter forBuffer(Path path, long width) throws IOException {
        if (width < 0 || width > 64) {
            throw new IllegalArgumentException("n must be between 0 and 64 ---> "+width);
        }
        System.out.println(path+" ---> "+width);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ByteWriter fileWriter = new ByteWriter() {
            @Override
            public void writeByte(byte b) throws IOException {
                buffer.write(b);
            }
        };
        return new BitPackedWriter(path, fileWriter, width, buffer);
    }
    
    /*
    public static BitPackedWriter forFile(Path file, int width) throws IOException {
        BufferedOutputStream fos = new BufferedOutputStream( new FileOutputStream(file.toFile()));
        ByteWriter fileWriter = new ByteWriter() {
            @Override
            public void writeByte(byte b) throws IOException {
                fos.write(b);
            }
        };
        return new BitPackedWriter(fileWriter, width, fos);
    }*/
    
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

    /*
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
        return new BitPackedWriter(bufferWriter, width, fos);
    }*/

    /*
    public static void main(String[] args) throws IOException {
        BitPackedWriter fileWriter = BitPackedWriter.forFile(new File("output.dat"), 3);
        fileWriter.writeInteger(5); // 101
        fileWriter.writeInteger(3); // 011
        fileWriter.writeInteger(7); // 111
        fileWriter.writeInteger(2); // 111
        fileWriter.writeInteger(1); // 111
        fileWriter.writeInteger(1); // 111
        fileWriter.writeInteger(4); // 111
        fileWriter.close();

        
        ByteBuffer buffer = ByteBuffer.allocate(10);
        BitPackedWriter bufferWriter = BitPackedWriter.forByteBuffer(buffer,3);
        bufferWriter.writeInteger(5);
        bufferWriter.writeInteger(3);
        bufferWriter.writeInteger(7);
        bufferWriter.close();

        buffer.rewind();
        System.out.println(toBinaryString(buffer, " "));
    }*/
}
