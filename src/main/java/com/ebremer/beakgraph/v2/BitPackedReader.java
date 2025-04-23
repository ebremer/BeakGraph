package com.ebremer.beakgraph.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class BitPackedReader {
    private ByteBuffer buffer;
    private final int b;

    public BitPackedReader(ByteBuffer buffer, int b) {
        this.buffer = buffer;
        this.b = b;
    }

    public int readNthValue(int n) {
        int bitPos = n * b;
        int byteIdx = bitPos / 8;
        if (byteIdx >= buffer.capacity()) {
            throw new IndexOutOfBoundsException("n is out of range");
        }
        return readInteger(bitPos);
    }

    private int readInteger(int bitPos) {
        int value = 0;
        for (int i = 0; i < b; i++) {
            int currentBitPos = bitPos + i;
            int byteIdx = currentBitPos / 8;
            int bitIdx = currentBitPos % 8;
            byte currentByte = buffer.get(byteIdx);
            int bit = (currentByte >> (7 - bitIdx)) & 1;
            value = (value << 1) | bit;
        }
        return value;
    }
    
    public static BitPackedReader fromFile(String filePath, int b) throws IOException {
        FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ);
        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        return new BitPackedReader(buffer, b);
    }

    public static void main(String[] args) throws IOException {
        byte[] data = {(byte) 0b10101111, (byte) 0b10000000}; // 10101111 10000000
  
        BitPackedReader reader = BitPackedReader.fromFile("output.dat", 3);
        System.out.println("n=0: " + reader.readNthValue(0));
        System.out.println("n=1: " + reader.readNthValue(1));
        System.out.println("n=2: " + reader.readNthValue(2));
        System.out.println("n=2: " + reader.readNthValue(3));
        System.out.println("n=2: " + reader.readNthValue(4));
        System.out.println("n=2: " + reader.readNthValue(5));      
        System.out.println("n=2: " + reader.readNthValue(6));
        
        /*
        BitPackedReader reader = new BitPackedReader(ByteBuffer.wrap(data), 3);
        System.out.println("n=0: " + reader.readNthValue(0)); // Should be 5
        System.out.println("n=1: " + reader.readNthValue(1)); // Should be 3
        System.out.println("n=2: " + reader.readNthValue(2)); // Should be 7
        System.out.println("n=2: " + reader.readNthValue(3));
        System.out.println("n=2: " + reader.readNthValue(4));
        System.out.println("n=2: " + reader.readNthValue(5));
*/
    }
}