package com.ebremer.beakgraph.hdtish;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FCDBuilder {
    private ByteBuffer buffer;
    private final int blockSize;
    private int stringsInCurrentBlock = 0;
    private String previousString = null;

    public FCDBuilder(int blockSize) {
        this.blockSize = blockSize;
        this.buffer = ByteBuffer.allocate(1024); // Initial capacity, e.g., 1KB
    }

    public void add(String item) {
        if (stringsInCurrentBlock == 0) {
            // Start of a new block: write plain string
            writePlainString(item);
            previousString = item;
            stringsInCurrentBlock = 1;
        } else {
            // Write front-coded string
            int prefixLength = commonPrefixLength(previousString, item);
            writeVByte(prefixLength);
            writeSuffix(item, prefixLength);
            previousString = item;
            stringsInCurrentBlock++;
            if (stringsInCurrentBlock == blockSize) {
                stringsInCurrentBlock = 0;
            }
        }
    }

    private void writePlainString(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        ensureCapacity(bytes.length + 1);
        buffer.put(bytes);
        buffer.put((byte) 0);
    }

    private void writeSuffix(String str, int prefixLength) {
        String suffix = str.substring(prefixLength);
        byte[] bytes = suffix.getBytes(StandardCharsets.UTF_8);
        ensureCapacity(bytes.length + 1);
        buffer.put(bytes);
        buffer.put((byte) 0);
    }

    private void writeVByte(int value) {
        while (true) {
            int byteValue = value & 0x7F;
            value >>= 7;
            if (value == 0) {
                ensureCapacity(1);
                buffer.put((byte) byteValue);
                break;
            } else {
                ensureCapacity(1);
                buffer.put((byte) (byteValue | 0x80));
            }
        }
    }

    private int commonPrefixLength(String s1, String s2) {
        int minLength = Math.min(s1.length(), s2.length());
        for (int i = 0; i < minLength; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return i;
            }
        }
        return minLength;
    }

    private void ensureCapacity(int required) {
        if (buffer.remaining() < required) {
            int newCapacity = Math.max(buffer.capacity() * 2, buffer.capacity() + required);
            ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
            buffer.flip(); // Prepare for reading
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
    }

    public ByteBuffer getBuffer() {
        buffer.flip(); // Prepare for reading
        return buffer;
    }
}