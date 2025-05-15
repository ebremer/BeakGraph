package com.ebremer.beakgraph.utils;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A class that represents an array of long values stored in native memory,
 * managed using the Foreign Function & Memory (FFM) API.
 *
 * This class implements AutoCloseable to ensure that the underlying native memory
 * is released when the LongArray is no longer needed. It's recommended to use
 * this class with a try-with-resources statement.
 */
public class IntegerArray implements AutoCloseable {

    private final Arena arena;
    private final MemorySegment segment;
    private final long length;

    public IntegerArray(long arrayLength) {
        if (arrayLength < 0) {
            throw new IllegalArgumentException("Array length cannot be negative: " + arrayLength);
        }
        this.length = arrayLength;
        this.arena = Arena.ofConfined();
        if (arrayLength == 0) {
            this.segment = arena.allocate(ValueLayout.JAVA_INT, 0);
        } else {
            this.segment = arena.allocate(ValueLayout.JAVA_INT, arrayLength);
        }
        clear();
    }

    public void set(int index, int value) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for length " + length);
        }
        segment.setAtIndex(ValueLayout.JAVA_INT, index, value);       
    }

    public int get(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for length " + length);
        }
        return segment.getAtIndex(ValueLayout.JAVA_INT, index);
    }

    public void clear() {
        if (length > 0) {
            segment.fill((byte) 0);
        }
    }

    public long getLength() {
        return length;
    }

    @Override
    public void close() {
        arena.close();
    }
    
    public static void main(String[] args) {
        try (IntegerArray a = new IntegerArray((long) Math.pow(2d, 35d))) {
            System.out.println(a.getLength());
        }
    }
}
