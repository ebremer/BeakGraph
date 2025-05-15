package com.ebremer.beakgraph.HDTish;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Buffer {
    private ByteArrayOutputStream mainbuffer;
    private List<EntryMetadata> entries;

    public Buffer() {
        mainbuffer = new ByteArrayOutputStream();
        entries = new ArrayList<>();
    }

    public void add(String element) {
        int startPosition = mainbuffer.size();
        byte[] bytes = element.getBytes(StandardCharsets.UTF_8);
        mainbuffer.write(bytes, 0, bytes.length);
        entries.add(new EntryMetadata(startPosition, DataType.STRING));
    }

    public void add(float f) {
        int startPosition = mainbuffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
        buffer.putFloat(f);
        mainbuffer.write(buffer.array(), 0, Float.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.FLOAT));
    }

    public void add(double d) {
        int startPosition = mainbuffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(d);
        mainbuffer.write(buffer.array(), 0, Double.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.DOUBLE));
    }

    public void add(int n) {
        int startPosition = mainbuffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(n);
        mainbuffer.write(buffer.array(), 0, Integer.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.INTEGER));
    }

    public void add(long n) {
        int startPosition = mainbuffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(n);
        mainbuffer.write(buffer.array(), 0, Long.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.LONG));
    }

    public int size() {
        return mainbuffer.size();
    }

    private enum DataType {
        STRING,
        FLOAT,
        DOUBLE,
        INTEGER,
        LONG
    }

    private record EntryMetadata(int startPosition, DataType type) {}
}