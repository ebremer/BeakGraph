package com.ebremer.beakgraph.hdtish;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Buffer {
    private ByteArrayOutputStream buffer;
    private List<EntryMetadata> entries;

    public Buffer() {
        buffer = new ByteArrayOutputStream();
        entries = new ArrayList<>();
    }

    public void add(String string) {
        int startPosition = buffer.size();
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        buffer.write(bytes, 0, bytes.length);
        entries.add(new EntryMetadata(startPosition, DataType.STRING));
    }

    public void add(Float f) {
        int startPosition = buffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
        buffer.putFloat(f);
        this.buffer.write(buffer.array(), 0, Float.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.FLOAT));
    }

    public void add(Double d) {
        int startPosition = buffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(d);
        this.buffer.write(buffer.array(), 0, Double.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.DOUBLE));
    }

    public void add(int n) {
        int startPosition = buffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(n);
        this.buffer.write(buffer.array(), 0, Integer.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.INTEGER));
    }

    public void add(long n) {
        int startPosition = buffer.size();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(n);
        this.buffer.write(buffer.array(), 0, Long.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.LONG));
    }

    public int size() {
        return buffer.size();
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