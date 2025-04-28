package com.ebremer.beakgraph.hdtish;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Buffer {
    private ByteArrayOutputStream stream;
    private List<EntryMetadata> entries;

    public Buffer() {
        stream = new ByteArrayOutputStream();
        entries = new ArrayList<>();
    }

    public void add(String string) {
        int startPosition = stream.size();
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        stream.write(bytes, 0, bytes.length);
        entries.add(new EntryMetadata(startPosition, DataType.STRING));
    }

    public void add(Float f) {
        int startPosition = stream.size();
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
        buffer.putFloat(f);
        stream.write(buffer.array(), 0, Float.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.FLOAT));
    }

    public void add(Double d) {
        int startPosition = stream.size();
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(d);
        stream.write(buffer.array(), 0, Double.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.DOUBLE));
    }

    public void add(int n) {
        int startPosition = stream.size();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(n);
        stream.write(buffer.array(), 0, Integer.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.INTEGER));
    }

    public void add(long n) {
        int startPosition = stream.size();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(n);
        stream.write(buffer.array(), 0, Long.BYTES);
        entries.add(new EntryMetadata(startPosition, DataType.LONG));
    }

    public int size() {
        return stream.size();
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