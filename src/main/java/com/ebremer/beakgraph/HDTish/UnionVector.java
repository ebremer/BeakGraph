package com.ebremer.beakgraph.HDTish;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class UnionVector {
    private final List<Integer> offsets = new ArrayList<>();
    private final List<Byte> types = new ArrayList<>();
    private final ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
    private final DataOutputStream data = new DataOutputStream(dataStream);

    public enum DataType { STRING, INT, LONG, FLOAT, DOUBLE, SHORT }

    public void addString(String value) {
        offsets.add(dataStream.size());
        try {
            byte[] bytes = value.getBytes("UTF-8");
            data.writeInt(bytes.length);  // Store length first (4 bytes)
            data.write(bytes);            // Then the UTF-8 encoded bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write string to data buffer", e);
        }
        types.add((byte) DataType.STRING.ordinal());
    }

    public void addInt(int value) {
        offsets.add(dataStream.size());
        try {
            data.writeInt(value);  // 4 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write int to data buffer", e);
        }
        types.add((byte) DataType.INT.ordinal());
    }

    public void addLong(long value) {
        offsets.add(dataStream.size());
        try {
            data.writeLong(value);  // 8 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write long to data buffer", e);
        }
        types.add((byte) DataType.LONG.ordinal());
    }

    public void addFloat(float value) {
        offsets.add(dataStream.size());
        try {
            data.writeFloat(value);  // 4 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write float to data buffer", e);
        }
        types.add((byte) DataType.FLOAT.ordinal());
    }

    public void addDouble(double value) {
        offsets.add(dataStream.size());
        try {
            data.writeDouble(value);  // 8 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write double to data buffer", e);
        }
        types.add((byte) DataType.DOUBLE.ordinal());
    }

    public void addShort(short value) {
        offsets.add(dataStream.size());
        try {
            data.writeShort(value);  // 2 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write short to data buffer", e);
        }
        types.add((byte) DataType.SHORT.ordinal());
    }

    public int size() {
        return types.size();
    }

    public DataType getType(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        return DataType.values()[types.get(index)];
    }

    public Object getValue(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        DataType type = getType(index);
        int start = offsets.get(index);
        byte[] dataArray = dataStream.toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(dataArray).position(start);        
        switch (type) {
            case STRING -> {
                int len = buffer.getInt();
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                try {
                    return new String(bytes, "UTF-8");
                } catch (Exception e) {
                    throw new RuntimeException("Failed to decode string at index " + index, e);
                }
            }
            case INT -> {
                return buffer.getInt();
            }
            case LONG -> {
                return buffer.getLong();
            }
            case FLOAT -> {
                return buffer.getFloat();
            }
            case DOUBLE -> {
                return buffer.getDouble();
            }
            case SHORT -> {
                return buffer.getShort();
            }
            default -> throw new IllegalStateException("Unknown data type at index " + index);
        }
    }

    public int[] getOffsetBuffer() {
        int[] offsetArray = new int[offsets.size() + 1];
        for (int i = 0; i < offsets.size(); i++) {
            offsetArray[i] = offsets.get(i);
        }
        offsetArray[offsets.size()] = dataStream.size();
        return offsetArray;
    }

    public byte[] getTypeBuffer() {
        byte[] typeArray = new byte[types.size()];
        for (int i = 0; i < types.size(); i++) {
            typeArray[i] = types.get(i);
        }
        return typeArray;
    }

    public byte[] getDataBuffer() {
        return dataStream.toByteArray();
    }

    public static void main(String[] args) {
        UnionVector vector = new UnionVector();
        vector.addString("Hello");
        vector.addInt(42);
        vector.addDouble(3.14);
        System.out.println("Size: " + vector.size());
        for (int i = 0; i < vector.size(); i++) {
            System.out.println("Element " + i + ": Type=" + vector.getType(i) + ", Value=" + vector.getValue(i));
        }
        int[] offsets = vector.getOffsetBuffer();
        byte[] types = vector.getTypeBuffer();
        byte[] data = vector.getDataBuffer();
        System.out.println("Offsets: " + java.util.Arrays.toString(offsets));
        System.out.println("Types: " + java.util.Arrays.toString(types));
        System.out.println("Data length: " + data.length);
    }
}