package com.ebremer.beakgraph.v2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class UnionVector {
    private List<Integer> offsets = new ArrayList<>();
    private List<Byte> types = new ArrayList<>();
    private ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
    private DataOutputStream data = new DataOutputStream(dataStream);

    // Enum to represent supported data types with implicit ordinal values matching the requirement
    public enum DataType {
        STRING,  // 0
        INT,     // 1
        LONG,    // 2
        FLOAT,   // 3
        DOUBLE,  // 4
        SHORT    // 5
    }

    // Methods to add elements of each supported type

    /** Adds a String element to the union vector */
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

    /** Adds an int element to the union vector */
    public void addInt(int value) {
        offsets.add(dataStream.size());
        try {
            data.writeInt(value);  // 4 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write int to data buffer", e);
        }
        types.add((byte) DataType.INT.ordinal());
    }

    /** Adds a long element to the union vector
     * @param value */
    public void addLong(long value) {
        offsets.add(dataStream.size());
        try {
            data.writeLong(value);  // 8 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write long to data buffer", e);
        }
        types.add((byte) DataType.LONG.ordinal());
    }

    /** Adds a float element to the union vector */
    public void addFloat(float value) {
        offsets.add(dataStream.size());
        try {
            data.writeFloat(value);  // 4 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write float to data buffer", e);
        }
        types.add((byte) DataType.FLOAT.ordinal());
    }

    /** Adds a double element to the union vector */
    public void addDouble(double value) {
        offsets.add(dataStream.size());
        try {
            data.writeDouble(value);  // 8 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write double to data buffer", e);
        }
        types.add((byte) DataType.DOUBLE.ordinal());
    }

    /** Adds a short element to the union vector */
    public void addShort(short value) {
        offsets.add(dataStream.size());
        try {
            data.writeShort(value);  // 2 bytes
        } catch (IOException e) {
            throw new RuntimeException("Failed to write short to data buffer", e);
        }
        types.add((byte) DataType.SHORT.ordinal());
    }

    // Accessor methods

    /** Returns the number of elements in the union vector
     * @return  */
    public int size() {
        return types.size();
    }

    /** Returns the type of the element at the specified index
     * @param index
     * @return  */
    public DataType getType(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        return DataType.values()[types.get(index)];
    }

    /** Returns the value at the specified index as an Object, based on its type */
    public Object getValue(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
        }
        DataType type = getType(index);
        int start = offsets.get(index);
        byte[] dataArray = dataStream.toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(dataArray).position(start);
        
        switch (type) {
            case STRING:
                int len = buffer.getInt();
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                try {
                    return new String(bytes, "UTF-8");
                } catch (Exception e) {
                    throw new RuntimeException("Failed to decode string at index " + index, e);
                }
            case INT:
                return buffer.getInt();
            case LONG:
                return buffer.getLong();
            case FLOAT:
                return buffer.getFloat();
            case DOUBLE:
                return buffer.getDouble();
            case SHORT:
                return buffer.getShort();
            default:
                throw new IllegalStateException("Unknown data type at index " + index);
        }
    }

    /** Returns the offset buffer as an int array with size n+1, where n is the number of elements */
    public int[] getOffsetBuffer() {
        int[] offsetArray = new int[offsets.size() + 1];
        for (int i = 0; i < offsets.size(); i++) {
            offsetArray[i] = offsets.get(i);
        }
        offsetArray[offsets.size()] = dataStream.size();
        return offsetArray;
    }

    /** Returns the datatype buffer as a byte array */
    public byte[] getTypeBuffer() {
        byte[] typeArray = new byte[types.size()];
        for (int i = 0; i < types.size(); i++) {
            typeArray[i] = types.get(i);
        }
        return typeArray;
    }

    /** Returns the data buffer as a byte array */
    public byte[] getDataBuffer() {
        return dataStream.toByteArray();
    }

    // Optional: Example usage
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