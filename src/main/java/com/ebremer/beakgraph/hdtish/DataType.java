package com.ebremer.beakgraph.hdtish;

import java.math.BigDecimal;
import java.math.BigInteger;

public enum DataType {
    STRING(String.class, -1),
    BYTE(Byte.class, 1),
    BOOLEAN(Boolean.class, 1),
    SHORT(Short.class, 2),
    INT(Integer.class, 4),
    LONG(Long.class, 8),
    FLOAT(Float.class, 4),
    DOUBLE(Double.class, 8),
    BIG_INTEGER(BigInteger.class, -1),
    BIG_DECIMAL(BigDecimal.class, -1);

    private final Class<?> clazz;
    private final int sizeInBytes;

    DataType(Class<?> clazz, int sizeInBytes) {
        this.clazz = clazz;
        this.sizeInBytes = sizeInBytes;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public boolean isIntegerType() {
        return this == BYTE || this == SHORT || this == INT || this == LONG || this == BIG_INTEGER;
    }

    public boolean isFloatingPointType() {
        return this == FLOAT || this == DOUBLE || this == BIG_DECIMAL;
    }
}
