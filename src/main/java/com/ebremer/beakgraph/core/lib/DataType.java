package com.ebremer.beakgraph.core.lib;

import java.math.BigDecimal;
import java.math.BigInteger;

public enum DataType {
    BNODE(String.class, -1),
    IRI(String.class, -1),
    STRING(String.class, -1),
    BYTE(Byte.class, 1),
    BOOLEAN(Boolean.class, 1),
    SHORT(Short.class, 2),
    INTEGER(Integer.class, 4),
    LONG(Long.class, 8),
    FLOAT(Float.class, 4),
    DOUBLE(Double.class, 8),
    BIG_INTEGER(BigInteger.class, -1),
    BIG_DECIMAL(BigDecimal.class, -1),
    // A document-relative IRI (no scheme), e.g. <> or <sibling.png>. Stored
    // verbatim and resolved at query time against the file's serving URL.
    RELATIVE_IRI(String.class, -1),
    // RDF 1.2 triple term <<( s p o )>> (format v5). The row's offsets entry is
    // the term's ordinal k in the tripleTerms store: entries [3k, 3k+2] hold the
    // (s, p, o) component ids - s in the entity space, p in the predicate space,
    // o in the object space (entities + literals section, so nested triple terms
    // resolve recursively). Ordinals are the on-disk encoding: append only,
    // never reorder. This is value 14 of 15 before the datatypes column widens
    // from 5 to 6 bits.
    TRIPLE_TERM(org.apache.jena.graph.Triple.class, -1);

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
        return this == BYTE || this == SHORT || this == INTEGER || this == LONG || this == BIG_INTEGER;
    }

    public boolean isFloatingPointType() {
        return this == FLOAT || this == DOUBLE || this == BIG_DECIMAL;
    }
}
