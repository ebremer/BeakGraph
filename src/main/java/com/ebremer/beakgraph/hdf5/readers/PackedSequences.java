package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import com.ebremer.beakgraph.store.StoreGroup;
import com.ebremer.beakgraph.store.TypedArray;
import com.ebremer.beakgraph.store.TypedArrayView;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.Supplier;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.vocabulary.XSD;

/**
 * Packed reads (format v7, RDFARRY.md §3.6): a sequence that a tensor or
 * ragged class of {@code /.BG/arrays} holds reads its elements from the
 * class's {@code values} and {@code mask} instead of decoding a dictionary
 * literal per element. The terms are the very ones the dictionary gives
 * (the value is what §9.3 stores, and an {@code xsd:integer} or
 * {@code xsd:boolean} packs only in its canonical form); an inner level comes
 * back as a view over the same values. Record classes are read from their
 * canonical elements: their strings live in the dictionary anyway.
 *
 * <p>Nothing is read at open: a class's arrays open on the first read of one
 * of its sequences, and a row's values on the first read of one of its
 * elements. The row of a sequence is found by a binary search of a sample of
 * the class's {@code seq} (every {@value #SAMPLE}th id), then of one stretch
 * of it.
 */
final class PackedSequences {

    /** The {@code seq} ids kept in memory: one in this many. */
    static final int SAMPLE = 256;

    /** Whether readers opened from now on read packed sequences from their arrays ({@code beakgraph.arrays.read}, default true). */
    static boolean enabled() {
        return !"false".equalsIgnoreCase(System.getProperty("beakgraph.arrays.read"));
    }

    private final Supplier<StoreGroup> arrays;
    private final BitPackedUnSignedLongBuffer classOf;
    private final long firstSequenceObjectId;
    /** Per class number: its reader, or {@link #NOT_NUMERIC}; null slots open on first use. */
    private volatile Object[] classes;

    private static final Object NOT_NUMERIC = new Object();

    PackedSequences(Supplier<StoreGroup> arrays, BitPackedUnSignedLongBuffer classOf, long firstSequenceObjectId) {
        this.arrays = arrays;
        this.classOf = classOf;
        this.firstSequenceObjectId = firstSequenceObjectId;
    }

    /**
     * The elements of sequence {@code k} (of {@code length}) read from its
     * class's arrays, or null when it is not in a tensor or ragged class.
     */
    List<Node> row(long k, int length) {
        long c = classOf.get(k);
        if (c == 0) {
            return null;
        }
        PackedClass pc = classNumber((int) c);
        return (pc == null) ? null : new PackedRow(pc, firstSequenceObjectId + k, length);
    }

    /**
     * Sequence {@code k} when a tensor or ragged class holds it, its length from the class: in an
     * {@code -arrays-only} store (format v8) its canonical range is empty, and this is the sequence. Null for
     * one no such class holds.
     */
    List<Node> heldOnly(long k) {
        long c = classOf.get(k);
        if (c == 0) {
            return null;
        }
        PackedClass pc = classNumber((int) c);
        if (pc == null) {
            return null;
        }
        long id = firstSequenceObjectId + k;
        return new PackedRow(pc, id, pc.length(id));
    }

    /**
     * Whether the class holding sequence {@code k} shares values with the dictionary ({@code shared}, format
     * v8): a leaf or inner sequence of one of its rows has a dictionary row. A class that does not say is
     * taken to share.
     */
    boolean sharesDictionary(long k) {
        long c = classOf.get(k);
        PackedClass pc = (c == 0) ? null : classNumber((int) c);
        return pc == null || pc.shared;
    }

    /**
     * How many sequences the tensor and ragged classes hold: in a v8 store, those held only there, since no
     * class takes a sequence that keeps its elements. Opens every class.
     */
    long numericRows() {
        long n = 0;
        for (int c = 1; c < table().length; c++) {
            PackedClass pc = classNumber(c);
            if (pc != null) {
                n += pc.rows;
            }
        }
        return n;
    }

    /** The classes by number, each a name until opened; read from {@code /.BG/arrays} on the first call. */
    private Object[] table() {
        Object[] table = classes;
        if (table == null) {
            synchronized (this) {
                table = classes;
                if (table == null) {
                    StoreGroup g = arrays.get();
                    if (g == null) {
                        throw new IllegalStateException("Corrupt store: the literals section has sequenceArrays but there is no /.BG/arrays");
                    }
                    List<String> names = SequenceArrays.classOrder(g.groupNames());
                    table = new Object[names.size() + 1];
                    for (int i = 0; i < names.size(); i++) {
                        table[i + 1] = names.get(i);   // a name until opened
                    }
                    classes = table;
                }
            }
        }
        return table;
    }

    private PackedClass classNumber(int c) {
        Object[] table = table();
        if (c >= table.length) {
            throw new IllegalStateException("Corrupt store: sequenceArrays names class " + c + " of " + (table.length - 1));
        }
        Object slot = table[c];
        if (slot instanceof String name) {
            synchronized (this) {
                slot = table[c];
                if (slot instanceof String n) {
                    slot = PackedClass.open(n, arrays.get().group(n));
                    table[c] = (slot == null) ? NOT_NUMERIC : slot;
                }
            }
        }
        return (slot instanceof PackedClass pc) ? pc : null;
    }

    /** One tensor or ragged class: its leaf, the shape below its rows, and its arrays. */
    static final class PackedClass {
        final String name;
        final boolean tensor;
        final String datatype;
        /** The shape of one sequence below its outer length (the inner shape). */
        final int[] inner;
        final int per;
        final TypedArrayView values;
        final TypedArrayView mask;
        final TypedArrayView starts;
        final TypedArrayView seq;
        final long rows;
        /** Whether a leaf or inner sequence of a row has a dictionary row (format v8; true when unsaid). */
        final boolean shared;
        private volatile long[] samples;

        private PackedClass(String name, boolean tensor, String datatype, int[] inner, TypedArrayView values,
                            TypedArrayView mask, TypedArrayView starts, TypedArrayView seq, boolean shared) {
            this.name = name;
            this.shared = shared;
            this.tensor = tensor;
            this.datatype = datatype;
            this.inner = inner;
            int p = 1;
            for (int d : inner) {
                p = Math.multiplyExact(p, d);
            }
            this.per = p;
            this.values = values;
            this.mask = mask;
            this.starts = starts;
            this.seq = seq;
            this.rows = seq.shape()[0];
        }

        /** The class {@code name}, or null for a record class (read canonically). */
        static PackedClass open(String name, StoreGroup g) {
            String kind = g.stringAttribute("kind");
            if ("record".equals(kind)) {
                return null;
            }
            boolean tensor = "tensor".equals(kind);
            if (!tensor && !"ragged".equals(kind)) {
                throw new IllegalStateException("Corrupt store: /.BG/arrays/" + name + " has kind '" + kind + "'");
            }
            TypedArrayView values = g.typedArrayView("values");
            TypedArrayView seq = g.typedArrayView("seq");
            TypedArrayView starts = tensor ? null : g.typedArrayView("starts");
            if (values == null || seq == null || (!tensor && starts == null)) {
                throw new IllegalStateException("Corrupt store: /.BG/arrays/" + name + " lacks values, seq or starts");
            }
            long[] shape = values.shape();
            int skip = tensor ? 2 : 1;
            int[] inner = new int[shape.length - skip];
            for (int i = 0; i < inner.length; i++) {
                inner[i] = Math.toIntExact(shape[skip + i]);
            }
            boolean shared = !g.hasAttribute(SequenceArrays.SHARED) || g.attribute(SequenceArrays.SHARED) != 0;
            return new PackedClass(name, tensor, g.stringAttribute("datatype"), inner, values, g.typedArrayView("mask"),
                    starts, seq, shared);
        }

        /** The outer length of the sequence with object id {@code id}: the class's for a tensor, its starts' for a ragged one. */
        int length(long id) {
            if (tensor) {
                return Math.toIntExact(values.shape()[1]);
            }
            long[] s = (long[]) starts.readRows(rowOf(id), 2);
            return Math.toIntExact(s[1] - s[0]);
        }

        /**
         * The row of the sequence with object id {@code id}. When the class's ids run without a gap (seq
         * ascends, so its last minus its first is the row count less one), the row is {@code id - first};
         * else a binary search of the sample, then of one stretch of seq, which is kept: a scan reads the
         * sequences in id order, so it reads each stretch once.
         */
        long rowOf(long id) {
            long[] s = samples;
            if (s == null) {
                s = sample();
            }
            long from = contiguousFrom;
            if (from >= 0) {
                long row = id - from;
                if (row >= 0 && row < rows) {
                    return row;
                }
                throw notHeld(id);
            }
            Stretch t = stretch;
            if (t != null && id >= t.ids[0] && id <= t.ids[t.ids.length - 1]) {
                int j = java.util.Arrays.binarySearch(t.ids, id);
                if (j >= 0) {
                    return t.first + j;
                }
                throw notHeld(id);
            }
            int i = java.util.Arrays.binarySearch(s, id);
            if (i >= 0) {
                return (long) i * SAMPLE;
            }
            int block = -i - 2;   // the last sample below id
            if (block >= 0) {
                long first = (long) block * SAMPLE;
                long[] ids = (long[]) seq.readRows(first, Math.min(SAMPLE, rows - first));
                stretch = new Stretch(first, ids);
                int j = java.util.Arrays.binarySearch(ids, id);
                if (j >= 0) {
                    return first + j;
                }
            }
            throw notHeld(id);
        }

        private IllegalStateException notHeld(long id) {
            return new IllegalStateException("Corrupt store: sequenceArrays puts sequence " + id + " in /.BG/arrays/"
                    + name + ", whose seq does not hold it");
        }

        /** The stretch of seq read last, from row {@code first}; a racy cache of an immutable value. */
        private record Stretch(long first, long[] ids) {}

        private volatile Stretch stretch;
        /** The first id when the class's ids run without a gap, else -1; set with the sample. */
        private volatile long contiguousFrom = -1;

        private synchronized long[] sample() {
            if (samples == null) {
                long[] s = new long[Math.toIntExact((rows + SAMPLE - 1) / SAMPLE)];
                long step = (long) SAMPLE * 4096;   // read seq a stretch at a time
                long lastId = -1;
                for (long from = 0; from < rows; from += step) {
                    long[] part = (long[]) seq.readRows(from, Math.min(step, rows - from));
                    for (int k = 0; k < part.length; k += SAMPLE) {
                        s[Math.toIntExact((from + k) / SAMPLE)] = part[k];
                    }
                    lastId = part[part.length - 1];
                }
                // seq ascends strictly (one row a sequence, in id order): so a span of rows - 1 has no gap.
                if (rows > 0 && lastId - s[0] == rows - 1) {
                    contiguousFrom = s[0];
                }
                samples = s;
            }
            return samples;
        }

        /** A leaf as the dictionary gives it. */
        Node leaf(Object values, int at) {
            return switch (datatype) {
                case "http://www.w3.org/2001/XMLSchema#double" -> NodeFactory.createLiteralByValue(((double[]) values)[at]);
                case "http://www.w3.org/2001/XMLSchema#float" -> NodeFactory.createLiteralByValue(((float[]) values)[at]);
                case "http://www.w3.org/2001/XMLSchema#long" -> NodeFactory.createLiteralByValue(((long[]) values)[at]);
                case "http://www.w3.org/2001/XMLSchema#int" -> NodeFactory.createLiteralByValue(((int[]) values)[at]);
                case "http://www.w3.org/2001/XMLSchema#integer" ->
                        NodeFactory.createLiteralDT(Long.toString(((long[]) values)[at]), XSDDatatype.XSDinteger);
                case "http://www.w3.org/2001/XMLSchema#boolean" ->
                        NodeFactory.createLiteralDT(((boolean[]) values)[at] ? "true" : "false", XSDDatatype.XSDboolean);
                default -> throw new IllegalStateException("Corrupt store: /.BG/arrays/" + name + " has datatype '"
                        + datatype + "', which no class packs (" + XSD.getURI() + "...)");
            };
        }
    }

    /**
     * A packed sequence, or a level of one, as a lazy list: the values and
     * mask of the whole row are read on the first {@code get}, and each
     * element made once.
     */
    static final class PackedRow extends AbstractList<Node> implements RandomAccess {
        private final PackedClass cls;
        private final long objectId;
        private final int size;
        /** This level's shape: {@code size}, then the inner levels. */
        private final int[] shape;
        private final int per;
        private final int offset;
        // Benign races, as in the canonical lazy row: the same immutable slice and terms, each
        // published by one reference write (a slice's fields are final).
        private Slice slice;
        private Node[] read;
        private int hash;
        private boolean hashed;

        /** A row's values and mask (null without slots), read together. */
        private record Slice(Object values, boolean[] mask) {
        }

        PackedRow(PackedClass cls, long objectId, int size) {
            this(cls, objectId, size, withOuter(size, cls.inner), 0, null);
        }

        private PackedRow(PackedClass cls, long objectId, int size, int[] shape, int offset, Slice slice) {
            this.cls = cls;
            this.objectId = objectId;
            this.size = size;
            this.shape = shape;
            int p = 1;
            for (int i = 1; i < shape.length; i++) {
                p *= shape[i];
            }
            this.per = p;
            this.offset = offset;
            this.slice = slice;
        }

        private static int[] withOuter(int size, int[] inner) {
            int[] s = new int[1 + inner.length];
            s[0] = size;
            System.arraycopy(inner, 0, s, 1, inner.length);
            return s;
        }

        private Slice load() {
            long row = cls.rowOf(objectId);
            long first;
            long count;
            if (cls.tensor) {
                first = row;
                count = 1;
            } else {
                long[] s = (long[]) cls.starts.readRows(row, 2);
                first = s[0];
                count = s[1] - s[0];
            }
            Object v = cls.values.readRows(first, count);
            int n = TypedArray.checkedLength(cls.values.type(), v);
            if (n != (long) size * cls.per) {
                throw new IllegalStateException("Corrupt store: /.BG/arrays/" + cls.name + " row " + row + " holds " + n
                        + " values for sequence " + objectId + " of " + size + " elements");
            }
            Slice s = new Slice(v, (cls.mask == null) ? null : (boolean[]) cls.mask.readRows(first, count));
            slice = s;
            return s;
        }

        @Override
        public Node get(int index) {
            Objects.checkIndex(index, size);
            Node[] kept = read;
            if (kept == null) {
                kept = new Node[size];
                read = kept;
            }
            Node n = kept[index];
            if (n == null) {
                Slice s = slice;
                if (s == null) {
                    s = load();
                }
                int at = offset + index * per;
                if (shape.length == 1) {
                    n = (s.mask() != null && s.mask()[at]) ? com.ebremer.beakgraph.core.lib.SeqTerms.UNDEF : cls.leaf(s.values(), at);
                } else {
                    int[] below = java.util.Arrays.copyOfRange(shape, 1, shape.length);
                    n = com.ebremer.beakgraph.core.lib.SeqTerms.view(new PackedRow(cls, objectId, below[0], below, at, s));
                }
                kept[index] = n;
            }
            return n;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int hashCode() {
            if (!hashed) {
                hash = super.hashCode();
                hashed = true;
            }
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o == this || super.equals(o);
        }
    }
}
