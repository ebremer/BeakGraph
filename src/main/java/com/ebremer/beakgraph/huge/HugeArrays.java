package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Comparator;
import org.apache.jena.graph.Node;

/**
 * {@code -arrays} for the disk engines (format v7, RDFARRY.md §3.5, §3.5a),
 * with the in-memory engines' classifier and class writer, so that their
 * classes, values and layout are the same. The literals encode reaches the
 * sequences in id order: each top-level one (the object of a quad, as the
 * merge marked it) is probed, counted in its bucket and, when it packs,
 * spilled with its leaves. After the encode the counts decide the classes;
 * each sequence's class number goes to {@code sequenceArrays}; and at assembly
 * the spilled rows are sorted by (class, id) and each class is written from a
 * file of its own rows, one pass per array, so no class need fit in memory.
 */
final class HugeArrays implements AutoCloseable {

    /** One spilled row: its bucket (during the encode) or class (in the sort), object id, outer length or items, and leaves. */
    record Spilled(int key, long objectId, int count, Object[] leaves) {

        /** About what the row holds in memory: the sorter spills on it. */
        long bytes() {
            long n = 64 + 24L * leaves.length;
            for (Object v : leaves) {
                if (v instanceof String s) {
                    n += 40 + 2L * s.length();
                } else if (v instanceof String[] pair) {
                    n += 96 + 2L * (pair[0].length() + pair[1].length());
                }
            }
            return n;
        }
    }

    private static final int NULL = 0;
    private static final int DOUBLE = 1;
    private static final int FLOAT = 2;
    private static final int LONG = 3;
    private static final int BOOLEAN = 4;
    private static final int STRING = 5;
    private static final int LANG = 6;

    /** A row and its leaves exactly: a float's bits as they are (NaN payloads, -0.0), strings as UTF-8. */
    static final ExternalSorter.Codec<Spilled> CODEC = new ExternalSorter.Codec<>() {
        @Override
        public void write(DataOutput out, Spilled r) throws IOException {
            out.writeInt(r.key());
            out.writeLong(r.objectId());
            out.writeInt(r.count());
            out.writeInt(r.leaves().length);
            for (Object v : r.leaves()) {
                switch (v) {
                    case null -> out.writeByte(NULL);
                    case Double d -> {
                        out.writeByte(DOUBLE);
                        out.writeLong(Double.doubleToRawLongBits(d));
                    }
                    case Float f -> {
                        out.writeByte(FLOAT);
                        out.writeInt(Float.floatToRawIntBits(f));
                    }
                    case Long l -> {
                        out.writeByte(LONG);
                        out.writeLong(l);
                    }
                    case Boolean b -> {
                        out.writeByte(BOOLEAN);
                        out.writeBoolean(b);
                    }
                    case String s -> {
                        out.writeByte(STRING);
                        string(out, s);
                    }
                    case String[] pair -> {
                        out.writeByte(LANG);
                        string(out, pair[0]);
                        string(out, pair[1]);
                    }
                    default -> throw new IllegalArgumentException("a leaf of " + v.getClass());
                }
            }
        }

        private void string(DataOutput out, String s) throws IOException {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);   // packed strings hold no lone surrogate
            out.writeInt(utf8.length);
            out.write(utf8);
        }

        private String string(DataInput in) throws IOException {
            byte[] utf8 = new byte[in.readInt()];
            in.readFully(utf8);
            return new String(utf8, StandardCharsets.UTF_8);
        }

        @Override
        public Spilled read(DataInput in) throws IOException {
            int key = in.readInt();   // EOFException at the end of a run
            long objectId = in.readLong();
            int count = in.readInt();
            Object[] leaves = new Object[in.readInt()];
            for (int i = 0; i < leaves.length; i++) {
                int tag = in.readByte();
                leaves[i] = switch (tag) {
                    case NULL -> null;
                    case DOUBLE -> Double.longBitsToDouble(in.readLong());
                    case FLOAT -> Float.intBitsToFloat(in.readInt());
                    case LONG -> in.readLong();
                    case BOOLEAN -> in.readBoolean();
                    case STRING -> string(in);
                    case LANG -> new String[]{string(in), string(in)};
                    default -> throw new IOException("leaf tag " + tag);
                };
            }
            return new Spilled(key, objectId, count, leaves);
        }
    };

    private static final Comparator<Spilled> BY_CLASS_THEN_ID =
            Comparator.comparingInt(Spilled::key).thenComparingLong(Spilled::objectId);

    private final SequenceArrays.Options options;
    private final BitSet topLevel;
    private final long firstObjectId;
    private final Path workDir;
    private final int mergeFanIn;
    private final SequenceArrays.Classifier classifier;
    /** The packable rows in id order, with their buckets. */
    private final RecordFile<Spilled> rows;
    /** Each sequence's bucket + 1, 0 when it does not pack, in sequence order. */
    private final RecordFile<Long> bucketOfSequence;
    private SequenceArrays.Decision decision;

    /**
     * @param topLevel      which sequences (by their place in the section) are the object of a quad
     * @param firstObjectId the object id of the section's first sequence
     */
    HugeArrays(SequenceArrays.Options options, BitSet topLevel, long firstObjectId, Path workDir, int mergeFanIn) {
        this.options = options;
        this.topLevel = topLevel;
        this.firstObjectId = firstObjectId;
        this.workDir = workDir;
        this.mergeFanIn = Math.max(2, mergeFanIn);
        this.classifier = new SequenceArrays.Classifier(options);
        this.rows = new RecordFile<>(workDir.resolve("arrays.rows"), CODEC);
        this.bucketOfSequence = new RecordFile<>(workDir.resolve("arrays.buckets"), HugeRecords.VAR_LONG_CODEC);
    }

    /** The encode's callback for sequence {@code k} of the section, in order. */
    void onSequence(long k, Node seq) throws IOException {
        long code = 0;
        if (topLevel.get(Math.toIntExact(k))) {
            long id = firstObjectId + k;
            Object row = SequenceArrays.probeAny(id, seq, options);
            if (row != null) {
                int bucket = classifier.add(row);
                rows.append(new Spilled(bucket, id, SequenceArrays.countOf(row), SequenceArrays.leavesOf(row)));
                code = bucket + 1L;
            }
        }
        bucketOfSequence.append(code);
    }

    /**
     * After the encode: decides the classes, and returns each sequence's class
     * number for {@code sequenceArrays}, or null when no sequence packs (a store
     * then has neither it nor {@code /.BG/arrays}).
     */
    SpillBitPackedBuffer classNumbers() throws IOException {
        rows.finish();
        bucketOfSequence.finish();
        decision = classifier.decide();
        if (decision.classes().isEmpty()) {
            return null;
        }
        int[] classOf = decision.classOfBucket();
        SpillBitPackedBuffer out = new SpillBitPackedBuffer(workDir.resolve("sequenceArrays.store"), 64);
        try (var it = bucketOfSequence.read()) {
            while (it.hasNext()) {
                long code = it.next();
                out.writeLong((code == 0) ? 0 : classOf[(int) (code - 1)]);
            }
        }
        out.complete();
        return out;
    }

    /** Writes {@code arrays/} under {@code bg} (the {@code .BG} group), the classes in class-number order. */
    void write(WritableGroup bg) throws IOException {
        if (decision == null || decision.classes().isEmpty()) {
            return;
        }
        int[] classOf = decision.classOfBucket();
        long budget = SorterProvider.defaultTermSpillBytes(1);
        try (ExternalSorter<Spilled> sorter = new ExternalSorter<>(workDir, "arrays", CODEC, BY_CLASS_THEN_ID,
                Integer.MAX_VALUE, mergeFanIn, Spilled::bytes, budget)) {
            try (var it = rows.read()) {
                while (it.hasNext()) {
                    Spilled r = it.next();
                    int c = classOf[r.key()];
                    if (c > 0) {
                        sorter.add(new Spilled(c, r.objectId(), r.count(), r.leaves()));
                    }
                }
            }
            rows.delete();
            WritableGroup arrays = bg.putGroup(Params.ARRAYS);
            try (var sorted = sorter.sorted()) {
                Spilled next = sorted.hasNext() ? sorted.next() : null;
                for (int c = 1; c <= decision.classes().size(); c++) {
                    SequenceArrays.ClassDef def = decision.classes().get(c - 1);
                    RecordFile<Spilled> file = new RecordFile<>(workDir.resolve("arrays.class." + c), CODEC);
                    try {
                        while (next != null && next.key() == c) {
                            file.append(next);
                            next = sorted.hasNext() ? sorted.next() : null;
                        }
                        file.finish();
                        SequenceArrays.writeClass(def, () -> rowsOf(file, def), arrays, options.compress(), options.timeUnit());
                    } finally {
                        file.close();
                    }
                }
            }
        }
    }

    /** A pass over a class's file, as the rows the class writer takes. */
    private static SequenceArrays.RowSource.Rows rowsOf(RecordFile<Spilled> file, SequenceArrays.ClassDef def) {
        RecordSorter.SortedCursor<Spilled> cursor;
        try {
            cursor = file.read();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SequenceArrays.RowSource.Rows() {
            @Override
            public boolean hasNext() {
                return cursor.hasNext();
            }

            @Override
            public Object next() {
                Spilled s = cursor.next();
                return SequenceArrays.rowOf(def, s.objectId(), s.count(), s.leaves());
            }

            @Override
            public void close() {
                cursor.close();
            }
        };
    }

    @Override
    public void close() {
        rows.close();
        bucketOfSequence.close();
    }
}
