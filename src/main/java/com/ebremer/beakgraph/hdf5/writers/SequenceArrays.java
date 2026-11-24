package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.beakgraph.store.TypedArray;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.jena.graph.Node;
import org.apache.jena.vocabulary.XSD;

/**
 * The native arrays of {@code /.BG/arrays} (format v7, RDFARRY.md §3.5): the
 * numeric sequences of a store, laid out a second time as typed N-D arrays
 * that array tools read with no knowledge of RDF. The canonical elements of
 * the literals section stay complete; these are a copy.
 *
 * <p>Classification depends on the store's set of sequences alone, never on
 * input order or the engine:
 * <ul>
 *   <li>only top-level sequences (objects of a quad) are classified;</li>
 *   <li>every leaf is a literal of one packable datatype, or a slot, and a
 *       slot only where a leaf belongs; the levels below the outermost are
 *       rectangular; there are at least {@link Options#minLeaves} leaves;</li>
 *   <li>the key is (leaf datatype, inner shape); an outer length shared by at
 *       least {@link Options#minRows} sequences of a key is a tensor class of
 *       its own, and the key's other rows share one ragged class.</li>
 * </ul>
 * Classes are named by kind, datatype and shape. A top-level sequence that
 * no tensor or ragged class takes may be a record (§3.5a, {@link RecordArrays}):
 * a class {@code rec_<n>} per signature that at least {@code minRows}
 * sequences share. Classes are numbered 1, 2, ...: the tensor and ragged
 * classes in name order, then {@code rec_1}, {@code rec_2}, ...
 * ({@link #classOrder}); {@link #classes} gives each sequence row its class,
 * 0 when unpacked.
 */
public final class SequenceArrays {

    /**
     * {@code -arrays-time-unit}: what a record class's {@code xsd:dateTime}
     * column counts since 1970-01-01T00:00:00, one unit per store. A value
     * finer than the unit does not pack.
     */
    public enum TimeUnit {
        S("s", "seconds", 0, 1L),
        MS("ms", "milliseconds", 3, 1_000L),
        US("us", "microseconds", 6, 1_000_000L),
        NS("ns", "nanoseconds", 9, 1_000_000_000L);

        final String flag;
        /** The unit's CF name. */
        final String cf;
        /** The fractional digits of a second it holds. */
        final int digits;
        final long perSecond;

        TimeUnit(String flag, String cf, int digits, long perSecond) {
            this.flag = flag;
            this.cf = cf;
            this.digits = digits;
            this.perSecond = perSecond;
        }

        /** The unit {@code -arrays-time-unit} names: s, ms, us or ns. */
        public static TimeUnit of(String flag) {
            for (TimeUnit t : values()) {
                if (t.flag.equals(flag)) {
                    return t;
                }
            }
            throw new IllegalArgumentException("-arrays-time-unit is s, ms, us or ns, not '" + flag + "'");
        }

        /** The unit of a dateTime column's CF {@code units}, or null. */
        static TimeUnit ofUnits(String units) {
            for (TimeUnit t : values()) {
                if (RecordArrays.units(t).equals(units)) {
                    return t;
                }
            }
            return null;
        }
    }

    /** {@code -arrays}: on with these settings, or off (null). */
    public record Options(int minRows, int minLeaves, boolean compress, TimeUnit timeUnit) {
        public static final Options DEFAULT = new Options(16, 2, false, TimeUnit.US);

        public Options {
            if (minRows < 1 || minLeaves < 1) {
                throw new IllegalArgumentException("-arrays-min-rows and -arrays-min must be at least 1");
            }
            java.util.Objects.requireNonNull(timeUnit, "timeUnit");
        }

        /** Microseconds, the default time unit. */
        public Options(int minRows, int minLeaves, boolean compress) {
            this(minRows, minLeaves, compress, TimeUnit.US);
        }
    }

    /** A packable leaf datatype: its array type, its name in class names, and the XSD datatype it rebuilds. */
    enum Leaf {
        DOUBLE(TypedArray.Type.F64, "f64", XSD.xdouble.getURI()),
        FLOAT(TypedArray.Type.F32, "f32", XSD.xfloat.getURI()),
        INTEGER(TypedArray.Type.I64, "i64", XSD.integer.getURI()),
        LONG(TypedArray.Type.I64, "long", XSD.xlong.getURI()),
        INT(TypedArray.Type.I32, "i32", XSD.xint.getURI()),
        BOOLEAN(TypedArray.Type.BOOL, "bool", XSD.xboolean.getURI());

        final TypedArray.Type type;
        final String tag;
        final String datatype;

        Leaf(TypedArray.Type type, String tag, String datatype) {
            this.type = type;
            this.tag = tag;
            this.datatype = datatype;
        }

        static Leaf of(String datatypeUri) {
            for (Leaf l : values()) {
                if (l.datatype.equals(datatypeUri)) {
                    return l;
                }
            }
            return null;
        }

        /** The leaf its class-name tag ({@code f64}, {@code long}, ...) names, or null. */
        static Leaf ofTag(String tag) {
            for (Leaf l : values()) {
                if (l.tag.equals(tag)) {
                    return l;
                }
            }
            return null;
        }
    }

    private static final Pattern CANONICAL_INTEGER = Pattern.compile("-?(0|[1-9][0-9]*)");

    /** The value a packable literal leaf packs to, or null when it does not pack losslessly. */
    static Object packedValue(Node n, Leaf leaf) {
        Object v;
        try {
            v = n.getLiteralValue();
        } catch (RuntimeException ill) {
            return null;   // ill-typed: kept term-exact by the strings route, never packed
        }
        String lex = n.getLiteralLexicalForm();
        return switch (leaf) {
            case DOUBLE -> (v instanceof Number x) ? (Object) x.doubleValue() : null;
            case FLOAT -> (v instanceof Number x) ? (Object) x.floatValue() : null;
            case LONG, INT -> (v instanceof Number x) ? (Object) x.longValue() : null;
            case INTEGER -> {
                if (!CANONICAL_INTEGER.matcher(lex).matches()) {
                    yield null;
                }
                try {
                    yield Long.parseLong(lex);
                } catch (NumberFormatException tooBig) {
                    yield null;
                }
            }
            case BOOLEAN -> lex.equals("true") ? Boolean.TRUE : lex.equals("false") ? Boolean.FALSE : null;
        };
    }

    /** One packable sequence: its object id, outer length, inner shape, leaves (row-major; null for a slot). */
    record Row(long objectId, Leaf leaf, int length, int[] inner, Object[] leaves) {
        boolean hasSlot() {
            for (Object o : leaves) {
                if (o == null) return true;
            }
            return false;
        }
    }

    /**
     * {@code seq} as a row, or null when it does not pack: a non-rectangular
     * level, a slot where a row belongs, leaves of more than one datatype, an
     * unpackable leaf, no typed leaf, or fewer than {@code minLeaves} leaves.
     */
    static Row probe(long objectId, Node seq, int minLeaves) {
        int[] shape = shapeOf(seq);
        if (shape == null || shape.length == 0) {
            return null;
        }
        long count = 1;
        for (int d : shape) {
            count *= d;
        }
        if (count < minLeaves || count > Integer.MAX_VALUE - 8) {
            return null;
        }
        Object[] leaves = new Object[(int) count];
        Leaf[] leaf = {null};
        int[] at = {0};
        if (!flatten(seq, leaves, at, leaf) || leaf[0] == null) {
            return null;
        }
        return new Row(objectId, leaf[0], shape[0], Arrays.copyOfRange(shape, 1, shape.length), leaves);
    }

    /** The rectangular shape of {@code n} ({@code []} for a leaf or a slot), or null when a level is ragged. */
    private static int[] shapeOf(Node n) {
        if (!n.isNodeSeq()) {
            return new int[0];
        }
        List<Node> el = n.getSeq();
        int[] inner = null;
        for (Node e : el) {
            int[] s = shapeOf(e);
            if (s == null) {
                return null;
            }
            if (inner == null) {
                inner = s;
            } else if (!Arrays.equals(inner, s)) {
                return null;   // ragged below the outermost level, or a slot where a row belongs
            }
        }
        int[] out = new int[1 + ((inner == null) ? 0 : inner.length)];
        out[0] = el.size();
        if (inner != null) {
            System.arraycopy(inner, 0, out, 1, inner.length);
        } else if (el.isEmpty()) {
            return out;   // () : no inner levels to agree on
        }
        return out;
    }

    private static boolean flatten(Node n, Object[] leaves, int[] at, Leaf[] leaf) {
        if (n.isNodeSeq()) {
            for (Node e : n.getSeq()) {
                if (!flatten(e, leaves, at, leaf)) {
                    return false;
                }
            }
            return true;
        }
        if (n.isNodeUndef()) {
            leaves[at[0]++] = null;
            return true;
        }
        if (!n.isLiteral()) {
            return false;
        }
        Leaf l = Leaf.of(n.getLiteralDatatypeURI());
        if (l == null || (leaf[0] != null && leaf[0] != l)) {
            return false;
        }
        Object v = packedValue(n, l);
        if (v == null) {
            return false;
        }
        leaf[0] = l;
        leaves[at[0]++] = v;
        return true;
    }

    /** A class: its name, kind, leaf, row shape (tensor) or inner shape (ragged), and rows in id order. */
    record Class(String name, boolean tensor, Leaf leaf, int length, int[] inner, List<Row> rows) {}

    private final List<Class> classes;
    private final List<RecordArrays.RecClass> records;
    private final long[] classOfSequence;
    private final TimeUnit timeUnit;

    private SequenceArrays(List<Class> classes, List<RecordArrays.RecClass> records, long[] classOfSequence,
                           TimeUnit timeUnit) {
        this.classes = classes;
        this.records = records;
        this.classOfSequence = classOfSequence;
        this.timeUnit = timeUnit;
    }

    /**
     * Classifies the sequence suffix of the sorted literals section
     * {@code literals}: sequence {@code k} is {@code literals[first + k]}, its
     * object id {@code maxEntityId + first + k + 1}. {@code topLevel} says which
     * sequences are objects of a quad.
     */
    public static SequenceArrays classify(List<Node> literals, long maxEntityId, java.util.function.Predicate<Node> topLevel,
                                          Options options) {
        int first = literals.size();
        while (first > 0 && literals.get(first - 1).isNodeSeq()) {
            first--;
        }
        int numSequences = literals.size() - first;
        // key -> length -> rows, every map in a fixed order
        Map<String, TreeMap<Integer, List<Row>>> byKey = new TreeMap<>();
        Map<String, Row> keyExample = new HashMap<>();
        List<RecordArrays.RecRow> recordCandidates = new ArrayList<>();
        for (int k = 0; k < numSequences; k++) {
            Node seq = literals.get(first + k);
            if (!topLevel.test(seq)) {
                continue;
            }
            long objectId = maxEntityId + first + k + 1;
            Row row = probe(objectId, seq, options.minLeaves());
            if (row == null) {
                RecordArrays.RecRow rec = RecordArrays.probe(objectId, seq, options.timeUnit(), options.minLeaves());
                if (rec != null) {
                    recordCandidates.add(rec);
                }
                continue;
            }
            String key = row.leaf().name() + Arrays.toString(row.inner());
            byKey.computeIfAbsent(key, x -> new TreeMap<>()).computeIfAbsent(row.length(), x -> new ArrayList<>()).add(row);
            keyExample.putIfAbsent(key, row);
        }
        TreeMap<String, Class> named = new TreeMap<>();
        for (Map.Entry<String, TreeMap<Integer, List<Row>>> e : byKey.entrySet()) {
            Row example = keyExample.get(e.getKey());
            List<Row> ragged = new ArrayList<>();
            for (Map.Entry<Integer, List<Row>> byLength : e.getValue().entrySet()) {
                if (byLength.getValue().size() >= options.minRows()) {
                    String name = example.leaf().tag + "_" + dims(byLength.getKey(), example.inner());
                    named.put(name, new Class(name, true, example.leaf(), byLength.getKey(), example.inner(), byLength.getValue()));
                } else {
                    ragged.addAll(byLength.getValue());
                }
            }
            if (!ragged.isEmpty()) {
                ragged.sort(java.util.Comparator.comparingLong(Row::objectId));
                String name = example.leaf().tag + "_r" + (example.inner().length == 0 ? "" : "_" + dims(-1, example.inner()));
                named.put(name, new Class(name, false, example.leaf(), -1, example.inner(), ragged));
            }
        }
        List<Class> classes = new ArrayList<>(named.values());
        List<RecordArrays.RecClass> records = RecordArrays.classify(recordCandidates, options.minRows());
        long[] classOf = new long[numSequences];
        for (int c = 0; c < classes.size(); c++) {
            for (Row r : classes.get(c).rows()) {
                classOf[(int) (r.objectId() - maxEntityId - first - 1)] = c + 1;
            }
        }
        for (int c = 0; c < records.size(); c++) {
            for (RecordArrays.RecRow r : records.get(c).rows()) {
                classOf[(int) (r.objectId() - maxEntityId - first - 1)] = classes.size() + c + 1;
            }
        }
        return new SequenceArrays(classes, records, classOf, options.timeUnit());
    }

    private static final Pattern RECORD_NAME = Pattern.compile("rec_([1-9][0-9]{0,9})");

    /**
     * {@code names} (the groups of {@code /.BG/arrays}) in class-number order:
     * the tensor and ragged classes in name order, then the record classes by
     * their number ({@code rec_2} before {@code rec_10}).
     */
    public static List<String> classOrder(java.util.Collection<String> names) {
        List<String> out = new ArrayList<>(names);
        out.sort((a, b) -> {
            java.util.regex.Matcher ma = RECORD_NAME.matcher(a);
            java.util.regex.Matcher mb = RECORD_NAME.matcher(b);
            boolean ra = ma.matches();
            boolean rb = mb.matches();
            if (ra != rb) {
                return ra ? 1 : -1;
            }
            return ra ? Long.compare(Long.parseLong(ma.group(1)), Long.parseLong(mb.group(1))) : a.compareTo(b);
        });
        return out;
    }

    /**
     * The arrays of the literals section {@code literals} when {@code options}
     * is set and a sequence packs, else null; with any packed, the section is
     * also given each sequence row's class ({@code sequenceArrays}). One rule
     * for the in-memory engines, which all hold the sorted section.
     */
    public static SequenceArrays of(Options options, com.ebremer.beakgraph.core.DictionaryWriter literals,
                                    long maxEntityId, java.util.Set<Node> topLevel) {
        if (options == null || !(literals instanceof MultiTypeDictionaryWriter section)) {
            return null;
        }
        SequenceArrays arrays = classify(section.getNodes(), maxEntityId, topLevel::contains, options);
        if (arrays.isEmpty()) {
            return null;
        }
        section.setSequenceArrays(arrays.classes());
        return arrays;
    }

    /** "768", "2x3": a row's dimensions; {@code length < 0} leaves the outer one out. */
    private static String dims(int length, int[] inner) {
        StringBuilder sb = new StringBuilder();
        if (length >= 0) {
            sb.append(length);
        }
        for (int d : inner) {
            if (sb.length() > 0) sb.append('x');
            sb.append(d);
        }
        return sb.toString();
    }

    /** Whether any sequence packed: without one, a store has no {@code /.BG/arrays} and no {@code sequenceArrays}. */
    public boolean isEmpty() {
        return classes.isEmpty() && records.isEmpty();
    }

    /** Each sequence row's class number (1-based, in name order), 0 when it is not packed. */
    public long[] classes() {
        return classOfSequence.clone();
    }

    /** Writes {@code arrays/} under {@code bg} (the {@code .BG} group): a group per class, in name order. */
    public void write(WritableGroup bg, boolean compress) {
        if (isEmpty()) {
            return;
        }
        WritableGroup arrays = bg.putGroup(Params.ARRAYS);
        for (Class c : classes) {
            WritableGroup g = arrays.putGroup(c.name());
            g.putAttribute("kind", c.tensor() ? "tensor" : "ragged");
            g.putAttribute("datatype", c.leaf().datatype);
            g.putAttribute("shape", c.tensor() ? dims(c.length(), c.inner()) : dims(-1, c.inner()));
            int per = 1;
            for (int d : c.inner()) {
                per *= d;
            }
            int rows = c.rows().size();
            long items = 0;
            for (Row r : c.rows()) {
                items += r.length();
            }
            long[] valueShape;
            List<String> dimNames = new ArrayList<>();
            if (c.tensor()) {
                valueShape = new long[2 + c.inner().length];
                valueShape[0] = rows;
                valueShape[1] = c.length();
                dimNames.add("seq");
                dimNames.add("d1");
                for (int i = 0; i < c.inner().length; i++) {
                    valueShape[2 + i] = c.inner()[i];
                    dimNames.add("d" + (2 + i));
                }
            } else {
                valueShape = new long[1 + c.inner().length];
                valueShape[0] = items;
                dimNames.add("item");
                for (int i = 0; i < c.inner().length; i++) {
                    valueShape[1 + i] = c.inner()[i];
                    dimNames.add("d" + (1 + i));
                }
            }
            int total = Math.toIntExact(c.tensor() ? (long) rows * c.length() * per : items * per);
            Object values = newValues(c.leaf().type, total);
            boolean[] mask = new boolean[total];
            boolean anySlot = false;
            int at = 0;
            for (Row r : c.rows()) {
                for (Object leaf : r.leaves()) {
                    if (leaf == null) {
                        mask[at] = true;
                        anySlot = true;
                    } else {
                        set(values, c.leaf().type, at, leaf);
                    }
                    at++;
                }
            }
            g.putTypedArray("values", new TypedArray(c.leaf().type, valueShape, values, dimNames, null), compress);
            if (!c.tensor()) {
                long[] starts = new long[rows + 1];
                for (int i = 0; i < rows; i++) {
                    starts[i + 1] = starts[i] + c.rows().get(i).length();
                }
                g.putTypedArray("starts", new TypedArray(TypedArray.Type.I64, new long[]{rows + 1L}, starts,
                        List.of("bound"), null), compress);
            }
            if (anySlot) {
                g.putTypedArray("mask", new TypedArray(TypedArray.Type.BOOL, valueShape, mask, dimNames, null), compress);
            }
            long[] seq = new long[rows];
            for (int i = 0; i < rows; i++) {
                seq[i] = c.rows().get(i).objectId();
            }
            g.putTypedArray("seq", new TypedArray(TypedArray.Type.I64, new long[]{rows}, seq, List.of("seq"), null), compress);
        }
        for (RecordArrays.RecClass r : records) {
            RecordArrays.write(r, arrays, compress, timeUnit);
        }
    }

    /**
     * {@code -verify -deep}: whether the class {@code name} in {@code g}
     * rebuilds its sequences exactly - each row re-derived from the canonical
     * elements of the sequence its {@code seq} names, and compared value by
     * value (bit by bit for floats), slots against the mask. Null when it
     * does; else what does not.
     */
    public static String verifyClass(String name, com.ebremer.beakgraph.store.StoreGroup g,
                                     com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader dict) {
        String kind = g.stringAttribute("kind");
        if ("record".equals(kind)) {
            return RECORD_NAME.matcher(name).matches() ? RecordArrays.verify(name, g, dict)
                    : "a record class named '" + name + "' (expected rec_<n>)";
        }
        boolean tensor = "tensor".equals(kind);
        if (!tensor && !"ragged".equals(kind)) {
            return "kind '" + kind + "' (expected tensor or ragged)";
        }
        Leaf leaf = Leaf.of(g.stringAttribute("datatype"));
        if (leaf == null) {
            return "datatype '" + g.stringAttribute("datatype") + "' packs into no array type";
        }
        TypedArray values = g.typedArray("values");
        TypedArray seqArray = g.typedArray("seq");
        if (values == null || seqArray == null || values.type() != leaf.type) {
            return "values or seq missing, or values not " + leaf.type;
        }
        TypedArray maskArray = g.typedArray("mask");
        boolean[] mask = (maskArray == null) ? null : (boolean[]) maskArray.values();
        long[] seq = (long[]) seqArray.values();
        long[] starts = tensor ? null : (long[]) g.typedArray("starts").values();
        long[] shape = values.shape();
        long per = 1;
        for (int i = tensor ? 2 : 1; i < shape.length; i++) {
            per *= shape[i];
        }
        long at = 0;
        for (int r = 0; r < seq.length; r++) {
            if (r > 0 && seq[r] <= seq[r - 1]) {
                return "seq is not ascending at row " + r;
            }
            if (!dict.isSequenceObjectId(seq[r])) {
                return "row " + r + " names object " + seq[r] + ", which is no sequence";
            }
            Row row = probe(seq[r], dict.getObjects().extract(seq[r]), 1);
            if (row == null || row.leaf() != leaf) {
                return "row " + r + " (sequence " + seq[r] + ") does not pack as " + leaf.datatype;
            }
            long length = tensor ? shape[1] : starts[r + 1] - starts[r];
            if (row.length() != length || row.leaves().length != length * per) {
                return "row " + r + " (sequence " + seq[r] + ") has " + row.length() + " elements where the class holds " + length;
            }
            for (Object expected : row.leaves()) {
                boolean slot = mask != null && mask[(int) at];
                if (expected == null ? !slot : (slot || !sameValue(values, leaf.type, (int) at, expected))) {
                    return "row " + r + " (sequence " + seq[r] + ") differs at value " + at;
                }
                at++;
            }
        }
        return (at == values.elementCount()) ? null : "values hold " + values.elementCount() + " elements; the rows " + at;
    }

    static boolean sameValue(TypedArray values, TypedArray.Type type, int at, Object expected) {
        return switch (type) {
            case F64 -> Double.doubleToRawLongBits(((double[]) values.values())[at]) == Double.doubleToRawLongBits((Double) expected);
            case F32 -> Float.floatToRawIntBits(((float[]) values.values())[at]) == Float.floatToRawIntBits((Float) expected);
            case I64 -> ((long[]) values.values())[at] == (Long) expected;
            case I32 -> ((int[]) values.values())[at] == (int) (long) (Long) expected;
            case BOOL -> ((boolean[]) values.values())[at] == (Boolean) expected;
            case U16, STR -> false;   // no numeric leaf packs to these
        };
    }

    static Object newValues(TypedArray.Type type, int n) {
        return switch (type) {
            case F64 -> new double[n];
            case F32 -> new float[n];
            case I64 -> new long[n];
            case I32, U16 -> new int[n];
            case BOOL -> new boolean[n];
            case STR -> new String[n];
        };
    }

    static void set(Object values, TypedArray.Type type, int at, Object leaf) {
        switch (type) {
            case F64 -> ((double[]) values)[at] = (Double) leaf;
            case F32 -> ((float[]) values)[at] = (Float) leaf;
            case I64 -> ((long[]) values)[at] = (Long) leaf;
            case I32 -> ((int[]) values)[at] = (int) (long) (Long) leaf;
            case BOOL -> ((boolean[]) values)[at] = (Boolean) leaf;
            case U16, STR -> throw new IllegalArgumentException("no numeric leaf packs to " + type);
        }
    }
}
