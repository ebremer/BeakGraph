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

    /**
     * {@code -arrays}: on with these settings, or off (null). With {@code only} ({@code -arrays-only},
     * format v8, RDFARRY.md step 9), the sequences a tensor or ragged class takes are stored only there:
     * their rows keep their place in the dictionary, but they have no canonical elements, and their leaves
     * and inner sequences have dictionary rows only when something else uses them.
     */
    public record Options(int minRows, int minLeaves, boolean compress, TimeUnit timeUnit, boolean only) {
        public static final Options DEFAULT = new Options(16, 2, false, TimeUnit.US);

        public Options {
            if (minRows < 1 || minLeaves < 1) {
                throw new IllegalArgumentException("-arrays-min-rows and -arrays-min must be at least 1");
            }
            java.util.Objects.requireNonNull(timeUnit, "timeUnit");
        }

        /** A second copy, the canonical rows kept (format v7). */
        public Options(int minRows, int minLeaves, boolean compress, TimeUnit timeUnit) {
            this(minRows, minLeaves, compress, timeUnit, false);
        }

        /** Microseconds, the default time unit. */
        public Options(int minRows, int minLeaves, boolean compress) {
            this(minRows, minLeaves, compress, TimeUnit.US);
        }

        /** These settings, the packed sequences stored only as arrays ({@code -arrays-only}) or not. */
        public Options withOnly(boolean on) {
            return new Options(minRows, minLeaves, compress, timeUnit, on);
        }
    }

    /**
     * Refuses {@code -arrays-only} on an engine that cannot write it yet (RDFARRY.md step 9): its leaves
     * would be ranked into the dictionary before anything knew they belong only to arrays.
     */
    public static void refuseOnly(Options options, String engine) {
        if (options != null && options.only()) {
            throw new IllegalArgumentException("-arrays-only (format v8) is written by -method 0 only for now, not by "
                    + engine + " (RDFARRY.md step 9)");
        }
    }

    /**
     * Checks that every sequence {@code -arrays-only} left without canonical elements has a tensor or ragged
     * class in {@code arrays}: else it would be in the store nowhere. The ingest's per-sequence test and the
     * classification must agree, and this is where a drift between them would show.
     */
    static void requireHolds(SequenceArrays arrays, com.ebremer.beakgraph.core.DictionaryWriter literals,
                             java.util.Set<Node> packedOnly) {
        if (packedOnly.isEmpty()) {
            return;
        }
        if (arrays == null || !(literals instanceof MultiTypeDictionaryWriter section)) {
            throw new IllegalStateException("-arrays-only left " + packedOnly.size()
                    + " sequences without canonical elements, but no array class holds them");
        }
        List<Node> nodes = section.getNodes();
        int first = nodes.size() - arrays.classOfSequence.length;
        int held = 0;
        for (int k = 0; k < arrays.classOfSequence.length; k++) {
            Node seq = nodes.get(first + k);
            if (!packedOnly.contains(seq)) {
                continue;
            }
            long c = arrays.classOfSequence[k];
            if (c == 0 || arrays.decision.classes().get((int) c - 1).kind() == Kind.RECORD) {
                throw new IllegalStateException("-arrays-only left " + seq + " without canonical elements, but "
                        + (c == 0 ? "no class" : "a record class") + " holds it");
            }
            held++;
        }
        if (held != packedOnly.size()) {
            throw new IllegalStateException("-arrays-only left " + packedOnly.size() + " sequences without canonical"
                    + " elements, but " + held + " of them are rows of the literals section");
        }
    }

    /**
     * Whether {@code seq}, an object of some quad, packs into a tensor or ragged class at {@code minLeaves}:
     * then {@code -arrays-only} stores it only there. Every such sequence has a class ({@link Classifier}
     * gives a key's rows too few for a tensor of their length its ragged class), so this per-sequence test
     * is the classification's own.
     */
    public static boolean packsAsNumbers(Node seq, int minLeaves) {
        return seq.isNodeSeq() && probe(0, seq, minLeaves) != null;
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

    /** What a class is: a tensor (one length), ragged (a key's other lengths), or a record (§3.5a). */
    public enum Kind { TENSOR, RAGGED, RECORD }

    /**
     * One class as it is written: its name; for a numeric class its leaf,
     * length (tensor) and inner shape; for a record class its signature, item
     * and columns; its rows and items (list items, or a ragged class's outer
     * elements); whether it holds a slot (per column for a record); and each
     * {@code lang} column's tags in code order (null elsewhere).
     */
    public record ClassDef(String name, Kind kind, Leaf leaf, int length, int[] inner,
                           String signature, boolean list, RecordArrays.Sig item, List<RecordArrays.Column> columns,
                           long rows, long items, boolean slot, boolean[] columnSlot, List<List<String>> tags) {
    }

    /** The classes in class-number order (the number is the place plus one), and each bucket's class number (0: none). */
    public record Decision(List<ClassDef> classes, int[] classOfBucket) {
    }

    /**
     * The one classification every engine runs: rows arrive in id order and
     * fall into buckets - a numeric (leaf, inner shape, length), or a record
     * signature - which count their rows, items, slots and language tags;
     * {@link #decide} then makes the classes of §3.5 and §3.5a from the counts
     * alone, so an engine that spills its rows gets the classes of one that
     * holds them.
     */
    public static final class Classifier {

        private final Options options;
        private final Map<String, Bucket> byKey = new HashMap<>();
        private final List<Bucket> buckets = new ArrayList<>();

        /** One bucket: what its rows share, and what they hold. */
        static final class Bucket {
            final int id;
            final Leaf leaf;
            final int[] inner;
            final int length;
            final String signature;
            final boolean list;
            final RecordArrays.Sig item;
            final List<RecordArrays.Column> columns;
            final int width;
            long rows;
            long items;
            boolean slot;
            final boolean[] columnSlot;
            final List<java.util.TreeSet<String>> tags;

            Bucket(int id, Leaf leaf, int[] inner, int length, RecordArrays.RecRow record) {
                this.id = id;
                this.leaf = leaf;
                this.inner = inner;
                this.length = length;
                this.signature = (record == null) ? null : record.signature();
                this.list = record != null && record.list();
                this.item = (record == null) ? null : record.item();
                this.columns = (record == null) ? null : RecordArrays.columns(record.item());
                int w = 0;
                if (columns != null) {
                    for (RecordArrays.Column c : columns) {
                        w += c.width();
                    }
                }
                this.width = w;
                this.columnSlot = (columns == null) ? null : new boolean[columns.size()];
                this.tags = new ArrayList<>();
                if (columns != null) {
                    for (RecordArrays.Column c : columns) {
                        tags.add(c.leaf().kind() == RecordArrays.Kind.LANG ? new java.util.TreeSet<>() : null);
                    }
                }
            }

            boolean record() {
                return signature != null;
            }
        }

        public Classifier(Options options) {
            this.options = options;
        }

        /** The bucket of a row {@link #probeAny} made, counted. */
        public int add(Object row) {
            return (row instanceof Row r) ? add(r) : add((RecordArrays.RecRow) row);
        }

        /** The bucket of a numeric row, counted. */
        int add(Row r) {
            Bucket b = byKey.computeIfAbsent("n:" + r.leaf().name() + Arrays.toString(r.inner()) + ":" + r.length(),
                    k -> bucket(new Bucket(buckets.size(), r.leaf(), r.inner(), r.length(), null)));
            b.rows++;
            b.items += r.length();
            b.slot |= r.hasSlot();
            return b.id;
        }

        /** The bucket of a record row, counted, its slots and language tags noted. */
        int add(RecordArrays.RecRow r) {
            Bucket b = byKey.computeIfAbsent("r:" + r.signature(),
                    k -> bucket(new Bucket(buckets.size(), null, null, -1, r)));
            b.rows++;
            b.items += r.items();
            for (int ci = 0; ci < b.columns.size(); ci++) {
                RecordArrays.Column col = b.columns.get(ci);
                java.util.TreeSet<String> tags = b.tags.get(ci);
                for (int j = 0; j < r.items(); j++) {
                    for (int w = 0; w < col.width(); w++) {
                        Object v = r.leaves()[j * b.width + col.offset() + w];
                        if (v == null) {
                            b.columnSlot[ci] = true;
                        } else if (tags != null && tags.size() <= RecordArrays.MAX_LANGS) {
                            tags.add(((String[]) v)[1]);   // one past the cap is enough to know the class will not fit
                        }
                    }
                }
            }
            return b.id;
        }

        private Bucket bucket(Bucket b) {
            buckets.add(b);
            return b;
        }

        /**
         * The classes: per numeric key, a tensor class for each length at least
         * {@code minRows} rows share and one ragged class for its other lengths,
         * all in name order; then a record class {@code rec_<n>} per signature
         * at least {@code minRows} rows share, in the spellings' order (a class
         * whose {@code lang} column would need more than 65536 codes is not made).
         */
        public Decision decide() {
            TreeMap<String, List<Bucket>> numericKeys = new TreeMap<>();
            TreeMap<String, Bucket> signatures = new TreeMap<>();
            for (Bucket b : buckets) {
                if (b.record()) {
                    signatures.put(b.signature, b);
                } else {
                    numericKeys.computeIfAbsent(b.leaf.name() + Arrays.toString(b.inner), k -> new ArrayList<>()).add(b);
                }
            }
            TreeMap<String, ClassDef> named = new TreeMap<>();
            Map<String, List<Bucket>> bucketsOf = new HashMap<>();
            for (List<Bucket> key : numericKeys.values()) {
                key.sort(java.util.Comparator.comparingInt(b -> b.length));
                List<Bucket> ragged = new ArrayList<>();
                for (Bucket b : key) {
                    if (b.rows >= options.minRows()) {
                        String name = b.leaf.tag + "_" + dims(b.length, b.inner);
                        named.put(name, new ClassDef(name, Kind.TENSOR, b.leaf, b.length, b.inner, null, false, null, null,
                                b.rows, b.items, b.slot, null, null));
                        bucketsOf.put(name, List.of(b));
                    } else {
                        ragged.add(b);
                    }
                }
                if (!ragged.isEmpty()) {
                    Bucket e = ragged.get(0);
                    String name = e.leaf.tag + "_r" + (e.inner.length == 0 ? "" : "_" + dims(-1, e.inner));
                    long rows = 0;
                    long items = 0;
                    boolean slot = false;
                    for (Bucket b : ragged) {
                        rows += b.rows;
                        items += b.items;
                        slot |= b.slot;
                    }
                    named.put(name, new ClassDef(name, Kind.RAGGED, e.leaf, -1, e.inner, null, false, null, null,
                            rows, items, slot, null, null));
                    bucketsOf.put(name, ragged);
                }
            }
            List<ClassDef> classes = new ArrayList<>(named.values());
            int records = 0;
            for (Bucket b : signatures.values()) {
                if (b.rows < options.minRows()) {
                    continue;
                }
                boolean fits = true;
                List<List<String>> tags = new ArrayList<>();
                for (java.util.TreeSet<String> t : b.tags) {
                    fits &= (t == null || t.size() <= RecordArrays.MAX_LANGS);
                    tags.add((t == null) ? null : List.copyOf(t));
                }
                if (!fits) {
                    continue;   // more tags than uint16 codes: the rows stay canonical only
                }
                String name = "rec_" + (++records);
                classes.add(new ClassDef(name, Kind.RECORD, null, -1, null, b.signature, b.list, b.item, b.columns,
                        b.rows, b.items, false, b.columnSlot.clone(), tags));
                bucketsOf.put(name, List.of(b));
            }
            int[] classOfBucket = new int[buckets.size()];
            for (int c = 0; c < classes.size(); c++) {
                for (Bucket b : bucketsOf.get(classes.get(c).name())) {
                    classOfBucket[b.id] = c + 1;
                }
            }
            return new Decision(List.copyOf(classes), classOfBucket);
        }
    }

    private final Decision decision;
    /** Each class's rows in id order (a numeric class's {@link Row}s, a record class's RecRows). */
    private final List<List<Object>> rowsOfClass;
    private final long[] classOfSequence;
    private final TimeUnit timeUnit;

    private SequenceArrays(Decision decision, List<List<Object>> rowsOfClass, long[] classOfSequence, TimeUnit timeUnit) {
        this.decision = decision;
        this.rowsOfClass = rowsOfClass;
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
        Classifier classifier = new Classifier(options);
        List<Object> rows = new ArrayList<>();
        List<Integer> buckets = new ArrayList<>();
        for (int k = 0; k < numSequences; k++) {
            Node seq = literals.get(first + k);
            if (!topLevel.test(seq)) {
                continue;
            }
            long objectId = maxEntityId + first + k + 1;
            Object row = probeAny(objectId, seq, options);
            if (row != null) {
                rows.add(row);
                buckets.add(row instanceof Row r ? classifier.add(r) : classifier.add((RecordArrays.RecRow) row));
            }
        }
        Decision decision = classifier.decide();
        List<List<Object>> rowsOfClass = new ArrayList<>();
        for (int c = 0; c < decision.classes().size(); c++) {
            rowsOfClass.add(new ArrayList<>());
        }
        long[] classOf = new long[numSequences];
        for (int i = 0; i < rows.size(); i++) {
            int c = decision.classOfBucket()[buckets.get(i)];
            if (c > 0) {
                rowsOfClass.get(c - 1).add(rows.get(i));
                classOf[(int) (objectId(rows.get(i)) - maxEntityId - first - 1)] = c;
            }
        }
        return new SequenceArrays(decision, rowsOfClass, classOf, options.timeUnit());
    }

    /**
     * A top-level sequence as the row it packs as: a {@link Row} when a tensor
     * or ragged class can take it, else a record row (§3.5a), else null.
     */
    public static Object probeAny(long objectId, Node seq, Options options) {
        Row row = probe(objectId, seq, options.minLeaves());
        return (row != null) ? row : RecordArrays.probe(objectId, seq, options.timeUnit(), options.minLeaves());
    }

    /** The object id of a {@link Row} or a record row. */
    public static long objectId(Object row) {
        return (row instanceof Row r) ? r.objectId() : ((RecordArrays.RecRow) row).objectId();
    }

    /** A row's leaves, item-major, null at a slot: Double, Float, Long, Boolean, String or {lexical form, tag}. */
    public static Object[] leavesOf(Object row) {
        return (row instanceof Row r) ? r.leaves() : ((RecordArrays.RecRow) row).leaves();
    }

    /** A row's outer length (a numeric row) or items (a record row). */
    public static int countOf(Object row) {
        return (row instanceof Row r) ? r.length() : ((RecordArrays.RecRow) row).items();
    }

    /** The row of class {@code c} with these parts, as {@link #probeAny} made it: for an engine that spilled it. */
    public static Object rowOf(ClassDef c, long objectId, int count, Object[] leaves) {
        return (c.kind() == Kind.RECORD)
                ? new RecordArrays.RecRow(objectId, c.signature(), c.list(), c.item(), count, leaves)
                : new Row(objectId, c.leaf(), count, c.inner(), leaves);
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
    static String dims(int length, int[] inner) {
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
        return decision.classes().isEmpty();
    }

    /** Each sequence row's class number (1-based, in name order), 0 when it is not packed. */
    public long[] classes() {
        return classOfSequence.clone();
    }

    /** Writes {@code arrays/} under {@code bg} (the {@code .BG} group): a group per class, in class-number order. */
    public void write(WritableGroup bg, boolean compress) {
        if (isEmpty()) {
            return;
        }
        WritableGroup arrays = bg.putGroup(Params.ARRAYS);
        for (int c = 0; c < decision.classes().size(); c++) {
            List<Object> rows = rowsOfClass.get(c);
            writeClass(decision.classes().get(c), () -> RowSource.of(rows.iterator()), arrays, compress, timeUnit);
        }
    }

    /**
     * A class's rows in id order, as often as a writer asks: one pass per
     * array, so a class need not fit in memory.
     */
    @FunctionalInterface
    public interface RowSource {

        /** A pass over the rows; close it when done. */
        Rows open();

        /** One pass. */
        interface Rows extends java.util.Iterator<Object>, AutoCloseable {
            @Override
            void close();
        }

        /** A pass over rows held in memory. */
        static Rows of(java.util.Iterator<Object> it) {
            return new Rows() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Object next() {
                    return it.next();
                }

                @Override
                public void close() {
                }
            };
        }
    }

    /** How a pass fills one array from a row: how many elements the row gives, and putting them at {@code at}. */
    interface Filler {
        int elements(Object row);

        void fill(Object row, Object piece, int at);
    }

    /** The elements one append hands an array, at most (a piece holds at least one row). */
    static int pieceElements = 1 << 20;

    /**
     * Writes one array of a class in one pass over its rows, a piece of
     * elements at a time. Each piece is a fresh array, so an element no row
     * fills (a slot) is 0, false or {@code ""}.
     */
    static void pass(WritableGroup g, String name, TypedArray.Type type, long[] shape, List<String> dims,
                     Map<String, Object> attributes, boolean compress, RowSource source, Filler filler) {
        int capacity = (int) Math.min(pieceElements, Math.max(1, TypedArray.elementCount(shape)));
        try (com.ebremer.beakgraph.store.TypedArrayWriter w = g.createTypedArray(name, type, shape, dims, attributes, compress);
             RowSource.Rows rows = source.open()) {
            Object piece = null;
            int size = 0;
            int used = 0;
            while (rows.hasNext()) {
                Object row = rows.next();
                int n = filler.elements(row);
                if (piece != null && used + n > size) {
                    w.append(used == size ? piece : TypedArray.slice(type, piece, 0, used));
                    piece = null;
                }
                if (piece == null) {
                    size = Math.max(capacity, n);
                    piece = TypedArray.newValues(type, size);
                    used = 0;
                }
                filler.fill(row, piece, used);
                used += n;
            }
            if (piece != null) {
                w.append(used == size ? piece : TypedArray.slice(type, piece, 0, used));
            }
        }
    }

    /** {@code starts}, int64 [rows + 1]: 0, then where each row's items end. */
    static void starts(WritableGroup g, long rows, boolean compress, RowSource source,
                       java.util.function.ToIntFunction<Object> items) {
        long[] end = {0};
        boolean[] first = {true};
        // The leading 0 rides on the first row: two elements for it, one for each after.
        pass(g, "starts", TypedArray.Type.I64, new long[]{rows + 1}, List.of("bound"), null, compress, source, new Filler() {
            @Override
            public int elements(Object row) {
                return first[0] ? 2 : 1;
            }

            @Override
            public void fill(Object row, Object piece, int at) {
                long[] p = (long[]) piece;
                if (first[0]) {
                    p[at++] = 0;
                    first[0] = false;
                }
                end[0] += items.applyAsInt(row);
                p[at] = end[0];
            }
        });
    }

    /** {@code seq}, int64 [rows]: each row's sequence object id. */
    static void seq(WritableGroup g, long rows, boolean compress, RowSource source) {
        pass(g, "seq", TypedArray.Type.I64, new long[]{rows}, List.of("seq"), null, compress, source, new Filler() {
            @Override
            public int elements(Object row) {
                return 1;
            }

            @Override
            public void fill(Object row, Object piece, int at) {
                ((long[]) piece)[at] = objectId(row);
            }
        });
    }

    /**
     * Writes the class {@code c} under {@code arrays} from its rows, an array
     * at a time: a tensor's or ragged class's values, starts, mask and seq, or
     * a record class's columns ({@link RecordArrays#writeClass}).
     */
    public static void writeClass(ClassDef c, RowSource rows, WritableGroup arrays, boolean compress, TimeUnit unit) {
        if (c.kind() == Kind.RECORD) {
            RecordArrays.writeClass(c, rows, arrays, compress, unit);
            return;
        }
        boolean tensor = c.kind() == Kind.TENSOR;
        WritableGroup g = arrays.putGroup(c.name());
        g.putAttribute("kind", tensor ? "tensor" : "ragged");
        g.putAttribute("datatype", c.leaf().datatype);
        g.putAttribute("shape", tensor ? dims(c.length(), c.inner()) : dims(-1, c.inner()));
        long[] valueShape;
        List<String> dimNames = new ArrayList<>();
        if (tensor) {
            valueShape = new long[2 + c.inner().length];
            valueShape[0] = c.rows();
            valueShape[1] = c.length();
            dimNames.add("seq");
            dimNames.add("d1");
            for (int i = 0; i < c.inner().length; i++) {
                valueShape[2 + i] = c.inner()[i];
                dimNames.add("d" + (2 + i));
            }
        } else {
            valueShape = new long[1 + c.inner().length];
            valueShape[0] = c.items();
            dimNames.add("item");
            for (int i = 0; i < c.inner().length; i++) {
                valueShape[1 + i] = c.inner()[i];
                dimNames.add("d" + (1 + i));
            }
        }
        TypedArray.Type type = c.leaf().type;
        pass(g, "values", type, valueShape, dimNames, null, compress, rows, new Filler() {
            @Override
            public int elements(Object row) {
                return ((Row) row).leaves().length;
            }

            @Override
            public void fill(Object row, Object piece, int at) {
                for (Object leaf : ((Row) row).leaves()) {
                    if (leaf != null) {
                        set(piece, type, at, leaf);
                    }
                    at++;
                }
            }
        });
        if (!tensor) {
            starts(g, c.rows(), compress, rows, row -> ((Row) row).length());
        }
        if (c.slot()) {
            pass(g, "mask", TypedArray.Type.BOOL, valueShape, dimNames, null, compress, rows, new Filler() {
                @Override
                public int elements(Object row) {
                    return ((Row) row).leaves().length;
                }

                @Override
                public void fill(Object row, Object piece, int at) {
                    for (Object leaf : ((Row) row).leaves()) {
                        ((boolean[]) piece)[at++] = (leaf == null);
                    }
                }
            });
        }
        seq(g, c.rows(), compress, rows);
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
            if (dict.heldOnlyInArrays(seq[r])) {
                // -arrays-only (format v8): the row is the sequence's only copy, so there is nothing to compare
                // it with; the dictionary order check reads it back and sorts it against its neighbours.
                at += (tensor ? shape[1] : starts[r + 1] - starts[r]) * per;
                continue;
            }
            Row row = probe(seq[r], dict.extractCanonicalSequence(seq[r]), 1);
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
