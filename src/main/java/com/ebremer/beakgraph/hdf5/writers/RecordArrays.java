package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.beakgraph.store.StoreGroup;
import com.ebremer.beakgraph.store.TypedArray;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.jena.graph.Node;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

/**
 * The record classes of {@code /.BG/arrays} (format v7, RDFARRY.md §3.5a):
 * sequences that mix types by position, such as FHIR's
 * {@code ( code name ( value unit ) )}, laid out as one typed column per leaf
 * position, the way a table holds them.
 *
 * <p>A sequence's <i>signature</i> is its type structure, a function of the
 * sequence alone:
 * <pre>
 * signature := leaf | leaf "[" dims "]" | "(" signature ( "," signature )* ")" | "list(" signature ")"
 * </pre>
 * At the outermost level a sequence whose elements unify to one signature
 * {@code T} is {@code list(T)} (a slot unifies with any leaf); any other, and
 * every inner one, is a tuple of its elements' signatures, except that a
 * rectangular inner block of one numeric or boolean type is one N-D column
 * ({@code f64[768]}). A slot stands only where the sequence itself types it: at
 * a leaf of a block, or at a list item's leaf that another item types.
 *
 * <p>Leaves are the numeric ones of {@link SequenceArrays.Leaf}, {@code str},
 * {@code lang}, {@code iri}, {@code dt} (with its zone: {@code dt}, {@code dtZ},
 * {@code dt+05:00}) and {@code date}; a string, IRI or language tag packs only
 * without U+0000 or an unpaired surrogate (h5py reads strings to their first
 * NUL), and a date or dateTime only when the packed value rebuilds its exact
 * lexical form.
 */
final class RecordArrays {

    private RecordArrays() {
    }

    /** What a column holds. */
    enum Kind { NUMBER, STRING, LANG, IRI, DATETIME, DATE }

    /**
     * A record leaf: its spelling in signatures, its kind, the numeric leaf of a
     * {@code NUMBER}, and the zone of a {@code DATETIME} ({@code ""} for none,
     * {@code "Z"}, or an offset such as {@code "+05:00"}).
     */
    record RLeaf(String spelling, Kind kind, SequenceArrays.Leaf numeric, String zone) {
        static final RLeaf STR = new RLeaf("str", Kind.STRING, null, null);
        static final RLeaf LANG = new RLeaf("lang", Kind.LANG, null, null);
        static final RLeaf IRI = new RLeaf("iri", Kind.IRI, null, null);
        static final RLeaf DATE = new RLeaf("date", Kind.DATE, null, null);

        static RLeaf number(SequenceArrays.Leaf leaf) {
            return new RLeaf(leaf.tag, Kind.NUMBER, leaf, null);
        }

        static RLeaf dateTime(String zone) {
            return new RLeaf("dt" + zone, Kind.DATETIME, null, zone);
        }

        /** The leaf {@code spelling} names, or null. */
        static RLeaf ofSpelling(String spelling) {
            switch (spelling) {
                case "str": return STR;
                case "lang": return LANG;
                case "iri": return IRI;
                case "date": return DATE;
                case "dt": return dateTime("");
                case "dtZ": return dateTime("Z");
                default:
                    if (spelling.startsWith("dt") && offsetSeconds(spelling.substring(2)) != null) {
                        return dateTime(spelling.substring(2));
                    }
                    SequenceArrays.Leaf leaf = SequenceArrays.Leaf.ofTag(spelling);
                    return (leaf == null) ? null : number(leaf);
            }
        }

        TypedArray.Type columnType() {
            return switch (kind) {
                case NUMBER -> numeric.type;
                case STRING, LANG, IRI -> TypedArray.Type.STR;
                case DATETIME -> TypedArray.Type.I64;
                case DATE -> TypedArray.Type.I32;
            };
        }

        /** The XSD (or RDF) datatype IRI the column's literals have; null for IRIs. */
        String datatype() {
            return switch (kind) {
                case NUMBER -> numeric.datatype;
                case STRING -> XSD.xstring.getURI();
                case LANG -> RDF.dtLangString.getURI();
                case IRI -> null;
                case DATETIME -> XSD.dateTime.getURI();
                case DATE -> XSD.date.getURI();
            };
        }
    }

    /** A signature: a leaf, a numeric block, a tuple, or (while unifying) a slot. */
    sealed interface Sig permits Scalar, Block, Tuple, Slot {
    }

    record Scalar(RLeaf leaf) implements Sig {
    }

    record Block(SequenceArrays.Leaf leaf, List<Integer> dims) implements Sig {
    }

    record Tuple(List<Sig> parts) implements Sig {
    }

    enum Slot implements Sig { SLOT }

    /** A packed leaf: its type and value (a number or boolean as §3.5 packs it, a string, {lexical form, tag}, or a count). */
    record Typed(RLeaf leaf, Object value) {
    }

    /** The leaf {@code n} packs as, with its value, or null when it does not. */
    static Typed typed(Node n, SequenceArrays.TimeUnit unit) {
        if (n.isURI()) {
            String iri = n.getURI();
            return safe(iri) ? new Typed(RLeaf.IRI, iri) : null;
        }
        if (!n.isLiteral()) {
            return null;   // a blank node, a variable, a triple term
        }
        String datatype = n.getLiteralDatatypeURI();
        String lex = n.getLiteralLexicalForm();
        SequenceArrays.Leaf number = SequenceArrays.Leaf.of(datatype);
        if (number != null) {
            Object v = SequenceArrays.packedValue(n, number);
            return (v == null) ? null : new Typed(RLeaf.number(number), v);
        }
        if (XSD.xstring.getURI().equals(datatype)) {
            return safe(lex) ? new Typed(RLeaf.STR, lex) : null;
        }
        if (RDF.dtLangString.getURI().equals(datatype)) {   // rdf:dirLangString is another datatype, unpacked
            String tag = n.getLiteralLanguage();
            return (safe(lex) && safe(tag) && !tag.isEmpty()) ? new Typed(RLeaf.LANG, new String[]{lex, tag}) : null;
        }
        if (XSD.dateTime.getURI().equals(datatype)) {
            return dateTime(lex, unit);
        }
        if (XSD.date.getURI().equals(datatype)) {
            return date(lex);
        }
        return null;
    }

    /** No U+0000 (h5py ends a string there) and no unpaired surrogate (UTF-8 cannot hold one). */
    static boolean safe(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == 0 || Character.isLowSurrogate(c)) {
                return false;
            }
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1))) {
                    return false;
                }
                i++;
            }
        }
        return true;
    }

    private static final Pattern DATE_TIME = Pattern.compile(
            "([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\\.([0-9]+))?(Z|[+-][0-9]{2}:[0-9]{2})?");
    private static final Pattern DATE = Pattern.compile("([0-9]{4})-([0-9]{2})-([0-9]{2})");
    private static final Pattern OFFSET = Pattern.compile("[+-][0-9]{2}:[0-9]{2}");

    /** The seconds of a nonzero offset {@code +hh:mm} / {@code -hh:mm} within ±14:00, else null ({@code +00:00} is spelled {@code Z}). */
    static Integer offsetSeconds(String zone) {
        if (!OFFSET.matcher(zone).matches()) {
            return null;
        }
        int h = Integer.parseInt(zone.substring(1, 3));
        int m = Integer.parseInt(zone.substring(4, 6));
        if (h > 14 || m > 59 || (h == 14 && m > 0) || (h == 0 && m == 0)) {
            return null;
        }
        return (h * 3600 + m * 60) * (zone.charAt(0) == '-' ? -1 : 1);
    }

    private static ZoneOffset offsetOf(String zone) {
        return (zone.isEmpty() || zone.equals("Z")) ? ZoneOffset.UTC : ZoneOffset.ofTotalSeconds(offsetSeconds(zone));
    }

    /**
     * An {@code xsd:dateTime} as a count of {@code unit} since 1970-01-01T00:00:00:
     * the local time itself when it has no zone, its UTC instant when it has.
     * Null unless the count rebuilds the lexical form exactly (XSD's canonical
     * form: years 0001-9999, no trailing fractional zeros, {@code Z} for a zero
     * offset) and the fraction fits the unit.
     */
    static Typed dateTime(String lex, SequenceArrays.TimeUnit unit) {
        Matcher m = DATE_TIME.matcher(lex);
        if (!m.matches()) {
            return null;
        }
        String zone = (m.group(8) == null) ? "" : m.group(8);
        if (!zone.isEmpty() && !zone.equals("Z") && offsetSeconds(zone) == null) {
            return null;
        }
        String fraction = (m.group(7) == null) ? "" : m.group(7);
        if (fraction.length() > unit.digits) {
            return null;   // finer than the store's time unit
        }
        int year = Integer.parseInt(m.group(1));
        if (year < 1) {
            return null;
        }
        LocalDateTime local;
        try {
            local = LocalDateTime.of(year, Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)),
                    Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)), Integer.parseInt(m.group(6)));
        } catch (DateTimeException invalid) {
            return null;   // 24:00:00, February 30th, a leap second, ...
        }
        long count;
        try {
            long sub = fraction.isEmpty() ? 0 : Long.parseLong(fraction + "0".repeat(unit.digits - fraction.length()));
            count = Math.addExact(Math.multiplyExact(local.toEpochSecond(offsetOf(zone)), unit.perSecond), sub);
        } catch (ArithmeticException tooFar) {
            return null;
        }
        return lex.equals(formatDateTime(count, zone, unit)) ? new Typed(RLeaf.dateTime(zone), count) : null;
    }

    /** The canonical lexical form of {@code count} {@code unit} since the epoch, in {@code zone}. */
    static String formatDateTime(long count, String zone, SequenceArrays.TimeUnit unit) {
        long seconds = Math.floorDiv(count, unit.perSecond);
        long sub = Math.floorMod(count, unit.perSecond);
        LocalDateTime local = LocalDateTime.ofEpochSecond(seconds, 0, offsetOf(zone));
        StringBuilder sb = new StringBuilder(String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d",
                local.getYear(), local.getMonthValue(), local.getDayOfMonth(),
                local.getHour(), local.getMinute(), local.getSecond()));
        if (sub != 0) {
            String digits = String.format(Locale.ROOT, "%0" + unit.digits + "d", sub);
            int end = digits.length();
            while (digits.charAt(end - 1) == '0') {
                end--;
            }
            sb.append('.').append(digits, 0, end);
        }
        return sb.append(zone).toString();
    }

    /** An {@code xsd:date} with no timezone as days since 1970-01-01, when canonical (years 0001-9999). */
    static Typed date(String lex) {
        Matcher m = DATE.matcher(lex);
        if (!m.matches() || Integer.parseInt(m.group(1)) < 1) {
            return null;
        }
        long days;
        try {
            days = LocalDate.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                    Integer.parseInt(m.group(3))).toEpochDay();
        } catch (DateTimeException invalid) {
            return null;
        }
        return lex.equals(formatDate(days)) ? new Typed(RLeaf.DATE, days) : null;
    }

    static String formatDate(long days) {
        LocalDate d = LocalDate.ofEpochDay(days);
        return String.format(Locale.ROOT, "%04d-%02d-%02d", d.getYear(), d.getMonthValue(), d.getDayOfMonth());
    }

    /** One sequence as a record row: its object id, signature, whether it is a list, its item signature, items, and leaves (item-major; null at a slot). */
    record RecRow(long objectId, String signature, boolean list, Sig item, int items, Object[] leaves) {
    }

    /**
     * {@code seq} as a record row, or null when it has no record signature: an
     * empty sequence anywhere, an unpackable leaf, a slot no position types, or
     * fewer than {@code minLeaves} leaves.
     */
    static RecRow probe(long objectId, Node seq, SequenceArrays.TimeUnit unit, int minLeaves) {
        if (!seq.isNodeSeq() || seq.getSeq().isEmpty()) {
            return null;
        }
        List<Node> elements = seq.getSeq();
        List<Object> leaves = new ArrayList<>();
        List<Sig> parts = new ArrayList<>(elements.size());
        for (Node e : elements) {
            Sig s = sigOf(e, unit, leaves);
            if (s == null) {
                return null;
            }
            parts.add(s);
        }
        if (leaves.size() < minLeaves) {
            return null;
        }
        Sig item = parts.get(0);
        for (int i = 1; i < parts.size() && item != null; i++) {
            item = unify(item, parts.get(i));
        }
        Object[] flat = leaves.toArray();
        if (item != null) {
            item = blockify(item);
            if (isTyped(item)) {
                return new RecRow(objectId, "list(" + spell(item) + ")", true, item, elements.size(), flat);
            }
        }
        Sig tuple = new Tuple(parts.stream().map(RecordArrays::blockify).toList());
        return isTyped(tuple) ? new RecRow(objectId, spell(tuple), false, tuple, 1, flat) : null;
    }

    /**
     * {@code s} with every tuple that is a rectangular block of one numeric or
     * boolean leaf as a {@link Block}, bottom-up. A slot at a leaf of a block
     * takes the block's type; a slot where a row of one belongs leaves it a tuple.
     */
    static Sig blockify(Sig s) {
        if (!(s instanceof Tuple t)) {
            return s;
        }
        List<Sig> parts = t.parts().stream().map(RecordArrays::blockify).toList();
        SequenceArrays.Leaf leaf = null;
        List<Integer> inner = null;
        boolean scalar = false;
        boolean slot = false;
        boolean block = false;
        for (Sig p : parts) {
            SequenceArrays.Leaf l;
            if (p == Slot.SLOT) {
                slot = true;
                continue;
            } else if (p instanceof Scalar x && x.leaf().kind() == Kind.NUMBER) {
                scalar = true;
                l = x.leaf().numeric();
            } else if (p instanceof Block b && (inner == null || inner.equals(b.dims()))) {
                block = true;
                inner = b.dims();
                l = b.leaf();
            } else {
                return new Tuple(parts);
            }
            if (leaf != null && leaf != l) {
                return new Tuple(parts);
            }
            leaf = l;
        }
        if (scalar && !block) {
            return new Block(leaf, List.of(parts.size()));
        }
        if (block && !scalar && !slot) {
            List<Integer> dims = new ArrayList<>();
            dims.add(parts.size());
            dims.addAll(inner);
            return new Block(leaf, List.copyOf(dims));
        }
        return new Tuple(parts);
    }

    private static Sig sigOf(Node e, SequenceArrays.TimeUnit unit, List<Object> leaves) {
        if (e.isNodeUndef()) {
            leaves.add(null);
            return Slot.SLOT;
        }
        if (e.isNodeSeq()) {
            List<Node> elements = e.getSeq();
            if (elements.isEmpty()) {
                return null;
            }
            List<Sig> parts = new ArrayList<>(elements.size());
            for (Node x : elements) {
                Sig s = sigOf(x, unit, leaves);
                if (s == null) {
                    return null;
                }
                parts.add(s);
            }
            return new Tuple(List.copyOf(parts));
        }
        Typed t = typed(e, unit);
        if (t == null) {
            return null;
        }
        leaves.add(t.value());
        return new Scalar(t.leaf());
    }

    /** The signature both items have, a slot taking the leaf the other has; null when they differ. */
    static Sig unify(Sig a, Sig b) {
        if (a == Slot.SLOT) {
            return (b == Slot.SLOT || b instanceof Scalar) ? b : null;
        }
        if (b == Slot.SLOT) {
            return (a instanceof Scalar) ? a : null;
        }
        if (a instanceof Tuple ta && b instanceof Tuple tb) {
            if (ta.parts().size() != tb.parts().size()) {
                return null;
            }
            List<Sig> parts = new ArrayList<>(ta.parts().size());
            for (int i = 0; i < ta.parts().size(); i++) {
                Sig u = unify(ta.parts().get(i), tb.parts().get(i));
                if (u == null) {
                    return null;
                }
                parts.add(u);
            }
            return new Tuple(List.copyOf(parts));
        }
        return a.equals(b) ? a : null;
    }

    private static boolean isTyped(Sig s) {
        return switch (s) {
            case Slot x -> false;
            case Tuple t -> t.parts().stream().allMatch(RecordArrays::isTyped);
            case Scalar x -> true;
            case Block x -> true;
        };
    }

    /** The spelling of a signature, as the grammar has it, without spaces. */
    static String spell(Sig s) {
        return switch (s) {
            case Scalar x -> x.leaf().spelling();
            case Block b -> b.leaf().tag + "[" + String.join("x", b.dims().stream().map(String::valueOf).toList()) + "]";
            case Tuple t -> "(" + String.join(",", t.parts().stream().map(RecordArrays::spell).toList()) + ")";
            case Slot x -> "_";
        };
    }

    /** A parsed signature: whether it is {@code list(…)}, and its item (or tuple) signature. */
    record Parsed(boolean list, Sig item) {
    }

    /** The signature {@code s} spells; IllegalArgumentException when it is none. */
    static Parsed parse(String s) {
        boolean list = s.startsWith("list(");
        int[] at = {list ? 5 : 0};
        Sig sig = parseSig(s, at);
        if (list) {
            expect(s, at, ')');
        }
        if (at[0] != s.length()) {
            throw new IllegalArgumentException("unexpected text at " + at[0]);
        }
        if (!list && !(sig instanceof Tuple)) {
            throw new IllegalArgumentException("a record signature is a tuple or list(...)");
        }
        return new Parsed(list, sig);
    }

    private static Sig parseSig(String s, int[] at) {
        if (at[0] < s.length() && s.charAt(at[0]) == '(') {
            at[0]++;
            List<Sig> parts = new ArrayList<>();
            while (true) {
                parts.add(parseSig(s, at));
                if (at[0] < s.length() && s.charAt(at[0]) == ',') {
                    at[0]++;
                    continue;
                }
                expect(s, at, ')');
                return new Tuple(List.copyOf(parts));
            }
        }
        int start = at[0];
        while (at[0] < s.length() && "(),[]".indexOf(s.charAt(at[0])) < 0) {
            at[0]++;
        }
        String token = s.substring(start, at[0]);
        if (at[0] < s.length() && s.charAt(at[0]) == '[') {
            SequenceArrays.Leaf leaf = SequenceArrays.Leaf.ofTag(token);
            int close = s.indexOf(']', at[0]);
            if (leaf == null || close < 0) {
                throw new IllegalArgumentException("'" + token + "[' at " + start + " is no numeric block");
            }
            List<Integer> dims = new ArrayList<>();
            for (String d : s.substring(at[0] + 1, close).split("x", -1)) {
                if (!d.matches("[1-9][0-9]{0,8}")) {
                    throw new IllegalArgumentException("dimension '" + d + "' at " + at[0]);
                }
                dims.add(Integer.parseInt(d));
            }
            at[0] = close + 1;
            return new Block(leaf, List.copyOf(dims));
        }
        RLeaf leaf = RLeaf.ofSpelling(token);
        if (leaf == null) {
            throw new IllegalArgumentException("'" + token + "' at " + start + " is no leaf");
        }
        return new Scalar(leaf);
    }

    private static void expect(String s, int[] at, char c) {
        if (at[0] >= s.length() || s.charAt(at[0]) != c) {
            throw new IllegalArgumentException("'" + c + "' expected at " + at[0]);
        }
        at[0]++;
    }

    /** A column: its name ({@code f3_1}), leaf, offset and width within an item, and block dims ({@code []} for a scalar). */
    record Column(String name, RLeaf leaf, int offset, int width, int[] dims) {
    }

    /** The columns of an item signature, in leaf order. */
    static List<Column> columns(Sig item) {
        List<Column> out = new ArrayList<>();
        walk(item, new ArrayList<>(), out, new int[]{0});
        return out;
    }

    private static void walk(Sig s, List<Integer> path, List<Column> out, int[] at) {
        switch (s) {
            case Scalar x -> {
                out.add(new Column(name(path), x.leaf(), at[0], 1, new int[0]));
                at[0]++;
            }
            case Block b -> {
                int width = 1;
                int[] dims = new int[b.dims().size()];
                for (int i = 0; i < dims.length; i++) {
                    dims[i] = b.dims().get(i);
                    width = Math.multiplyExact(width, dims[i]);
                }
                out.add(new Column(name(path), RLeaf.number(b.leaf()), at[0], width, dims));
                at[0] += width;
            }
            case Tuple t -> {
                for (int i = 0; i < t.parts().size(); i++) {
                    path.add(i + 1);
                    walk(t.parts().get(i), path, out, at);
                    path.remove(path.size() - 1);
                }
            }
            case Slot x -> throw new IllegalStateException("an untyped position");
        }
    }

    private static String name(List<Integer> path) {
        StringBuilder sb = new StringBuilder("f");
        for (int i = 0; i < path.size(); i++) {
            if (i > 0) {
                sb.append('_');
            }
            sb.append(path.get(i));
        }
        return sb.toString();
    }

    private static int itemWidth(List<Column> columns) {
        int w = 0;
        for (Column c : columns) {
            w += c.width();
        }
        return w;
    }

    /** At most this many language tags in one {@code lang} column: its codes are uint16. */
    static final int MAX_LANGS = 1 << 16;

    /**
     * Writes the record class {@code c} under {@code arrays}, an array at a
     * time from its rows: starts (a list class), then per column its values,
     * its {@code _lang} codes and its {@code _mask}, then seq.
     */
    static void writeClass(SequenceArrays.ClassDef c, SequenceArrays.RowSource rows, WritableGroup arrays, boolean compress,
                           SequenceArrays.TimeUnit unit) {
        WritableGroup g = arrays.putGroup(c.name());
        g.putAttribute("kind", "record");
        g.putAttribute("signature", c.signature());
        if (c.list()) {
            SequenceArrays.starts(g, c.rows(), compress, rows, row -> ((RecRow) row).items());
        }
        int width = itemWidth(c.columns());
        long length = c.list() ? c.items() : c.rows();
        for (int ci = 0; ci < c.columns().size(); ci++) {
            Column col = c.columns().get(ci);
            long[] shape = new long[1 + col.dims().length];
            List<String> dimNames = new ArrayList<>();
            shape[0] = length;
            dimNames.add(c.list() ? "item" : "seq");
            for (int i = 0; i < col.dims().length; i++) {
                shape[1 + i] = col.dims()[i];
                dimNames.add("d" + (i + 1));
            }
            TypedArray.Type type = col.leaf().columnType();
            List<String> tags = c.tags().get(ci);
            SequenceArrays.pass(g, col.name(), type, shape, dimNames, attributes(col.leaf(), unit, tags), compress, rows,
                    new Leaves(col, width) {
                        @Override
                        void put(Object piece, int at, Object v) {
                            switch (col.leaf().kind()) {
                                case NUMBER -> SequenceArrays.set(piece, type, at, v);
                                case STRING, IRI -> ((String[]) piece)[at] = (String) v;
                                case LANG -> ((String[]) piece)[at] = ((String[]) v)[0];
                                case DATETIME -> ((long[]) piece)[at] = (Long) v;
                                case DATE -> ((int[]) piece)[at] = Math.toIntExact((Long) v);
                            }
                        }
                    });
            if (tags != null) {
                SequenceArrays.pass(g, col.name() + "_lang", TypedArray.Type.U16, shape, dimNames, null, compress, rows,
                        new Leaves(col, width) {
                            @Override
                            void put(Object piece, int at, Object v) {
                                ((int[]) piece)[at] = Collections.binarySearch(tags, ((String[]) v)[1]);
                            }
                        });
            }
            if (c.columnSlot()[ci]) {
                SequenceArrays.pass(g, col.name() + "_mask", TypedArray.Type.BOOL, shape, dimNames, null, compress, rows,
                        new Leaves(col, width) {
                            @Override
                            void slot(Object piece, int at) {
                                ((boolean[]) piece)[at] = true;
                            }

                            @Override
                            void put(Object piece, int at, Object v) {
                            }
                        });
            }
        }
        SequenceArrays.seq(g, c.rows(), compress, rows);
    }

    /** A column's leaves of each row, item by item: {@link #put} a value, {@link #slot} a slot (left as it is). */
    private abstract static class Leaves implements SequenceArrays.Filler {
        private final Column col;
        private final int width;

        Leaves(Column col, int width) {
            this.col = col;
            this.width = width;
        }

        abstract void put(Object piece, int at, Object v);

        void slot(Object piece, int at) {
        }

        @Override
        public int elements(Object row) {
            return ((RecRow) row).items() * col.width();
        }

        @Override
        public void fill(Object row, Object piece, int at) {
            RecRow r = (RecRow) row;
            for (int j = 0; j < r.items(); j++) {
                for (int w = 0; w < col.width(); w++) {
                    Object v = r.leaves()[j * width + col.offset() + w];
                    if (v == null) {
                        slot(piece, at);
                    } else {
                        put(piece, at, v);
                    }
                    at++;
                }
            }
        }
    }

    /** "microseconds since 1970-01-01T00:00:00": a dateTime column's CF units. */
    static String units(SequenceArrays.TimeUnit unit) {
        return unit.cf + " since 1970-01-01T00:00:00";
    }

    static final String DATE_UNITS = "days since 1970-01-01";
    static final String CALENDAR = "proleptic_gregorian";

    private static Map<String, Object> attributes(RLeaf leaf, SequenceArrays.TimeUnit unit, List<String> tags) {
        Map<String, Object> a = new TreeMap<>();
        a.put("kind", leaf.spelling());
        if (leaf.datatype() != null) {
            a.put("datatype", leaf.datatype());
        }
        switch (leaf.kind()) {
            case DATETIME -> {
                a.put("units", units(unit));
                a.put("calendar", CALENDAR);
                if (!leaf.zone().isEmpty()) {
                    a.put("timezone", leaf.zone());
                }
            }
            case DATE -> {
                a.put("units", DATE_UNITS);
                a.put("calendar", CALENDAR);
            }
            case LANG -> a.put("langs", String.join(" ", tags));
            default -> {
            }
        }
        return a;
    }

    /**
     * {@code -verify -deep} of a record class: every row re-derived from the
     * canonical elements of the sequence its {@code seq} names, its signature
     * the class's, and each leaf the column's value there (slots against the
     * masks, language tags through the codes). Null when it holds; else what
     * does not.
     */
    static String verify(String name, StoreGroup g, com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader dict) {
        String signature = g.stringAttribute("signature");
        if (signature == null) {
            return "a record class without a signature";
        }
        Parsed parsed;
        try {
            parsed = parse(signature);
        } catch (IllegalArgumentException e) {
            return "signature '" + signature + "' does not parse: " + e.getMessage();
        }
        List<Column> columns = columns(parsed.item());
        int width = itemWidth(columns);
        TypedArray seqArray = g.typedArray("seq");
        if (seqArray == null || seqArray.type() != TypedArray.Type.I64) {
            return "seq missing, or not int64";
        }
        long[] seq = (long[]) seqArray.values();
        long[] starts = null;
        if (parsed.list()) {
            TypedArray s = g.typedArray("starts");
            if (s == null || s.type() != TypedArray.Type.I64 || s.elementCount() != seq.length + 1L) {
                return "starts missing, not int64, or not rows + 1 long";
            }
            starts = (long[]) s.values();
            if (starts[0] != 0) {
                return "starts does not begin at 0";
            }
        }
        TypedArray[] values = new TypedArray[columns.size()];
        boolean[][] masks = new boolean[columns.size()][];
        int[][] codes = new int[columns.size()][];
        String[][] langs = new String[columns.size()][];
        SequenceArrays.TimeUnit unit = null;
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            values[i] = g.typedArray(col.name());
            if (values[i] == null || values[i].type() != col.leaf().columnType()) {
                return "column " + col.name() + " missing, or not " + col.leaf().columnType();
            }
            if (!col.leaf().spelling().equals(values[i].attributes().get("kind"))) {
                return "column " + col.name() + " has kind '" + values[i].attributes().get("kind") + "', not '" + col.leaf().spelling() + "'";
            }
            if (col.leaf().kind() == Kind.DATETIME) {
                SequenceArrays.TimeUnit u = SequenceArrays.TimeUnit.ofUnits(String.valueOf(values[i].attributes().get("units")));
                if (u == null || (unit != null && u != unit)) {
                    return "column " + col.name() + " has units '" + values[i].attributes().get("units") + "'";
                }
                unit = u;
            }
            TypedArray m = g.typedArray(col.name() + "_mask");
            masks[i] = (m == null) ? null : (boolean[]) m.values();
            if (col.leaf().kind() == Kind.LANG) {
                TypedArray c = g.typedArray(col.name() + "_lang");
                Object tags = values[i].attributes().get("langs");
                if (c == null || c.type() != TypedArray.Type.U16 || !(tags instanceof String t)) {
                    return "column " + col.name() + " lacks its _lang codes or langs";
                }
                codes[i] = (int[]) c.values();
                langs[i] = t.split(" ", -1);
            }
        }
        if (unit == null) {
            unit = SequenceArrays.TimeUnit.US;   // no dateTime column: the unit types nothing
        }
        long item = 0;
        for (int r = 0; r < seq.length; r++) {
            if (r > 0 && seq[r] <= seq[r - 1]) {
                return "seq is not ascending at row " + r;
            }
            if (!dict.isSequenceObjectId(seq[r])) {
                return "row " + r + " names object " + seq[r] + ", which is no sequence";
            }
            RecRow row = probe(seq[r], dict.getObjects().extract(seq[r]), unit, 1);
            if (row == null || !row.signature().equals(signature)) {
                return "row " + r + " (sequence " + seq[r] + ") does not have the signature " + signature;
            }
            if (starts != null && starts[r + 1] - starts[r] != row.items()) {
                return "row " + r + " (sequence " + seq[r] + ") has " + row.items() + " items where the class holds "
                        + (starts[r + 1] - starts[r]);
            }
            for (int j = 0; j < row.items(); j++) {
                for (int i = 0; i < columns.size(); i++) {
                    Column col = columns.get(i);
                    for (int w = 0; w < col.width(); w++) {
                        long at = (item + j) * col.width() + w;
                        if (at >= values[i].elementCount()) {
                            return "column " + col.name() + " holds " + values[i].elementCount() + " values; the rows more";
                        }
                        Object expected = row.leaves()[j * width + col.offset() + w];
                        boolean slot = masks[i] != null && masks[i][(int) at];
                        if (expected == null ? !slot : (slot || !same(col, values[i], codes[i], langs[i], (int) at, expected))) {
                            return "row " + r + " (sequence " + seq[r] + ") differs at " + col.name() + "[" + at + "]";
                        }
                    }
                }
            }
            item += row.items();
        }
        if (starts != null && starts[seq.length] != item) {
            return "starts ends at " + starts[seq.length] + "; the rows hold " + item + " items";
        }
        for (int i = 0; i < columns.size(); i++) {
            if (values[i].elementCount() != item * columns.get(i).width()) {
                return "column " + columns.get(i).name() + " holds " + values[i].elementCount() + " values; the rows "
                        + item * columns.get(i).width();
            }
        }
        return null;
    }

    private static boolean same(Column col, TypedArray values, int[] codes, String[] langs, int at, Object expected) {
        return switch (col.leaf().kind()) {
            case NUMBER -> SequenceArrays.sameValue(values, values.type(), at, expected);
            case STRING, IRI -> ((String[]) values.values())[at].equals(expected);
            case LANG -> ((String[]) values.values())[at].equals(((String[]) expected)[0])
                    && codes[at] < langs.length && langs[codes[at]].equals(((String[]) expected)[1]);
            case DATETIME -> ((long[]) values.values())[at] == (Long) expected;
            case DATE -> ((int[]) values.values())[at] == (Long) expected;
        };
    }
}
