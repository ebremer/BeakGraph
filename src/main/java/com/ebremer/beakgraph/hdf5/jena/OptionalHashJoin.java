package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexCounts;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.main.iterator.QueryIterOptionalIndex;
import org.apache.jena.sparql.serializer.SerializationContext;

/**
 * {@code OPTIONAL { ?s <p> ?o }} read once into a hash table, when the rows it is joined to are many.
 *
 * <p>Jena runs an OPTIONAL a row at a time ({@link QueryIterOptionalIndex}): the optional part, the row's
 * values substituted, is executed for each row - an index iterator built per row. OKN's
 * {@code scales/jail-population} joins {@code OPTIONAL { ?booking nc:EndDate ?releaseDate }} to 5.2 million
 * rows (each case's docket entries multiply its bookings), and spent 57% of its 21 s building them, for a
 * predicate with 4 million triples. Read once, the 4 million pairs are a table each row is a probe of.
 *
 * <p>Which way is decided by the rows: up to max({@value #MIN_ROWS}, the pattern's rows ÷ {@value #RATIO})
 * of them are taken first; if no more come, they go a row at a time, as Jena does. A row that does not bind
 * {@code ?s}, binds {@code ?o} already, or binds {@code ?s} to a predicate's id, goes a row at a time too, so
 * the answer is Jena's in every case. Patterns of more than {@value #MAX_ROWS} rows are not read into memory.
 */
public final class OptionalHashJoin {

    /** Hash joins run: tests see which way an OPTIONAL went. */
    public static final AtomicLong HITS = new AtomicLong();

    /** At least this many rows are taken before reading the pattern in full is considered ({@code beakgraph.optional.hash.minrows}). */
    static final long MIN_ROWS = 4096;

    /** A row looked up costs about as much as this many pairs read into the table. */
    static final long RATIO = 32;

    /** Patterns with more rows than this stay a lookup per row: the table would be too large. */
    static final long MAX_ROWS = 8_000_000;

    private OptionalHashJoin() {}

    /** The optional part's shape and the graph it is read in. */
    record Shape(Var s, Node p, Var o, Triple triple, BeakGraph graph, HDF5Reader reader, long rows) {}

    /**
     * The shape when {@code right} is {@code ?s <p> ?o} - one pattern, a term for the predicate, two different
     * variables - over a BeakGraph whose rows of it can be counted and are not too many; else null.
     */
    static Shape shape(Op right, ExecutionContext execCxt) {
        if (!(right instanceof OpBGP bgp) || bgp.getPattern().size() != 1) return null;
        Triple t = bgp.getPattern().get(0);
        if (!t.getSubject().isVariable() || !t.getObject().isVariable() || !t.getPredicate().isURI()) return null;
        if (t.getSubject().equals(t.getObject())) return null;
        if (!(execCxt.getActiveGraph() instanceof BeakGraph bg) || !(bg.getReader() instanceof HDF5Reader reader)) return null;
        if (!(reader.getDictionary() instanceof com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader dict)) return null;
        long pi = dict.getPredicates().locate(t.getPredicate());
        List<Node> graphs = (bg.getMemberGraphs() != null) ? bg.getMemberGraphs() : List.of(bg.getNamedGraph());
        long rows = 0;
        if (pi >= 1) {
            for (long gi : reader.graphIdsOf(graphs)) {
                long[] n = IndexCounts.predicateRowsAndObjects(reader, gi, pi);
                if (n == null) return null;
                rows += n[0];
            }
        }
        if (rows > MAX_ROWS) return null;
        return new Shape(Var.alloc(t.getSubject()), t.getPredicate(), Var.alloc(t.getObject()), t, bg, reader, rows);
    }

    static QueryIterator execute(QueryIterator left, Op right, Shape shape, ExecutionContext execCxt) {
        return new Join(left, right, shape, execCxt);
    }

    private static final class Join extends QueryIter1 {
        private final Op right;
        private final Shape shape;
        private final NodeTable nodeTable;
        private final ArrayDeque<Binding> out = new ArrayDeque<>();
        private QueryIterator delegate;   // a row at a time, when the rows were few
        private Iterator<Binding> rows;   // the rows, when the table is read
        private LongMultiMap table;
        private boolean started;

        Join(QueryIterator left, Op right, Shape shape, ExecutionContext execCxt) {
            super(left, execCxt);
            this.right = right;
            this.shape = shape;
            this.nodeTable = shape.reader().getNodeTable();
        }

        private void start() {
            started = true;
            QueryIterator input = getInput();
            long take = Math.max(Long.getLong("beakgraph.optional.hash.minrows", MIN_ROWS), shape.rows() / RATIO);
            List<Binding> first = new ArrayList<>();
            while (first.size() < take && input.hasNext()) {
                first.add(input.next());
            }
            if (!input.hasNext()) {
                delegate = new QueryIterOptionalIndex(QueryIterPlainWrapper.create(first.iterator(), getExecContext()), right, getExecContext());
                return;
            }
            HITS.incrementAndGet();
            table = new LongMultiMap();
            Iterator<BindingNodeId> pairs = shape.graph().read(new BindingNodeId(), shape.triple(), null);
            while (pairs.hasNext()) {
                BindingNodeId b = pairs.next();
                table.put(NodeId.id(b.get(shape.s())), b.get(shape.o()));
            }
            Iterator<Binding> rest = input;
            rows = new Iterator<>() {
                private final Iterator<Binding> head = first.iterator();
                @Override public boolean hasNext() { return head.hasNext() || rest.hasNext(); }
                @Override public Binding next() { return head.hasNext() ? head.next() : rest.next(); }
            };
        }

        @Override
        protected boolean hasNextBinding() {
            if (!started) start();
            if (delegate != null) return delegate.hasNext();
            while (out.isEmpty() && rows.hasNext()) {
                expand(rows.next());
            }
            return !out.isEmpty();
        }

        @Override
        protected Binding moveToNextBinding() {
            if (delegate != null) return delegate.next();
            return out.poll();
        }

        /** The rows {@code row} makes: with each match of the pattern, or alone. */
        private void expand(Binding row) {
            Var s = shape.s(), o = shape.o();
            if (!row.contains(s) || row.contains(o)) {
                byOneRow(row);
                return;
            }
            long id;
            if (row instanceof BindingBG bb && bb.getGraph().getReader() == shape.reader()) {
                id = bb.getBindingId().get(s);
            } else {
                id = nodeTable.getNodeIdForNode(row.get(s));
            }
            if (id == NodeId.NONE || NodeId.isPredicateSpace(id)) {
                byOneRow(row); // an id the table is not keyed by: Jena's way
                return;
            }
            if (NodeId.type(id) == NodeType.OBJECT) {
                // Bound in an object position: an entity shares its id with the subject position, but a
                // literal is never a subject, and a triple term goes Jena's way.
                Node term = row.get(s);
                if (term.isLiteral()) {
                    out.add(row);
                    return;
                }
                if (term.isTripleTerm()) {
                    byOneRow(row);
                    return;
                }
            }
            boolean matched = false;
            if (!NodeId.isSpecial(id)) {
                for (int i = table.first(NodeId.id(id)); i >= 0; i = table.next(i)) {
                    Node n = nodeTable.getNodeForNodeId(table.value(i));
                    if (n != null) {
                        out.add(BindingFactory.binding(row, o, n));
                        matched = true;
                    }
                }
            }
            if (!matched) {
                out.add(row);
            }
        }

        private void byOneRow(Binding row) {
            QueryIterator one = new QueryIterOptionalIndex(QueryIterSingleton.create(row, getExecContext()), right, getExecContext());
            try {
                one.forEachRemaining(out::add);
            } finally {
                one.close();
            }
        }

        @Override
        protected void requestSubCancel() {
            if (delegate != null) delegate.cancel();
        }

        @Override
        protected void closeSubIterator() {
            if (delegate != null) delegate.close();
            table = null;
        }

        @Override
        protected void details(IndentedWriter out, SerializationContext sCxt) {
            out.print("OptionalHashJoin " + shape.triple());
        }
    }

    /** A multimap of long keys to long values in arrays: open addressing for the keys, chains for the values. */
    public static final class LongMultiMap {
        private long[] keys = new long[1 << 10];
        private int[] heads = new int[1 << 10];
        private int used;
        private long[] values = new long[1 << 10];
        private int[] next = new int[1 << 10];
        private int size;

        public LongMultiMap() {
            Arrays.fill(keys, -1);
        }

        public void put(long key, long value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
                next = Arrays.copyOf(next, size * 2);
            }
            if ((used + 1) * 2 > keys.length) {
                rehash();
            }
            int slot = slot(key);
            if (keys[slot] == -1) {
                keys[slot] = key;
                heads[slot] = -1;
                used++;
            }
            values[size] = value;
            next[size] = heads[slot];
            heads[slot] = size++;
        }

        /** The first value's position for {@code key}, or -1. */
        public int first(long key) {
            int slot = slot(key);
            return (keys[slot] == key) ? heads[slot] : -1;
        }

        public int next(int position) {
            return next[position];
        }

        public long value(int position) {
            return values[position];
        }

        private int slot(long key) {
            int mask = keys.length - 1;
            int i = (int) (mix(key) & mask);
            while (keys[i] != -1 && keys[i] != key) {
                i = (i + 1) & mask;
            }
            return i;
        }

        private void rehash() {
            long[] oldKeys = keys;
            int[] oldHeads = heads;
            keys = new long[oldKeys.length * 2];
            heads = new int[oldKeys.length * 2];
            Arrays.fill(keys, -1);
            for (int i = 0; i < oldKeys.length; i++) {
                if (oldKeys[i] != -1) {
                    int slot = slot(oldKeys[i]);
                    keys[slot] = oldKeys[i];
                    heads[slot] = oldHeads[i];
                }
            }
        }

        private static long mix(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            return k;
        }
    }
}
