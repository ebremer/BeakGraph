package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpAssign;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpTopN;
import org.apache.jena.sparql.algebra.walker.Walker;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.E_Function;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprFunction0;
import org.apache.jena.sparql.expr.ExprFunction1;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprFunction3;
import org.apache.jena.sparql.expr.ExprFunctionN;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprSystem;
import org.apache.jena.sparql.expr.ExprVisitorBase;
import org.apache.jena.sparql.expr.Unstable;
import org.apache.jena.sparql.function.Function;
import org.apache.jena.sparql.function.FunctionFactory;
import org.apache.jena.sparql.function.FunctionRegistry;

/**
 * Answers kept, so that a query asked again is answered without running it.
 *
 * <p>A BeakGraph store is read-only - adds, deletes and write transactions are refused - so the open store gives the
 * same answer to the same query every time. An answer is kept under the open store itself (not its file: a store
 * reopened is a new key), the query as parsed (so comments and spacing don't matter), the base that relative IRIs
 * are resolved against, and the response format. Only complete answers are kept: not one cut short by the time
 * limit, an error or a client that went away, nor one larger than {@code beakgraph.result.cache.entry.mb}. All of
 * them together are held to {@code beakgraph.result.cache.mb} (by default a sixteenth of the heap), the least used
 * going first; 0 turns the cache off.
 *
 * <p>Never kept: a query whose answer can change between runs - {@code NOW()}, {@code RAND()}, {@code UUID()},
 * {@code STRUUID()}, {@code BNODE()}, their {@code afn:} forms - or that calls another endpoint ({@code SERVICE}).
 * Each response says what happened in {@value #HEADER}: {@code hit}, {@code miss} (run, and kept if it qualified)
 * or {@code bypass} (run, never kept).
 */
final class ResultCache {

    /** The response header that tells a client whether its answer came from the cache. */
    static final String HEADER = "X-BeakGraph-Cache";

    /** What the cache key holds besides the store. */
    record Key(long store, String query, String base, String format) {}

    /** A kept answer: its content type and body. */
    record Entry(String contentType, byte[] body) {}

    private static final long MB = 1L << 20;
    private static final AtomicLong NEXT_STORE = new AtomicLong();
    /** A number for each open store, compared by identity, forgotten when the store is gone. */
    private static final Cache<BeakGraph, Long> STORES = Caffeine.newBuilder().weakKeys().build();

    private static volatile Cache<Key, Entry> answers;
    private static volatile long entryLimit;

    static {
        configureFromProperties();
    }

    private ResultCache() {}

    /** Replaces the cache, empty, with the limits the system properties give (or the defaults). */
    static void configureFromProperties() {
        long total = Long.getLong("beakgraph.result.cache.mb", Runtime.getRuntime().maxMemory() / 16 / MB) * MB;
        long entry = Long.getLong("beakgraph.result.cache.entry.mb", Math.max(1, total / 8 / MB)) * MB;
        configure(total, entry);
    }

    /** Replaces the cache, empty, with these limits in bytes; a total of 0 turns it off. */
    static synchronized void configure(long totalBytes, long entryBytes) {
        entryLimit = Math.min(Math.min(entryBytes, totalBytes), Integer.MAX_VALUE - 1024);
        answers = (totalBytes <= 0) ? null : Caffeine.newBuilder()
                .maximumWeight(totalBytes)
                .weigher((Key k, Entry e) -> e.body().length + 2 * k.query().length() + 2 * k.base().length() + 64)
                .build();
    }

    /** Empties the cache. */
    static void clear() {
        Cache<Key, Entry> c = answers;
        if (c != null) c.invalidateAll();
    }

    /** The largest body kept, in bytes. */
    static long entryLimit() {
        return entryLimit;
    }

    /**
     * The key for {@code query} (parsed, with the protocol's dataset applied) over {@code ds}, or null when the answer
     * is not to be kept: the cache is off, the dataset is not a BeakGraph store's, or the query's answer may change.
     */
    static Key key(Dataset ds, Query query, String base, String format) {
        if (answers == null || format == null) return null;
        if (!(ds.asDatasetGraph() instanceof BGDatasetGraph dsg) || dsg.getBeakGraph() == null) return null;
        if (!stable(query)) return null;
        long store = STORES.get(dsg.getBeakGraph(), g -> NEXT_STORE.incrementAndGet());
        return new Key(store, query.serialize(), (base == null) ? "" : base, format);
    }

    static Entry get(Key key) {
        Cache<Key, Entry> c = answers;
        return (c == null || key == null) ? null : c.getIfPresent(key);
    }

    static void put(Key key, String contentType, byte[] body) {
        Cache<Key, Entry> c = answers;
        if (c != null && key != null && body.length <= entryLimit) {
            c.put(key, new Entry(contentType, body));
        }
    }

    /** False when the query's answer may differ from one run to the next. */
    static boolean stable(Query query) {
        AtomicBoolean unstable = new AtomicBoolean();
        try {
            walk(Algebra.compile(query), unstable);
        } catch (RuntimeException e) {
            return false; // can't tell: don't keep it
        }
        return !unstable.get();
    }

    /**
     * Every operator of {@code op} and every expression they hold. Jena's walker leaves out some expressions
     * (aggregates, sort conditions), so each operator that holds expressions has them walked here.
     */
    private static void walk(Op op, AtomicBoolean unstable) {
        Exprs exprs = new Exprs(unstable);
        Walker.walk(op, new OpVisitorBase() {
            @Override public void visit(OpService opService) {
                unstable.set(true); // another endpoint's answer
            }
            @Override public void visit(OpFilter f) { exprs.walk(f.getExprs()); }
            @Override public void visit(OpLeftJoin j) { exprs.walk(j.getExprs()); }
            @Override public void visit(OpExtend e) { exprs.walk(e.getVarExprList()); }
            @Override public void visit(OpAssign a) { exprs.walk(a.getVarExprList()); }
            @Override public void visit(OpGroup g) {
                exprs.walk(g.getGroupVars());
                for (ExprAggregator a : g.getAggregators()) exprs.walk(a.getAggregator().getExprList());
            }
            @Override public void visit(OpOrder o) { exprs.walk(o.getConditions()); }
            @Override public void visit(OpTopN t) { exprs.walk(t.getConditions()); }
        });
    }

    /** Marks a function whose value can change between runs, looking into EXISTS as well. */
    private static final class Exprs extends ExprVisitorBase {
        private final AtomicBoolean unstable;

        Exprs(AtomicBoolean unstable) {
            this.unstable = unstable;
        }

        void walk(Expr e) {
            if (e != null) Walker.walk(e, this);
        }

        void walk(ExprList list) {
            if (list != null) list.forEach(this::walk);
        }

        void walk(VarExprList list) {
            if (list != null) list.getExprs().values().forEach(this::walk);
        }

        void walk(List<SortCondition> conditions) {
            if (conditions != null) conditions.forEach(c -> walk(c.getExpression()));
        }

        private void check(Expr e) {
            if (e instanceof Unstable || e instanceof ExprSystem) {
                unstable.set(true);
            } else if (e instanceof E_Function f && unstableFunction(f.getFunctionIRI())) {
                unstable.set(true);
            }
        }

        @Override public void visit(ExprFunction0 f) { check(f); }
        @Override public void visit(ExprFunction1 f) { check(f); }
        @Override public void visit(ExprFunction2 f) { check(f); }
        @Override public void visit(ExprFunction3 f) { check(f); }
        @Override public void visit(ExprFunctionN f) { check(f); }

        @Override public void visit(ExprFunctionOp f) {
            check(f);
            ResultCache.walk(f.getGraphPattern(), unstable);
        }

        @Override public void visit(ExprAggregator a) {
            walk(a.getAggregator().getExprList());
        }
    }

    /** Jena's functions by IRI ({@code afn:now}, {@code afn:uuid}, ...) whose value changes from run to run. */
    private static boolean unstableFunction(String iri) {
        FunctionFactory factory = FunctionRegistry.get().get(iri);
        if (factory == null) return false; // unknown: the query fails, and nothing is kept
        try {
            Function f = factory.create(iri);
            return f instanceof org.apache.jena.sparql.function.library.now
                    || f instanceof org.apache.jena.sparql.function.library.uuid
                    || f instanceof org.apache.jena.sparql.function.library.struuid
                    || f instanceof org.apache.jena.sparql.function.library.bnode
                    || f instanceof Unstable;
        } catch (RuntimeException e) {
            return true; // can't tell: don't keep it
        }
    }

    /** A copy of a response body as it is written, given up once it passes {@link #entryLimit()}. */
    static final class Capture {
        private ByteArrayOutputStream copy = new ByteArrayOutputStream(8192);
        private final long limit = entryLimit;

        void write(int b) {
            if (copy == null) return;
            if (copy.size() + 1 > limit) { copy = null; return; }
            copy.write(b);
        }

        void write(byte[] b, int off, int len) {
            if (copy == null) return;
            if (copy.size() + (long) len > limit) { copy = null; return; }
            copy.write(b, off, len);
        }

        /** The whole body, or null if it grew past the limit. */
        byte[] bytes() {
            return (copy == null) ? null : copy.toByteArray();
        }
    }
}
