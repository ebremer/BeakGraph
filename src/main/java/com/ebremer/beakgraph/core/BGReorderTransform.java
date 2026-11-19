package com.ebremer.beakgraph.core;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Join reordering for BeakGraph's two indexes (GSPO + GPOS): the order of a BGP's triples that
 * keeps the rows the join carries smallest.
 *
 * <p>The query engine substitutes the incoming row's bindings into the BGP before calling this, so a
 * slot is "bound" when it is a concrete term or a variable an earlier triple of the order binds
 * ({@link BeakGraph#BOUND} stands for a variable bound outside the BGP, value unknown). The join
 * reads the triples left to right, an index lookup per row, so an order costs what it reads: for
 * each triple, a lookup per row it is handed plus the rows it hands on. A triple turns each row into
 * its <em>fan-out</em> of rows:
 * <ul>
 *   <li>subject and object free: its matches, exact when its predicate and object are terms;</li>
 *   <li>object bound: the subjects an object has - exact for a term, else rows ÷ distinct objects
 *       of its predicate;</li>
 *   <li>subject bound: the rows a subject has - rows ÷ distinct subjects of its predicate, at least
 *       one;</li>
 *   <li>both bound: a check, the chance the triple is there - at most one; a value check filters at
 *       its square root, and a type check, or one of an object a join bound, is trusted to remove
 *       at most half the rows ({@link #CHECK}).</li>
 * </ul>
 * Only a bound subject (GSPO) or predicate (GPOS) gives an index prefix; a triple bound on its object
 * alone scans its graphs, so it waits until the join binds its subject or predicate.
 *
 * <p>Every triple is tried as the first; the rest of each order is chosen greedily, the triple that
 * adds least next - a cross product only when nothing joined is left - and the cheapest order wins.
 * On a tie the order as written stays. Choosing each
 * step's cheapest triple from the start, as this used to, began {@code sockg/biomassCarbohydrate}
 * at {@code ?param a sockg:Parameter} (156 matches) - and the next join, {@code ?m sockg:of ?param},
 * turned those into 3.18 million measurements, about 20,000 a parameter. Begun at
 * {@code ?f a sockg:BiomassCarbohydrate} (1,367 matches), the join stays near that size.
 *
 * <p>The figures are those of the graphs the query reads ({@link Counts}) where the store can count
 * them, and otherwise the store-wide VoID statistics. Before exact counts, every
 * {@code ?s <p> <o>} in a large store came out below one match and tied, so the written order
 * decided: {@code ?fac rdf:type fio:Facility} (4.86 M matches) ran before
 * {@code ?fac fio:ofIndustry naics:NAICS-562212} (14,306), and the query timed out.
 */
final class BGReorderTransform implements ReorderTransformation {

    private static final Logger logger = LoggerFactory.getLogger(BGReorderTransform.class);

    /** Costs equal within this relative margin keep the earlier choice, so rounding never reorders. */
    private static final double TIE = 1e-9;

    /**
     * The most of its rows a type check - {@code ?s a <class>}, subject bound - or a check of an
     * object a join bound is trusted to remove. Its chance by the counts, {@code ?s a ex:Sample}'s
     * samples among all typed subjects, takes the subjects it is handed for random ones; but they
     * come from the join, and every subject that has a measurement is a sample. Trusted fully, such
     * a check made a large first pattern look small.
     *
     * <p>A value check is a filter, at the square root of its chance: the join may lean the same
     * way as the value - 114 of the abdomen's 838 parts in ubergraph are organs (14%), where the
     * counts give {@code ?organ rdfs:subClassOf organ:} 0.12% (3.4% so damped) - and the square
     * root is the usual backoff for that. Halving instead hid that
     * {@code ?cell spatial:connectedTo <county>} keeps few of a state's wells (0.04% by the counts,
     * 2% so damped), and that the wells so narrowed should come before
     * {@code federation/sawgraph-hydrology-spatial_03}'s cross product with its aquifers.
     */
    private static final double CHECK = 0.5;

    private static final Node TYPE = org.apache.jena.vocabulary.RDF.type.asNode();

    /** Rows and costs are capped here, so a long cross product cannot overflow to infinity. */
    private static final double CAP = 1e200;

    /**
     * What the graphs a query reads hold, summed over them; -1 for a figure that cannot be counted.
     */
    @FunctionalInterface
    interface Counts {
        /**
         * Exact matches of {@code ?s <predicate> <object>} - or {@code ?s <predicate> ?o} when
         * {@code object} is null.
         */
        long matches(Node predicate, Node object);

        /** Distinct objects of {@code predicate}. */
        default long objects(Node predicate) {
            return -1;
        }

        /** Distinct subjects of {@code predicate}. */
        default long subjects(Node predicate) {
            return -1;
        }

        /** Quads. */
        default long quads() {
            return -1;
        }

        /** Distinct subjects. */
        default long subjects() {
            return -1;
        }

        /** Distinct predicates. */
        default long predicates() {
            return -1;
        }
    }

    private final VoidStats stats;
    private final Counts counts;

    BGReorderTransform(VoidStats stats) {
        this(stats, null);
    }

    BGReorderTransform(VoidStats stats, Counts counts) {
        this.stats = stats;
        this.counts = counts;
    }

    /** The dataset statistics this transform costs by (shared by every view of a store). */
    VoidStats stats() {
        return stats;
    }

    @Override
    public BasicPattern reorder(BasicPattern pattern) {
        return reorderIndexes(pattern).reorder(pattern);
    }

    @Override
    public ReorderProc reorderIndexes(BasicPattern pattern) {
        int[] order = plan(pattern.getList());
        return original -> {
            BasicPattern out = new BasicPattern();
            for (int idx : order) {
                out.add(original.get(idx));
            }
            return out;
        };
    }

    /** The graphs' size: quads, distinct subjects, objects and predicates, each at least 1. */
    private record Totals(double quads, double subjects, double objects, double predicates) {}

    /**
     * A triple's figures that do not depend on what is bound: its predicate's rows, their distinct
     * subjects and objects, and its exact matches when its predicate and object are terms (else -1).
     * Null for a predicate that is not a term.
     */
    private record Shape(double rows, double subjects, double objects, double exact) {}

    /** One triple placed after what is already bound: index lookup cost per incoming row, rows out per row. */
    private record Step(double access, double fanout) {}

    /** The cheapest order of {@code triples} found, as indexes into it. */
    int[] plan(List<Triple> triples) {
        int n = triples.size();
        int[] best = new int[n];
        for (int i = 0; i < n; i++) {
            best[i] = i;
        }
        if (n < 2) {
            return best;
        }
        Totals totals = totals();
        Shape[] shapes = new Shape[n];
        for (int i = 0; i < n; i++) {
            shapes[i] = shape(triples.get(i), totals);
        }
        double bestCost = Double.POSITIVE_INFINITY;
        for (int start = 0; start < n; start++) {
            int[] order = new int[n];
            double cost = greedy(triples, shapes, totals, start, order);
            if (start == 0 || cost < bestCost * (1 - TIE)) {
                best = order;
                bestCost = cost;
            }
        }
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (int idx : best) {
                sb.append("\n    ").append(triples.get(idx));
            }
            logger.debug("reorder: estimated cost {}{}", String.format(java.util.Locale.ROOT, "%.4g", bestCost), sb);
        }
        return best;
    }

    /** The order the greedy pass makes beginning at {@code start} (tests pin its step rule by it). */
    int[] orderFrom(List<Triple> triples, int start) {
        Totals totals = totals();
        Shape[] shapes = new Shape[triples.size()];
        for (int i = 0; i < shapes.length; i++) {
            shapes[i] = shape(triples.get(i), totals);
        }
        int[] order = new int[triples.size()];
        greedy(triples, shapes, totals, start, order);
        return order;
    }

    /** The estimated cost of running {@code triples} in {@code order} (tests compare orders by it). */
    double cost(List<Triple> triples, int[] order) {
        Totals totals = totals();
        Set<Node> bound = new HashSet<>();
        double rows = 1, cost = 0;
        for (int idx : order) {
            Triple t = triples.get(idx);
            Step s = step(t, shape(t, totals), bound, totals);
            cost = Math.min(CAP, cost + rows * (s.access() + s.fanout()));
            rows = carried(rows, s.fanout());
            bindVars(bound, t);
        }
        return cost;
    }

    /**
     * Fills {@code order} with {@code start} and then, step by step, the triple adding least - its
     * lookup cost and rows out per incoming row - and returns that order's cost: per triple, a
     * lookup per row it is handed plus the rows it hands on.
     *
     * <p>A triple that shares no variable with what is bound is a cross product: it multiplies every
     * later step's rows and binds nothing they join on, so it waits until no joined triple is left -
     * unless it adds at most a row a row. Judged by its own rows alone, the cross product of
     * {@code federation/sawgraph-hydrology-spatial_03}, its sand-and-gravel aquifers, looked
     * cheaper than a well's {@code kwg-ont:sfWithin} cells and ran first; every step after it then
     * ran on wells × aquifers, and the query timed out.
     */
    private double greedy(List<Triple> triples, Shape[] shapes, Totals totals, int start, int[] order) {
        int n = triples.size();
        boolean[] used = new boolean[n];
        Set<Node> bound = new HashSet<>();
        double rows = 1, cost = 0;
        for (int k = 0; k < n; k++) {
            int next = start;
            Step step = null;
            if (k > 0) {
                next = -1;
                boolean nextJoins = false;
                double least = Double.POSITIVE_INFINITY;
                for (int i = 0; i < n; i++) {
                    if (used[i]) {
                        continue;
                    }
                    Step s = step(triples.get(i), shapes[i], bound, totals);
                    boolean joins = s.fanout() <= 1 || sharesVariable(triples.get(i), bound);
                    double c = s.access() + s.fanout();
                    if (next < 0 || (joins && !nextJoins) || (joins == nextJoins && c < least * (1 - TIE))) {
                        next = i;
                        nextJoins = joins;
                        least = c;
                        step = s;
                    }
                }
            } else {
                step = step(triples.get(next), shapes[next], bound, totals);
            }
            cost = Math.min(CAP, cost + rows * (step.access() + step.fanout()));
            rows = carried(rows, step.fanout());
            used[next] = true;
            order[k] = next;
            bindVars(bound, triples.get(next));
        }
        return cost;
    }

    /**
     * The rows expected after a triple with {@code fanout} - below one when a check likely ends the
     * row. Held at one or more, a fully bound check (a well's cell connected to an aquifer, which
     * fails for most of a well-by-aquifer cross product) saved later steps nothing, and ran after
     * them; checks are damped ({@link #CHECK}), so a small chance stays a fair one.
     */
    private static double carried(double rows, double fanout) {
        double r = rows * fanout;
        return (r <= 0) ? 0 : Math.min(CAP, r);
    }

    private Step step(Triple t, Shape shape, Set<Node> bound, Totals totals) {
        boolean s = isBound(t.getSubject(), bound);
        boolean p = isBound(t.getPredicate(), bound);
        boolean o = isBound(t.getObject(), bound);
        double rows, subjects, objects, exact = -1;
        if (shape != null) {
            rows = shape.rows();
            subjects = shape.subjects();
            objects = shape.objects();
            exact = shape.exact();
        } else if (p) {
            // A variable predicate bound earlier (or BeakGraph.BOUND): some predicate, an average one.
            rows = Math.max(1, totals.quads() / totals.predicates());
            subjects = Math.min(rows, totals.subjects());
            objects = Math.min(rows, totals.objects());
        } else {
            rows = totals.quads();
            subjects = totals.subjects();
            objects = totals.objects();
        }
        // The subjects one object has, and the rows one subject has.
        double perObject = (exact >= 0) ? exact : rows / objects;
        double perSubject = Math.max(1, rows / subjects);
        double fanout;
        if (rows <= 0) {
            fanout = 0;
        } else if (!s && !o) {
            fanout = rows;
        } else if (!s) {
            fanout = perObject;
        } else if (!o) {
            fanout = perSubject;
        } else if (perObject <= 0) {
            fanout = 0;                 // an object the predicate never has: nothing passes
        } else {
            double chance = Math.min(1, perObject / subjects);
            // A value - an object term, not a class - filters, at the square root of its chance. A
            // class, or an object a join bound, is mostly what the join hands it anyway (CHECK).
            boolean filters = known(t.getObject()) && !TYPE.equals(t.getPredicate());
            fanout = filters ? Math.sqrt(chance) : Math.max(CHECK, chance);
        }
        double access;
        if (s || p) {
            // A GSPO or GPOS prefix; with only the subject and object bound, the subject's rows are read.
            access = (s && !p && o) ? Math.max(1, totals.quads() / totals.subjects()) : 1;
        } else {
            access = totals.quads();    // no index prefix: a scan of the graphs
        }
        return new Step(access, fanout);
    }

    /** {@code t}'s bound-independent figures, or null when its predicate is not a term. */
    private Shape shape(Triple t, Totals totals) {
        Node p = t.getPredicate();
        if (!known(p)) {
            return null;
        }
        long r = (counts != null) ? counts.matches(p, null) : -1;
        if (r < 0) {
            r = stats.predicateCount(p);
        }
        double rows = (r >= 0) ? r : Math.max(1, totals.quads() / totals.predicates());
        long o = (counts != null) ? counts.objects(p) : -1;
        double objects = Math.max(1, (o >= 0) ? Math.min(o, rows) : Math.min(rows, totals.objects()));
        long sp = (counts != null) ? counts.subjects(p) : -1;
        double subjects = Math.max(1, (sp >= 0) ? Math.min(sp, rows) : Math.min(rows, totals.subjects()));
        double exact = -1;
        // Counted while the subject is a variable: a concrete subject is a row's substituted value,
        // and a BGP run per row of an index join is reordered per row.
        if (counts != null && known(t.getObject()) && !t.getSubject().isConcrete()) {
            exact = counts.matches(p, t.getObject());
        }
        return new Shape(rows, subjects, objects, exact);
    }

    private Totals totals() {
        long q = (counts != null) ? counts.quads() : -1;
        long s = (counts != null) ? counts.subjects() : -1;
        long p = (counts != null) ? counts.predicates() : -1;
        double storeQuads = Math.max(1, stats.totalTriples());
        double quads = Math.max(1, (q >= 0) ? q : stats.totalTriples());
        double subjects = Math.max(1, (s >= 0) ? s : stats.distinctSubjects());
        double predicates = Math.max(1, (p >= 0) ? p : stats.distinctPredicates());
        // No index counts the graphs' distinct objects: the store's, in proportion to their quads.
        double objects = Math.max(1, stats.distinctObjects() * Math.min(1, quads / storeQuads));
        return new Totals(quads, subjects, objects, predicates);
    }

    /** A term whose value is known: concrete, and not BeakGraph.BOUND (a bound variable, value unknown). */
    private static boolean known(Node n) {
        return n.isConcrete() && !BeakGraph.BOUND.equals(n);
    }

    /** True when a variable of {@code t} - an embedded one too - is in {@code bound}. */
    private static boolean sharesVariable(Triple t, Set<Node> bound) {
        Set<Node> vars = new HashSet<>();
        bindVars(vars, t);
        for (Node v : vars) {
            if (bound.contains(v)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isBound(Node n, Set<Node> bound) {
        return n.isConcrete() || (n.isVariable() && bound.contains(n));
    }

    private static void bindVars(Set<Node> bound, Triple t) {
        if (t.getSubject().isVariable())   bound.add(t.getSubject());
        if (t.getPredicate().isVariable()) bound.add(t.getPredicate());
        addObjectVars(bound, t.getObject());
    }

    /**
     * Records the object position's produced variables: the object itself, or -
     * for a var-containing triple-term pattern - every variable embedded in it
     * (all depths). Without this a later triple joining on an embedded variable
     * was costed as if the join were free-floating (wrong order, not wrong
     * answers).
     */
    private static void addObjectVars(Set<Node> bound, Node o) {
        if (o.isVariable()) {
            bound.add(o);
            return;
        }
        if (o.isTripleTerm() && !o.isConcrete()) {
            Triple t = o.getTriple();
            addObjectVars(bound, t.getSubject());
            addObjectVars(bound, t.getPredicate());
            addObjectVars(bound, t.getObject());
        }
    }
}
