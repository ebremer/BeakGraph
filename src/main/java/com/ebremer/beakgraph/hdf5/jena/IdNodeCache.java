package com.ebremer.beakgraph.hdf5.jena;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.jena.graph.Node;

/**
 * The terms of a store by their ids, as many as were last asked for: what
 * {@link SimpleNodeTable} asks before it goes to the dictionary. Every value
 * a query reads - a row written out, a {@code COUNT(?o)}, a filter - comes
 * through here, so a hit has to cost next to nothing.
 *
 * <p>The ids of a dictionary are counted up from 1 in the order of its
 * terms, and the indexes hand them out in that order, so the ids a query
 * asks for lie close together: the subjects of a graph one after another,
 * the objects of a subject in a run. The cache is built on that. The terms
 * of {@value #PAGE} consecutive ids share a <em>page</em>, an array of
 * {@value #PAGE} references, and a table finds the page of an id. A hit is
 * the table's slot, the page, and the reference - and for the next id most
 * likely the same three places again. No key object, no lock, no record of
 * the read, and nothing done between requests. The cache this replaces
 * (Caffeine, keyed by a boxed {@code Long}) kept an eviction policy up to
 * date on every read and ran its upkeep on the JVM's common pool
 * (PERFORMANCE.md).
 *
 * <p>The table is buckets of {@value #WAYS} slots; a page is in one of the
 * slots of its bucket, and a page that finds its bucket full of others puts
 * one of them out. The table is kept no more than an eighth full, so that
 * seldom happens (to one new page in a thousand at the fullest), and three
 * pages in four are the first of their bucket: a lookup reads the slot and
 * opens the page, and its own number says that it is the one. The pages of
 * a run of {@value #RUN} - 4,096 ids - have buckets that follow one another,
 * and where a run begins is hashed: a query that walks the ids in order
 * walks the table in order too, which is what a processor fetches ahead of
 * the reading, and no two stretches of ids can fall on each other more than
 * chance would have them.
 *
 * <p>The same order is why the terms come to lie in it. A collection moves
 * what it keeps in the order it finds it, and it finds the terms through
 * this table: page after page, and after each page its terms. A query in
 * the order of the ids then reads memory from one end to the other.
 *
 * <p>How it stays bounded, in the terms of
 * {@code beakgraph.nodetable.cache.size}:
 * <ul>
 *   <li><b>The terms</b> weigh what {@link SimpleNodeTable#weightOf} says -
 *       1, or more for a long literal, whose node keeps its parsed value
 *       with it. Each page counts the weight of its own terms and the cache
 *       the weight of all. Over the budget, pages are dropped whole, from a
 *       position that moves round the table, until a tenth of the budget is
 *       free: the terms of the last query make room for those of the next,
 *       where a rule that only refused new terms would keep the old ones for
 *       ever.</li>
 *   <li><b>The pages</b> are made when a term of theirs is first kept, so
 *       there are never more of them than terms, and for ids that lie
 *       together a sixteenth as many. A page is 144 bytes with its slots:
 *       nine bytes a term when its ids are all in use, and about what an
 *       entry of the old cache cost when only one of them is.</li>
 *   <li><b>The table</b> starts small and doubles while more than an eighth
 *       of it is in use. A store that is opened and asked little holds a
 *       kilobyte of table.</li>
 * </ul>
 *
 * <p>Any number of threads may read and write, and none waits for another
 * to read, to keep a term or to make a page. A page stays the same object
 * for as long as it is kept, wherever in the table it lies, so a thread that
 * holds one can go on writing to it while the table grows. Growing and
 * dropping take a lock; growing empties the old table slot by slot as it
 * goes ({@link #MOVED}), so a page made meanwhile is either carried over or
 * not made at all. A page that leaves the table closes its count
 * ({@link #GONE}) and takes its weight with it; a term written to it after
 * that is not counted and not found. The weights are therefore exact
 * whenever nothing is being written, and the bound is the budget, give or
 * take the terms being written at one moment.
 */
final class IdNodeCache {

    /** Bits of an id that choose a term within its page. */
    static final int PAGE_BITS = 4;
    /** Terms of a page: one cache line of references. */
    static final int PAGE = 1 << PAGE_BITS;
    /** Slots of a bucket. */
    static final int WAYS = 4;
    /** Slots the table has for each page it holds, at the least. */
    static final int SPARE = 8;
    /** Bits of a page number that choose its bucket within its run. */
    static final int RUN_BITS = 8;
    /** Pages whose buckets follow one another. */
    static final int RUN = 1 << RUN_BITS;
    /** Slots of a new or cleared table. */
    static final int MIN_SLOTS = 1 << 8;
    /** No table larger than this, whatever the budget: 2^26 slots are 256 MB of references. */
    static final int MAX_SLOTS = 1 << 26;

    private static final long GOLDEN = 0x9E3779B97F4A7C15L;
    /** The count of a page that has left the table. */
    private static final int GONE = Integer.MIN_VALUE;
    private static final VarHandle TERMS = MethodHandles.arrayElementVarHandle(Node[].class);

    /** The terms of {@value #PAGE} consecutive ids; as a number, the weight of those it holds. */
    private static final class Page extends AtomicInteger {
        private static final long serialVersionUID = 1L;

        final long number;
        final transient Node[] terms = new Node[PAGE];

        Page(long number) {
            this.number = number;
        }
    }

    /** What a slot of a table that is being replaced holds once it has been emptied: no id has this page number. */
    private static final Page MOVED = new Page(-1L);

    /** The slots, how many of them hold a page, and what makes a bucket of a page number. */
    private static final class Table {
        final AtomicReferenceArray<Page> slots;
        final AtomicInteger pages = new AtomicInteger();
        final int shift;
        final int mask;

        Table(int length) {
            int buckets = length / WAYS;
            this.slots = new AtomicReferenceArray<>(length);
            this.shift = 64 - Integer.numberOfTrailingZeros(buckets);
            this.mask = buckets - 1;
        }

        /** The first slot of the bucket of a page: where its run begins, multiplied apart from the others, and its place in the run. */
        int bucket(long number) {
            int run = (int) (((number >>> RUN_BITS) * GOLDEN) >>> shift);
            return ((run + ((int) number & (RUN - 1))) & mask) * WAYS;
        }

        Page find(long number) {
            int first = bucket(number);
            for (int k = first; k < first + WAYS; k++) {
                Page p = slots.getAcquire(k);
                if (p != null && p.number == number) {
                    return p;
                }
            }
            return null;
        }
    }

    private final long maxWeight;
    private final int maxSlots;
    private volatile Table table = new Table(MIN_SLOTS);
    private final AtomicLong weight = new AtomicLong();
    private final Object restructuring = new Object();
    private int sweepAt;    // where the next sweep begins; guarded by restructuring
    private int turn;       // which slot of a full bucket gives way next; written without care, any value will do

    /**
     * @param maxWeight the budget: the weights of the terms kept, summed, stay under it
     */
    IdNodeCache(long maxWeight) {
        this.maxWeight = Math.max(0, maxWeight);
        // At the worst every term is alone on its page: a page for each term of weight 1 the budget has.
        long wanted = Math.min(MAX_SLOTS, Math.max(MIN_SLOTS, SPARE * Math.min(this.maxWeight, MAX_SLOTS)));
        this.maxSlots = (int) Math.min(MAX_SLOTS, Long.highestOneBit(wanted - 1) << 1);
    }

    /** The term cached for {@code id}, or null. */
    Node get(long id) {
        Table t = table;
        long number = id >>> PAGE_BITS;
        AtomicReferenceArray<Page> slots = t.slots;
        int first = t.bucket(number);
        for (int k = first; k < first + WAYS; k++) {
            // A page is found by its own number, which never changes: whatever a slot holds by the time it is read,
            // the terms that come back are those of the ids asked for.
            Page p = slots.getAcquire(k);
            if (p != null && p.number == number) {
                return (Node) TERMS.getAcquire(p.terms, (int) id & (PAGE - 1));
            }
        }
        return null;
    }

    /**
     * Caches {@code node} for {@code id}, if its weight is one the budget can
     * hold at all. An id that has a term keeps it: the store does not
     * change, so the two are the same term.
     */
    void put(long id, Node node) {
        int w = SimpleNodeTable.weightOf(node);
        if (w > maxWeight) {
            return;
        }
        long number = id >>> PAGE_BITS;
        Page page = table.find(number);
        if (page == null) {
            page = admit(number);
            if (page == null) {
                return; // the table is being replaced: this term is not cached this time
            }
        }
        if (!TERMS.compareAndSet(page.terms, (int) id & (PAGE - 1), null, node)) {
            return;
        }
        for (;;) {
            int held = page.get();
            if (held == GONE) {
                return; // the page left the table meanwhile, and what it weighed with it
            }
            if (page.compareAndSet(held, held + w)) {
                break;
            }
        }
        if (weight.addAndGet(w) > maxWeight) {
            sweep(page);
        }
    }

    /** The page of that number: the one another thread has just made, or a new one in a slot of its bucket. */
    private Page admit(long number) {
        Page page = new Page(number);
        for (int attempt = 0; attempt < 4; attempt++) {
            Table t = table;
            AtomicReferenceArray<Page> slots = t.slots;
            int first = t.bucket(number);
            int free = -1;
            boolean moving = false;
            for (int k = first; k < first + WAYS; k++) {
                Page p = slots.getAcquire(k);
                if (p == null) {
                    if (free < 0) {
                        free = k;
                    }
                } else if (p.number == number) {
                    return p;
                } else if (p == MOVED) {
                    moving = true;
                }
            }
            if (moving) {
                return null;
            }
            if (free >= 0) {
                if (slots.compareAndSet(free, null, page)) {
                    if (t.pages.incrementAndGet() > slots.length() / SPARE && slots.length() < maxSlots) {
                        grow(t);
                    }
                    return page;
                }
                continue; // another thread took the slot, perhaps for this very page: look again
            }
            // The bucket is full of other pages: they give way in turn.
            int at = first + (turn++ & (WAYS - 1));
            Page old = slots.getAcquire(at);
            if (old != null && old != MOVED && slots.compareAndSet(at, old, page)) {
                retire(old);
                return page;
            }
        }
        return null;
    }

    /** A page that has just been taken out of the table: its count is closed and its weight given back. */
    private void retire(Page page) {
        int held = page.getAndSet(GONE);
        if (held != GONE) {
            weight.addAndGet(-held);
        }
    }

    /** Twice the slots, the pages moved over; a page that finds its new bucket full is dropped. */
    private void grow(Table from) {
        synchronized (restructuring) {
            if (table != from) {
                return; // grown, or cleared, by another thread
            }
            int length = from.slots.length();
            Table to = new Table(Math.min(maxSlots, length * 2));
            int n = 0;
            for (int k = 0; k < length; k++) {
                // Emptied as it is read: a thread that would make a page here from now on sees that it cannot.
                Page p = from.slots.getAndSet(k, MOVED);
                if (p == null) {
                    continue;
                }
                int first = to.bucket(p.number);
                boolean placed = false;
                for (int i = first; i < first + WAYS && !placed; i++) {
                    if (to.slots.getPlain(i) == null) {
                        to.slots.setPlain(i, p);
                        placed = true;
                    }
                }
                if (placed) {
                    n++;
                } else {
                    retire(p);
                }
            }
            to.pages.set(n);
            table = to;
        }
    }

    /**
     * Over the budget: drops pages from a position that moves round the
     * table until a tenth of the budget is free - any page but {@code keep},
     * the one whose new term asked for the room. Only if that page alone is
     * more than the budget holds does it go too.
     */
    private void sweep(Page keep) {
        synchronized (restructuring) {
            Table t = table;
            AtomicReferenceArray<Page> slots = t.slots;
            int length = slots.length();
            long target = maxWeight - maxWeight / 10;
            for (int steps = 0; steps < length && weight.get() > target; steps++) {
                int k = sweepAt++ & (length - 1);
                Page p = slots.getAcquire(k);
                if (p != null && p != keep && slots.compareAndSet(k, p, null)) {
                    t.pages.decrementAndGet();
                    retire(p);
                }
            }
            if (weight.get() > maxWeight) {
                int first = t.bucket(keep.number);
                for (int k = first; k < first + WAYS; k++) {
                    if (slots.compareAndSet(k, keep, null)) {
                        t.pages.decrementAndGet();
                        retire(keep);
                        break;
                    }
                }
            }
        }
    }

    /** Drops everything, and the table with it. */
    void clear() {
        synchronized (restructuring) {
            Table old = table;
            for (int k = 0; k < old.slots.length(); k++) {
                Page p = old.slots.getAndSet(k, MOVED);
                if (p != null) {
                    retire(p);
                }
            }
            table = new Table(MIN_SLOTS);
        }
    }

    /** The terms held now, counted from the pages. */
    int entries() {
        Table t = table;
        int n = 0;
        for (int k = 0; k < t.slots.length(); k++) {
            Page p = t.slots.getAcquire(k);
            if (p != null) {
                for (int i = 0; i < PAGE; i++) {
                    if (TERMS.getAcquire(p.terms, i) != null) {
                        n++;
                    }
                }
            }
        }
        return n;
    }

    /** The weights of the terms held now, summed from the terms themselves. */
    long weightHeld() {
        Table t = table;
        long w = 0;
        for (int k = 0; k < t.slots.length(); k++) {
            Page p = t.slots.getAcquire(k);
            if (p != null) {
                for (int i = 0; i < PAGE; i++) {
                    Node node = (Node) TERMS.getAcquire(p.terms, i);
                    if (node != null) {
                        w += SimpleNodeTable.weightOf(node);
                    }
                }
            }
        }
        return w;
    }

    /** The weight the cache has counted: what {@link #weightHeld} finds, whenever nothing is being written. */
    long weightCounted() {
        return weight.get();
    }

    /** The pages held now. */
    int pages() {
        return table.pages.get();
    }

    /** The slots of the table now. */
    int slots() {
        return table.slots.length();
    }
}
