package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.Test;

/**
 * The cache of terms by id ({@link IdNodeCache}): what is put is found, what
 * is found is the term of the id asked for and never another's, and what is
 * kept stays within the budget - by count, and by weight when the terms are
 * long literals - without ever refusing the terms of the present for those of
 * the past. Ids that lie together share a page, ids that do not have one
 * each, and either way the cache counts what it holds exactly.
 */
class IdNodeCacheTest {

    private static long id(long n) {
        return NodeId.pack(NodeType.SUBJECT, n);
    }

    private static Node term(long n) {
        return NodeFactory.createURI("http://ex.org/t" + n);
    }

    /** A literal of weight {@code 1 + chars / 256}; the id is in its text, so a term found can be held to the id asked for. */
    private static Node longLiteral(long n, int chars) {
        String head = "L" + n + ":";
        return NodeFactory.createLiteralString(head + "x".repeat(chars - head.length()));
    }

    @Test
    void whatIsPutIsFoundAndWhatIsNotIsNot() {
        IdNodeCache cache = new IdNodeCache(1_000_000);
        assertNull(cache.get(id(7)));
        Node seven = term(7);
        cache.put(id(7), seven);
        assertSame(seven, cache.get(id(7)));
        assertNull(cache.get(id(8)), "its neighbour on the page has no term yet");
        assertNull(cache.get(id(7 + IdNodeCache.PAGE)), "nor has the id in its place on the next page");
        assertNull(cache.get(NodeId.pack(NodeType.OBJECT, 7)), "the same number in another role is another id");
        cache.put(id(7), term(7));
        assertSame(seven, cache.get(id(7)), "an id that has its term keeps it");
        assertEquals(1, cache.entries(), "and there is one of it");
        assertEquals(1, cache.weightCounted());
        assertEquals(IdNodeCache.MIN_SLOTS, cache.slots(), "a cache asked little is a small table");
        cache.clear();
        assertNull(cache.get(id(7)));
        assertEquals(0, cache.entries());
        assertEquals(0, cache.weightCounted());
        assertEquals(0, cache.pages());
    }

    @Test
    void idsThatLieTogetherShareAPage() {
        IdNodeCache cache = new IdNodeCache(1_000_000);
        for (int i = IdNodeCache.PAGE; i < 2 * IdNodeCache.PAGE; i++) {
            cache.put(id(i), term(i));
        }
        assertEquals(1, cache.pages());
        assertEquals(IdNodeCache.PAGE, cache.entries());
        cache.put(id(2 * IdNodeCache.PAGE), term(2 * IdNodeCache.PAGE));
        assertEquals(2, cache.pages());
        cache.put(NodeId.pack(NodeType.OBJECT, IdNodeCache.PAGE), term(-1));
        assertEquals(3, cache.pages(), "the ids of another role are on pages of their own");
        for (int i = IdNodeCache.PAGE; i <= 2 * IdNodeCache.PAGE; i++) {
            assertEquals(term(i), cache.get(id(i)));
        }
        assertEquals(term(-1), cache.get(NodeId.pack(NodeType.OBJECT, IdNodeCache.PAGE)));
    }

    @Test
    void theTableGrowsWithWhatIsKept() {
        IdNodeCache cache = new IdNodeCache(1_000_000);
        int n = 100_000;
        for (int i = 1; i <= n; i++) {
            cache.put(id(i), term(i));
        }
        // 6,251 pages: grown until no more than an eighth of the slots hold one.
        assertEquals(1 << 16, cache.slots());
        int found = 0;
        for (int i = 1; i <= n; i++) {
            Node node = cache.get(id(i));
            if (node != null) {
                assertEquals(term(i), node, "the term of the id asked for");
                found++;
            }
        }
        // A page can find its bucket full of others while the table is an eighth in use; it is rare.
        assertTrue(found >= n * 999L / 1000, found + " of " + n + " terms are still there");
        assertEquals(found, cache.entries());
        assertEquals(found, cache.weightHeld(), "a term weighs 1");
        assertEquals(found, cache.weightCounted(), "and the cache has counted every one");
    }

    @Test
    void idsFarApartHaveAPageEachAndTheBudgetStillHolds() {
        IdNodeCache roomy = new IdNodeCache(1_000_000);
        int n = 50_000;
        for (int i = 1; i <= n; i++) {
            roomy.put(id(i * 1000L), term(i));
        }
        assertEquals(1 << 19, roomy.slots(), "a slot in eight for 50,000 pages");
        int found = 0;
        for (int i = 1; i <= n; i++) {
            Node node = roomy.get(id(i * 1000L));
            if (node != null) {
                assertEquals(term(i), node);
                found++;
            }
        }
        assertTrue(found >= n * 999L / 1000, found + " of " + n + " terms are still there");
        assertEquals(found, roomy.pages());
        assertEquals(found, roomy.weightCounted());

        IdNodeCache tight = new IdNodeCache(3000);
        for (int i = 1; i <= 200_000; i++) {
            tight.put(id(i * 1000L), term(i));
            if (i % 20_000 == 0) {
                assertTrue(tight.entries() <= 3000, tight.entries() + " entries under a budget of 3000");
                assertTrue(tight.pages() <= 3000, tight.pages() + " pages under a budget of 3000");
            }
        }
        assertEquals(32_768, tight.slots(), "the largest table the budget asks for: eight slots a term, to the next power of two");
        assertTrue(tight.entries() >= 2600, "the budget is used: " + tight.entries() + " entries kept");
        assertNotNull(tight.get(id(200_000 * 1000L)), "the term put last is there");
        assertEquals(tight.weightHeld(), tight.weightCounted());
    }

    @Test
    void noMoreIsKeptThanTheBudgetHasEntries() {
        IdNodeCache cache = new IdNodeCache(3000);
        for (int i = 1; i <= 200_000; i++) {
            cache.put(id(i), term(i));
            if (i % 20_000 == 0) {
                assertTrue(cache.entries() <= 3000, cache.entries() + " entries under a budget of 3000");
            }
        }
        assertEquals(2048, cache.slots(), "188 pages of sixteen hold the budget: a table of two thousand slots, not the thirty-two thousand it may have");
        assertTrue(cache.entries() >= 2700 - IdNodeCache.PAGE, "the budget is used: " + cache.entries() + " entries kept");
        assertNotNull(cache.get(id(200_000)), "the term put last is there");
        assertEquals(cache.weightHeld(), cache.weightCounted());
        // Every term found is its id's: a slot given to another page never answers for the one that had it.
        for (int i = 1; i <= 200_000; i++) {
            Node node = cache.get(id(i));
            if (node != null) {
                assertEquals(term(i), node);
            }
        }
    }

    @Test
    void longLiteralsAreKeptByWeightAndTheNewOnesFindRoom() {
        IdNodeCache cache = new IdNodeCache(4096);
        int chars = 2560; // weight 11
        assertEquals(11, SimpleNodeTable.weightOf(longLiteral(1, chars)));
        for (int i = 1; i <= 20_000; i++) {
            Node literal = longLiteral(i, chars);
            cache.put(id(i), literal);
            // The budget holds 372 of them; whatever was kept before, the one just read is cached.
            assertTrue(literal == cache.get(id(i)), "literal " + i + " was not kept");
            if (i % 500 == 0) {
                assertTrue(cache.weightHeld() <= 4096, "weight " + cache.weightHeld() + " under a budget of 4096 after " + i);
            }
        }
        assertTrue(cache.entries() >= 300, "the budget is used, not emptied: " + cache.entries() + " literals kept");
        assertEquals(cache.weightHeld(), cache.weightCounted());
    }

    @Test
    void aTermHeavierThanTheBudgetIsNotKeptAndABudgetOfNothingKeepsNothing() {
        IdNodeCache small = new IdNodeCache(8);
        small.put(id(1), longLiteral(1, 4096)); // weight 17
        assertNull(small.get(id(1)));
        small.put(id(2), term(2));
        assertEquals(term(2), small.get(id(2)));
        IdNodeCache none = new IdNodeCache(0);
        none.put(id(1), term(1));
        assertNull(none.get(id(1)));
        assertEquals(0, none.entries());
    }

    /** A budget that one page of terms is already over: the page goes when it is, and the bound holds term by term. */
    @Test
    void aBudgetSmallerThanAPageIsStillKept() {
        IdNodeCache cache = new IdNodeCache(8);
        for (int i = 1; i <= 1000; i++) {
            cache.put(id(i), term(i));
            assertTrue(cache.entries() <= 8, cache.entries() + " entries under a budget of 8 after " + i);
            assertEquals(cache.weightHeld(), cache.weightCounted());
        }
        IdNodeCache heavy = new IdNodeCache(100);
        for (int i = 1; i <= 1000; i++) {
            heavy.put(id(i), longLiteral(i, 2560)); // weight 11: nine of them are the budget, a page has room for sixteen
            assertTrue(heavy.weightHeld() <= 100, "weight " + heavy.weightHeld() + " under a budget of 100 after " + i);
        }
        assertEquals(heavy.weightHeld(), heavy.weightCounted());
    }

    /**
     * Readers and writers on every core at once, over more ids than the
     * budget holds, with long literals among them so that the table grows,
     * is swept and has its slots given away throughout: no term is ever
     * found under another's id, what is left stays within the budget, and
     * the cache's own count of it is what the pages hold.
     */
    @Test
    void manyThreadsNeverFindATermUnderAnothersId() throws Exception {
        for (int spread : new int[] {1, 37}) { // ids that share pages, and ids that seldom do
            int budget = 20_000;
            IdNodeCache cache = new IdNodeCache(budget);
            int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
            int ids = 60_000;
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int seed = t;
                Thread worker = new Thread(() -> {
                    Random random = new Random(seed);
                    try {
                        start.await();
                        for (int k = 0; k < 150_000 && failure.get() == null; k++) {
                            long n = 1 + random.nextInt(ids);
                            long at = id(n * spread);
                            Node node = cache.get(at);
                            if (node == null) {
                                cache.put(at, (n % 50 == 0) ? longLiteral(n, 1024) : term(n));
                            } else if (node.isURI()) {
                                assertEquals("http://ex.org/t" + n, node.getURI());
                            } else {
                                assertTrue(node.getLiteralLexicalForm().startsWith("L" + n + ":"), "id " + n + " answered " + node);
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
                worker.start();
                workers.add(worker);
            }
            start.countDown();
            for (Thread worker : workers) {
                worker.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertTrue(cache.slots() <= 262_144, cache.slots() + " slots for a budget of " + budget);
            assertTrue(cache.weightHeld() <= budget, "weight " + cache.weightHeld() + " under a budget of " + budget);
            assertTrue(cache.weightHeld() >= budget / 2, "the budget is used: weight " + cache.weightHeld());
            assertEquals(cache.weightHeld(), cache.weightCounted(), "what the cache has counted is what its pages hold");
        }
    }
}
