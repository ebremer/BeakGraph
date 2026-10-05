package com.ebremer.beakgraph.core.lib;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * {@link NodeComparator} with a PRIVATE NodeValue memo, replacing Jena's one
 * global bounded cache in the literal-comparison path. Two reasons, both
 * discovered on a 1.4B-quad PubMed merge:
 *
 * <ul>
 * <li><b>Contention:</b> {@code NodeValue.makeNode} funnels every thread of a
 *     parallel term-run sort through the same Caffeine cache, whose eviction
 *     lock times out under load ("excessive wait times" warnings) and stalls
 *     ingestion behind the spill it is backpressured on.</li>
 * <li><b>Work:</b> a sort converts each literal on every comparison -
 *     O(n log n) makeNode calls; the memo converts each distinct node once
 *     per cap window.</li>
 * </ul>
 *
 * Ordering is EXACTLY the parent's: the override only changes where the
 * Node-to-NodeValue conversion result comes from. One instance per sorter or
 * per sort; the map is cleared when it exceeds {@code maxEntries} (a sorter
 * outlives many runs, so an uncapped memo would grow with the whole build's
 * term population).
 *
 * <p>Originally wired into the hugeUltra/plaid term sorters only; every
 * engine's literal sorts now run on one - {@link NodeSorter} (methods 0/2/3
 * dictionaries and the method 0 quad sorts) and the sequential
 * {@code SorterProvider} (method 1 term runs) - so no build sorts literals
 * through the shared global cache any more (BG-249).
 */
public final class CachingNodeComparator extends NodeComparator {

    private final int maxEntries;
    private final ConcurrentHashMap<Node, NodeValue> memo;

    public CachingNodeComparator(int maxEntries) {
        this.maxEntries = maxEntries;
        this.memo = new ConcurrentHashMap<>(1 << 16);
    }

    @Override
    protected NodeValue nodeValue(Node n) {
        NodeValue v = memo.get(n);
        if (v != null) {
            return v;
        }
        v = NodeValue.makeNode(n);
        if (memo.size() >= maxEntries) {
            // Crude but contention-free pressure valve: a full reset costs one
            // re-conversion per live node, an eviction policy would cost
            // bookkeeping on EVERY hit. Sorted runs re-touch a node many times
            // in a short window, so recency hardly matters here.
            memo.clear();
        }
        memo.put(n, v);
        return v;
    }
}
