package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.huge.HugeRecords.IdQuad;
import com.ebremer.beakgraph.huge.RecordSorter;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * {@link RecordSorter} for dictionary-encoded quads that never sorts an
 * object: each quad's four ids concatenate - in the target index's component
 * order, first component highest - into one unsigned key of at most 126 bits.
 * Unsigned key order IS the index's comparator order (ids are NodeComparator
 * ranks), so a {@link PackedLongSorter} does all the work and quads
 * materialize back into {@link IdQuad} records only at the consuming scan.
 */
final class PackedQuadSorter implements RecordSorter<IdQuad> {

    private final PackedLongSorter sorter;
    private final boolean gspo;
    private final int b0, b1, b2, b3; // component widths in sort order (b0 needs no mask, only the guard)

    PackedQuadSorter(Path workDir, String tag, Index order,
                     long numEntities, long numPredicates, long numObjects,
                     int batch, int fanIn, ForkJoinPool pool, ExecutorService exec) {
        this(workDir, tag, order, numEntities, numPredicates, numObjects, batch, fanIn, pool, exec,
                UltraSorterProvider.defaultMergeConcurrency(fanIn, pool.getParallelism()));
    }

    PackedQuadSorter(Path workDir, String tag, Index order,
                     long numEntities, long numPredicates, long numObjects,
                     int batch, int fanIn, ForkJoinPool pool, ExecutorService exec, int maxConcurrentMerges) {
        this.gspo = order == Index.GSPO;
        int bG = MinBits(Math.max(1, numEntities));
        int bS = bG;
        int bP = MinBits(Math.max(1, numPredicates));
        int bO = MinBits(Math.max(1, numObjects));
        this.b0 = bG;
        this.b1 = gspo ? bS : bP;
        this.b2 = gspo ? bP : bO;
        this.b3 = gspo ? bO : bS;
        int totalBits = b0 + b1 + b2 + b3;
        this.sorter = new PackedLongSorter(workDir, tag, totalBits, batch, fanIn, pool, exec, maxConcurrentMerges);
    }

    @Override
    public void add(IdQuad q) throws IOException {
        long v0 = q.g();
        long v1 = gspo ? q.s() : q.p();
        long v2 = gspo ? q.p() : q.o();
        long v3 = gspo ? q.o() : q.s();
        // The provider's counts are the caller's promise that every id fits
        // its width; an id past it used to shift into its neighbour's bits and
        // silently corrupt both the order and the decoded ids (BG-139).
        if ((v0 >>> b0) != 0 || (v1 >>> b1) != 0 || (v2 >>> b2) != 0 || (v3 >>> b3) != 0) {
            throw new IllegalStateException("Quad component exceeds its declared width in sorter (widths "
                    + b0 + "/" + b1 + "/" + b2 + "/" + b3 + " bits, " + (gspo ? "g/s/p/o" : "g/p/o/s") + "): " + q);
        }
        // Running 128-bit (shift-left, or-in) append; component widths are far
        // below 64, so the shifts are always legal.
        long lo = v0;
        long hi = 0;
        hi = (hi << b1) | (lo >>> (64 - b1)); lo = (lo << b1) | v1;
        hi = (hi << b2) | (lo >>> (64 - b2)); lo = (lo << b2) | v2;
        hi = (hi << b3) | (lo >>> (64 - b3)); lo = (lo << b3) | v3;
        sorter.add(hi, lo);
    }

    @Override
    public long size() {
        return sorter.size();
    }

    @Override
    public SortedCursor<IdQuad> sorted() throws IOException {
        final PackedLongSorter.KeyCursor keys = sorter.sorted();
        return new SortedCursor<>() {
            private IdQuad head = fetch();

            private IdQuad fetch() {
                try {
                    if (!keys.advance()) {
                        keys.close();
                        return null;
                    }
                } catch (IOException e) {
                    keys.close();
                    throw new UncheckedIOException("Sorted quad stream failed", e);
                }
                long hi = keys.hi();
                long lo = keys.lo();
                long v3 = extract(hi, lo, 0, b3);
                long v2 = extract(hi, lo, b3, b2);
                long v1 = extract(hi, lo, b2 + b3, b1);
                long v0 = extract(hi, lo, b1 + b2 + b3, 63);
                return gspo
                        ? new IdQuad(v0, v1, v2, v3)
                        : new IdQuad(v0, v3, v1, v2); // (g,p,o,s) back to record order
            }

            @Override
            public boolean hasNext() {
                return head != null;
            }

            @Override
            public IdQuad next() {
                if (head == null) throw new NoSuchElementException();
                IdQuad q = head;
                head = fetch();
                return q;
            }

            @Override
            public void close() {
                keys.close();
            }
        };
    }

    static long extract(long hi, long lo, int off, int bits) {
        long mask = (1L << bits) - 1;
        if (off >= 64) return (hi >>> (off - 64)) & mask;
        if (off + bits <= 64) return (lo >>> off) & mask;
        return ((lo >>> off) | (hi << (64 - off))) & mask;
    }

    @Override
    public void close() {
        sorter.close();
    }
}
