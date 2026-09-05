package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.RecordSorter;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * {@link RecordSorter} for the (row, id) join records, packed as
 * {@code row:id} unsigned keys (row in the high bits) so key order is row
 * order and a {@link PackedLongSorter} does the sorting object-free.
 */
final class PackedRowIdSorter implements RecordSorter<RowId> {

    private final PackedLongSorter sorter;
    private final int idBits;
    private final boolean twoWords;

    PackedRowIdSorter(Path workDir, String tag, long maxRow, long maxId,
                      int batch, int fanIn, ForkJoinPool pool, ExecutorService exec) {
        this(workDir, tag, maxRow, maxId, batch, fanIn, pool, exec,
                UltraSorterProvider.defaultMergeConcurrency(fanIn, pool.getParallelism()));
    }

    PackedRowIdSorter(Path workDir, String tag, long maxRow, long maxId,
                      int batch, int fanIn, ForkJoinPool pool, ExecutorService exec, int maxConcurrentMerges) {
        int rowBits = MinBits(Math.max(1, maxRow));
        this.idBits = MinBits(Math.max(1, maxId));
        int totalBits = rowBits + idBits;
        this.twoWords = totalBits > 63;
        this.sorter = new PackedLongSorter(workDir, tag, totalBits, batch, fanIn, pool, exec, maxConcurrentMerges);
    }

    @Override
    public void add(RowId r) throws IOException {
        long lo = (r.row() << idBits) | r.id();
        long hi = twoWords ? (r.row() >>> (64 - idBits)) : 0;
        sorter.add(hi, lo);
    }

    @Override
    public long size() {
        return sorter.size();
    }

    @Override
    public SortedCursor<RowId> sorted() throws IOException {
        final PackedLongSorter.KeyCursor keys = sorter.sorted();
        final long idMask = (1L << idBits) - 1;
        return new SortedCursor<>() {
            private RowId head = fetch();

            private RowId fetch() {
                try {
                    if (!keys.advance()) {
                        keys.close();
                        return null;
                    }
                } catch (IOException e) {
                    keys.close();
                    throw new UncheckedIOException("Sorted row-id stream failed", e);
                }
                long lo = keys.lo();
                long id = lo & idMask;
                long row = twoWords ? ((keys.hi() << (64 - idBits)) | (lo >>> idBits)) : (lo >>> idBits);
                return new RowId(row, id);
            }

            @Override
            public boolean hasNext() {
                return head != null;
            }

            @Override
            public RowId next() {
                if (head == null) throw new NoSuchElementException();
                RowId r = head;
                head = fetch();
                return r;
            }

            @Override
            public void close() {
                keys.close();
            }
        };
    }

    @Override
    public void close() {
        sorter.close();
    }
}
