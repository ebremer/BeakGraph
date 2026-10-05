package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.Index;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * BG-213: when one stage of a concurrent stage group fails, the pipeline
 * reports it at once and cancels the siblings instead of waiting for their
 * multi-hour sorts. BG-124: after a failure every workspace file - the
 * materialized columns, the dictionaries, the predicate column - is closed
 * by close(), so the workspace can be removed (on Windows an open stream
 * makes its file undeletable). No native HDF5 needed: the failures are
 * injected before the file is assembled.
 */
class PipelineFailureTest {

    @TempDir
    Path dir;

    /**
     * A sorter that delegates, except that one operation (sorted() for the
     * term sorters, whose sort runs inside the column-sort stage group; add()
     * for the row-id sorters, fed inside the id-join stage group) fails or
     * blocks until interrupted.
     */
    private static final class Rigged<T> implements RecordSorter<T> {
        final RecordSorter<T> delegate;
        final boolean fail;
        final boolean onAdd;
        final CountDownLatch never = new CountDownLatch(1);
        final AtomicInteger interrupted;

        Rigged(RecordSorter<T> delegate, boolean fail, boolean onAdd, AtomicInteger interrupted) {
            this.delegate = delegate;
            this.fail = fail;
            this.onAdd = onAdd;
            this.interrupted = interrupted;
        }

        private void misbehave() throws IOException {
            if (fail) {
                throw new IOException("boom");
            }
            try {
                never.await();   // a "multi-hour sort": only an interrupt ends it
            } catch (InterruptedException e) {
                interrupted.incrementAndGet();
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
        }

        @Override
        public void add(T record) throws IOException {
            if (onAdd) {
                misbehave();
            }
            delegate.add(record);
        }

        @Override public long size() { return delegate.size(); }

        @Override
        public SortedCursor<T> sorted() throws IOException {
            if (!onAdd) {
                misbehave();
            }
            return delegate.sorted();
        }

        @Override public void close() { delegate.close(); }
    }

    private File source() throws IOException {
        File src = dir.resolve("src.nq").toFile();
        StringBuilder nq = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            nq.append("<http://ex.org/s").append(i % 20).append("> <http://ex.org/p").append(i % 3)
              .append("> \"v").append(i).append("\" .\n");
        }
        Files.writeString(src.toPath(), nq.toString(), StandardCharsets.UTF_8);
        return src;
    }

    /** The sequential provider with one rigged tag: {@code failing} throws, the {@code blocking} tags block until interrupted. */
    private static SorterProvider rigged(String failing, List<String> blocking, AtomicInteger interrupted) {
        SorterProvider base = SorterProvider.sequential(1 << 18, 1 << 21, 64);
        return new SorterProvider() {
            private <T> RecordSorter<T> wrap(String tag, RecordSorter<T> s) {
                boolean onAdd = tag.endsWith("id");   // row-id sorters are fed inside the id-join stages
                if (tag.equals(failing)) return new Rigged<>(s, true, onAdd, interrupted);
                if (blocking.contains(tag)) return new Rigged<>(s, false, onAdd, interrupted);
                return s;
            }

            @Override
            public RecordSorter<HugeRecords.TermRow> termSorter(Path workDir, String tag) {
                return wrap(tag, base.termSorter(workDir, tag));
            }

            @Override
            public RecordSorter<HugeRecords.RowId> rowIdSorter(Path workDir, String tag, long maxRow, long maxId) {
                return wrap(tag, base.rowIdSorter(workDir, tag, maxRow, maxId));
            }

            @Override
            public RecordSorter<HugeRecords.IdQuad> quadSorter(Path workDir, String tag, Index order,
                                                               long numEntities, long numPredicates, long numObjects) {
                return wrap(tag, base.quadSorter(workDir, tag, order, numEntities, numPredicates, numObjects));
            }
        };
    }

    private void runAndExpectBoom(String failing, List<String> blocking) throws Exception {
        File src = source();
        Path work = Files.createDirectories(dir.resolve("work-" + failing));
        AtomicInteger interrupted = new AtomicInteger();
        ForkJoinPool pool = new ForkJoinPool(4);
        long start = System.nanoTime();
        try (HugeBuildPipeline pipeline = new HugeBuildPipeline(List.of(src), false, false, VoidMode.NONE, work,
                rigged(failing, blocking, interrupted), pool)) {
            IOException ex = org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                    () -> pipeline.run(dir.resolve("out-" + failing + ".h5")));
            assertEquals("boom", ex.getMessage(), "the FIRST failure is the one reported");
        } finally {
            pool.shutdownNow();
        }
        long seconds = (System.nanoTime() - start) / 1_000_000_000L;
        assertTrue(seconds < 30, "the failure surfaced without waiting for the blocked siblings: " + seconds + " s");
        assertEquals(blocking.size(), interrupted.get(), "every sibling stage was interrupted and drained");
        // BG-124: every file the build created is closed - the workspace goes away.
        Workspaces.deleteTree(work, LoggerFactory.getLogger(PipelineFailureTest.class));
        assertTrue(Files.notExists(work), "the workspace is fully removable after the failure");
    }

    @Test
    void aFailedColumnSortCancelsItsSiblings() throws Exception {
        runAndExpectBoom("gcol", List.of("scol", "ocol"));
    }

    @Test
    void aFailedIdJoinCancelsItsSiblingsAndLeavesNoOpenFile() throws Exception {
        // By the id join the materialized columns, both dictionaries and the
        // predicate column (being rewritten by the fourth, concurrent stage)
        // exist in the workspace.
        runAndExpectBoom("gid", List.of("sid", "oid"));
    }
}
