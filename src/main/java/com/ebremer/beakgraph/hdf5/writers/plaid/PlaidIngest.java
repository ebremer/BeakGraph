package com.ebremer.beakgraph.hdf5.writers.plaid;

import com.ebremer.beakgraph.core.Futures;
import com.ebremer.beakgraph.core.lib.RelativeIris;
import com.ebremer.beakgraph.core.fuseki.BGVoIDSD;
import com.ebremer.beakgraph.huge.HugeBuildPipeline;
import com.ebremer.beakgraph.huge.SpatialAugmenter;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.jena.riot.system.AsyncParserBuilder;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallel Pass A for the disk-based pipeline: source documents parse
 * CONCURRENTLY, each on its own worker. Everything expensive per quad - gzip
 * inflation, tokenization, default-graph rewrite, blank-node scoping,
 * relativization, numeric canonicalization, spatial/feature augmentation, and
 * the VoID statistics (thread-safe) - happens on the workers; only the
 * pipeline's batch sink (row assignment + sorter buffer appends) is serial.
 * Transforms are the EXACT shared implementations from
 * {@link HugeBuildPipeline} and {@link SpatialAugmenter}, so output is
 * identical to the sequential ingest's: rows interleave differently across
 * runs, but row numbers are internal - every sorted artifact and therefore
 * the finished store is the same.
 *
 * <p>Workers run on a DEDICATED fixed pool (not the sort pool): parse tasks
 * block - on the sink lock, on spatial futures, on spill backpressure - and
 * must never be able to starve the spill/merge workers they wait for.
 */
final class PlaidIngest implements HugeBuildPipeline.ParallelIngest {

    private static final Logger logger = LoggerFactory.getLogger(PlaidIngest.class);

    /** Rows accumulated per worker before one locked commit. */
    private static final int COMMIT_BATCH = 8192;

    private final int parseThreads;
    // One fully-materialized document at a time: JSON-LD has no streaming
    // parser, and -cores of them expanding concurrently would multiply the
    // (already unbounded) peak (BG-425).
    private final Semaphore materializedGate = new Semaphore(1);
    // Set on the first failed document: the other workers, CPU-bound inside
    // the parser where an interrupt is never checked, stop at their next quad.
    private volatile boolean aborted = false;

    PlaidIngest(int parseThreads) {
        this.parseThreads = Math.max(1, parseThreads);
    }

    @Override
    public void run(List<File> sources, File sourceRoot, boolean spatial, boolean features,
                    BGVoIDSD voidStats, BatchSink sink) throws IOException {
        int workers = Math.min(parseThreads, sources.size());
        logger.info("Plaid ingest: parsing {} document(s) on {} parse worker(s)", sources.size(), workers);
        ExecutorService parsePool = Executors.newFixedThreadPool(workers);
        List<Future<?>> tasks = new ArrayList<>(sources.size());
        try {
            for (int i = 0; i < sources.size(); i++) {
                final File input = sources.get(i);
                // Same per-document blank-node scoping rule as the sequential
                // ingest: ordinal prefix only when merging several documents.
                final String scope = sources.size() > 1 ? (i + "/") : null;
                final String parseBase = RelativeIris.parseBase(input, sources, sourceRoot);
                tasks.add(parsePool.submit(() -> {
                    parseDocument(input, scope, parseBase, spatial, features, voidStats, sink);
                    return null;
                }));
            }
            IOException failure = null;
            for (int i = 0; i < tasks.size(); i++) {
                try {
                    tasks.get(i).get();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    if (failure == null) failure = new IOException("Interrupted while parsing " + sources.get(i), ex);
                } catch (ExecutionException ex) {
                    Throwable c = ex.getCause();
                    if (failure == null) {
                        failure = (c instanceof IOException io) ? io
                                : (c instanceof UncheckedIOException uio) ? uio.getCause()
                                : new IOException("Failed to parse " + sources.get(i), c);
                    }
                }
                if (failure != null) {
                    aborted = true;
                    tasks.forEach(t -> t.cancel(true));
                    break;
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            // DRAIN before returning: a worker still inside sink.commit() or a
            // sorter's add() when the pipeline was closed and the workspace
            // deleted underneath it was the BG-110 race.
            parsePool.shutdownNow();
            boolean interrupted = false;
            for (int attempt = 0; attempt < 5; attempt++) {
                try {
                    if (parsePool.awaitTermination(1, TimeUnit.MINUTES)) {
                        break;
                    }
                    logger.warn("Parse workers still running {} minute(s) after the ingest ended", attempt + 1);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void parseDocument(File input, String scope, String parseBase, boolean spatial, boolean features,
                               BGVoIDSD voidStats, BatchSink sink) throws IOException {
        boolean gated = !RdfSources.isStreaming(RdfSources.langOf(input));
        if (gated) {
            materializedGate.acquireUninterruptibly();
        }
        try {
            parseDocumentNow(input, scope, parseBase, spatial, features, voidStats, sink);
        } finally {
            if (gated) {
                materializedGate.release();
            }
        }
    }

    private void parseDocumentNow(File input, String scope, String parseBase, boolean spatial, boolean features,
                                  BGVoIDSD voidStats, BatchSink sink) throws IOException {
        logger.info("Parsing {} (plaid, parallel disk-based build)", input);
        final long start = System.nanoTime();
        try (RdfSources.OpenedSource opened = RdfSources.open(input)) {
            RdfSources.warnIfMaterialized(input, opened.lang(), logger);   // JSON-LD: whole document in RAM (BG-425)
            AsyncParserBuilder parserBuilder = RdfSources.parser(opened, parseBase, input);
            SpatialAugmenter augmenter = new SpatialAugmenter(features);
            // Batch state: source quads and their count in this batch (derived
            // spatial quads ride along but are not counted as source quads).
            final ArrayList<Quad> batch = new ArrayList<>(COMMIT_BATCH);
            final long[] sourceInBatch = {0};
            final int maxInFlight = 16;
            final ArrayDeque<Future<ArrayList<Quad>>> inFlight = new ArrayDeque<>();
            // The quad stream closes before the executor: that aborts and joins
            // the parser thread when the loop throws (BG-100).
            try (ExecutorService scopeExec = Executors.newVirtualThreadPerTaskExecutor();
                 Stream<Quad> quads = parserBuilder.streamQuads()) {
                quads
                    .map(quad -> quad.isDefaultGraph()
                            ? new Quad(Quad.defaultGraphIRI, quad.getSubject(), quad.getPredicate(), quad.getObject())
                            : quad)
                    .map(quad -> HugeBuildPipeline.scopeBlankNodes(quad, scope))
                    .map(HugeBuildPipeline::relativize)
                    .map(HugeBuildPipeline::canonicalizeNumericObject)
                    .forEach(quad -> {
                        try {
                            if (aborted || Thread.currentThread().isInterrupted()) {
                                throw new UncheckedIOException(new IOException("Aborted: another source document failed"));
                            }
                            if (voidStats != null) {
                                voidStats.add(quad); // thread-safe; off the sink lock
                            }
                            batch.add(quad);
                            sourceInBatch[0]++;
                            if (spatial && SpatialAugmenter.isGeoLiteral(quad)) {
                                inFlight.add(scopeExec.submit(() -> augmenter.addSpatial(quad)));
                                while (inFlight.size() >= maxInFlight) {
                                    drainOne(inFlight, batch, input);
                                }
                            }
                            if (batch.size() >= COMMIT_BATCH) {
                                sink.commit(batch, sourceInBatch[0]);
                                batch.clear();
                                sourceInBatch[0] = 0;
                            }
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
                while (!inFlight.isEmpty()) {
                    drainOne(inFlight, batch, input);
                }
            } catch (UncheckedIOException ex) {
                throw new IOException("Failed while parsing/processing RDF source: " + input, ex.getCause());
            } catch (Exception ex) {
                throw new IOException("Failed while parsing/processing RDF source: " + input, ex);
            }
            if (!batch.isEmpty()) {
                sink.commit(batch, sourceInBatch[0]);
            }
        } catch (java.io.FileNotFoundException e) {
            throw new IOException("Source file not found: " + input, e);
        }
        logger.info("Parsed {} in {} ms", input.getName(), (System.nanoTime() - start) / 1_000_000L);
    }

    /** Collects one finished spatial task's derived quads into the current batch. */
    private static void drainOne(ArrayDeque<Future<ArrayList<Quad>>> inFlight,
                                 ArrayList<Quad> batch, File input) throws IOException {
        Future<ArrayList<Quad>> task = inFlight.poll();
        if (task == null) return;
        ArrayList<Quad> extraQuads = Futures.join(task, "collecting spatial results for " + input);
        for (Quad q : extraQuads) {
            batch.add(HugeBuildPipeline.canonicalizeNumericObject(q));
        }
    }
}
