/**
 * The plaid writer (CLI: {@code -method 5}): the hugeUltra disk-based build
 * (bounded RAM, packed-key radix sorting, background spills, concurrent
 * stages) with the last big serial phase removed - PARALLEL MULTI-FILE
 * INGEST.
 *
 * <p>{@link com.ebremer.beakgraph.hdf5.writers.plaid.PlaidIngest} parses up
 * to {@code -cores} source documents concurrently on a dedicated worker pool
 * (kept separate from the sort pool so blocking parse work can never starve
 * spills). Each worker runs the full per-quad transform chain - the shared
 * static implementations from {@code HugeBuildPipeline} plus
 * {@code SpatialAugmenter} and the (thread-safe) VoID statistics - and hands
 * finished rows to the pipeline in batches. The pipeline's batch sink is the
 * one serial section: it assigns row numbers and appends to the sorter
 * buffers under a lock, which preserves every single-threaded invariant of
 * the sequential ingest (most importantly the positional predicate column,
 * whose entry i must BE row i). Spill sorting/writing still happens on
 * background workers, so the lock covers only buffer appends.
 *
 * <p>Rows therefore interleave nondeterministically across runs - and it
 * does not matter: row numbers are internal, every artifact the store keeps
 * is sorted, and the output is identical to a method-1/4 build of the same
 * sources. Best suited to merges of many files; a single-file build degrades
 * gracefully to one parse worker (method-4 behaviour).
 */
package com.ebremer.beakgraph.hdf5.writers.plaid;
