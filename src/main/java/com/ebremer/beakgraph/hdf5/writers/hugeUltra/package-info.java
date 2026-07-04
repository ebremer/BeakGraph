/**
 * The hugeUltra writer (CLI: {@code -method 4}): the disk-based
 * {@code com.ebremer.beakgraph.huge} build pipeline - bounded RAM at any quad
 * count - driven by parallel, primitive sorting machinery for
 * multi-billion-quad builds.
 *
 * <p>The pipeline itself ({@code HugeBuildPipeline}) is shared with
 * {@code -method 1}; this package supplies what gets injected into it:
 *
 * <ul>
 * <li>{@link com.ebremer.beakgraph.hdf5.writers.hugeUltra.PackedLongSorter} -
 *     external sorter over 1-2-word unsigned keys: parallel radix-sorted RAM
 *     runs written by background workers (double-buffered so ingestion never
 *     stalls), fixed-width binary spills, concurrent intermediate merges;</li>
 * <li>{@link com.ebremer.beakgraph.hdf5.writers.hugeUltra.PackedQuadSorter} /
 *     {@link com.ebremer.beakgraph.hdf5.writers.hugeUltra.PackedRowIdSorter} -
 *     the id-record sorters bit-packed onto it (no objects, no comparators,
 *     no codecs in the hot path);</li>
 * <li>{@link com.ebremer.beakgraph.hdf5.writers.hugeUltra.ParallelSpillSorter} -
 *     the term-column sorter: background spilling, concurrent merges, an
 *     order-preserving 8-byte prefix key that avoids most NodeComparator
 *     calls, and a grouped run format writing each distinct term's text once
 *     per run;</li>
 * <li>{@link com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer} -
 *     the public entry point ({@code BeakGraphWriter}); owns the worker pool
 *     and the atomic tmp-then-move publish.</li>
 * </ul>
 *
 * The independent pipeline stages (three column sorts, three dictionary
 * encodes, three id joins) run concurrently on the same pool, and GPOS sorts
 * only the deduplicated quads teed out of the GSPO scan. Output format and
 * readers are unchanged; like -method 1 the native HDF5 backend is required
 * and output is isomorphic (bnode labels are kept as parsed).
 */
package com.ebremer.beakgraph.hdf5.writers.hugeUltra;
