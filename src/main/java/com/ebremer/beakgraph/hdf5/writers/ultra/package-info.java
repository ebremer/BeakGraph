/**
 * The "ultra" in-memory BeakGraph writer (CLI: {@code -method 3}, thread
 * count via {@code -cores}): the same HDF5 output format as
 * {@link com.ebremer.beakgraph.hdf5.writers.HDF5Writer}, built as a fully
 * parallel dependency DAG instead of a phase pipeline.
 *
 * <p>What it parallelizes beyond the {@code -method 2} writer
 * ({@code com.ebremer.beakgraph.hdf5.writers.parallel}):
 *
 * <ul>
 * <li><b>Parsing</b> - source documents parse concurrently (bnode alignment is
 *     per-document state by RDF semantics), and dictionary-set deduplication
 *     plus statistics run as parallel passes over the collected quads
 *     ({@link com.ebremer.beakgraph.hdf5.writers.ultra.UltraIngest}).</li>
 * <li><b>Id resolution</b> - dictionary ids are 1-based NodeComparator ranks,
 *     so one shared sort per sub-dictionary feeds the storage encoder AND an
 *     O(1) node-to-id hash map; no binary search ever runs
 *     ({@link com.ebremer.beakgraph.hdf5.writers.ultra.UltraDictionary}).</li>
 * <li><b>Index sorting</b> - each quad's four ids pack into one primitive key
 *     per ordering (one or two words), sorted by a parallel LSD radix sort or
 *     the JDK primitive sort; duplicates collapse once and GPOS reuses the
 *     deduplicated GSPO set
 *     ({@link com.ebremer.beakgraph.hdf5.writers.ultra.ParallelRadixSort},
 *     {@link com.ebremer.beakgraph.hdf5.writers.ultra.UltraBGIndex}).</li>
 * <li><b>Index emission</b> - the level-emission scan becomes two parallel
 *     passes (count, prefix-sum, positional write) over byte-aligned buffers
 *     and an atomically-OR-merged bitmap; the SB/BB rank directories fall out
 *     of word popcount prefix sums.</li>
 * </ul>
 *
 * The sort/dedup/statistics/dictionary/index stages run on one dedicated
 * {@code ForkJoinPool} of {@code -cores} threads, so with {@code -threads N}
 * in the CLI, N conversions run at once, each with its own core budget for
 * those stages. Two things run OUTSIDE that pool: each document's parse adds
 * one Jena AsyncParser producer thread, and with {@code -spatial} the
 * per-geometry augmentation runs on virtual threads scheduled by the JDK, not
 * bounded by {@code -cores} (BG-120). Given the same parsed quads the emitted
 * buffers are byte-identical to the sequential writer's; whole files differ
 * only through VoID's per-write random blank-node labels (single source) or
 * per-document blank-node scoping (merged sources).
 */
package com.ebremer.beakgraph.hdf5.writers.ultra;
