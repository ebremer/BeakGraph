/**
 * Multi-threaded construction of BeakGraphs from RDF files.
 *
 * <p>The sequential file-based writers ({@code com.ebremer.beakgraph.hdf5.writers})
 * build one store on one thread: three sub-dictionaries one after another, three
 * columnar id lists one after another, then the GSPO index, then the GPOS index.
 * This package is a drop-in twin of that pipeline that runs the independent
 * stages concurrently on a bounded worker pool (CLI: {@code -method 2}, sized
 * with {@code -cores}, default 4):
 *
 * <ul>
 *   <li>{@link com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer} -
 *       the public entry point (a {@code BeakGraphWriter}, drop-in alternative
 *       to {@code HDF5Writer}); owns the {@code ForkJoinPool} every stage runs
 *       in, so one conversion never uses more than the requested cores;</li>
 *   <li>{@link com.ebremer.beakgraph.hdf5.writers.parallel.ParallelPositionalDictionaryWriterBuilder} -
 *       inherits the whole ingest pipeline (parse, bnode alignment, spatial /
 *       feature augmentation, VoID statistics) from the sequential builder and
 *       only swaps which writer consumes the collected state;</li>
 *   <li>{@link com.ebremer.beakgraph.hdf5.writers.parallel.ParallelPositionalDictionaryWriter} -
 *       builds the entities / predicates / literals dictionaries concurrently,
 *       then populates the graphs / subjects / objects id lists concurrently
 *       (dictionary lookups fan out across the pool; the append-only bit-packed
 *       writes stay ordered);</li>
 *   <li>{@link com.ebremer.beakgraph.hdf5.writers.parallel.ParallelBGIndex} -
 *       resolves every quad's four dictionary ids once, in parallel, shares the
 *       tuples between both orderings, and builds GSPO and GPOS concurrently,
 *       sorting by id tuple (provably the same order the sequential writer
 *       obtains by comparing nodes: ids are NodeComparator ranks).</li>
 * </ul>
 *
 * <p>Only the build is parallel; the jHDF write of the finished buffers is the
 * sequential writer's, and files are read by the same unmodified readers
 * ({@code HDF5Reader}). Given the same parsed quads, the output is
 * byte-identical to the sequential writer's; across separate writes only the
 * VoID metadata's freshly minted blank-node labels differ, exactly as between
 * two sequential runs (see ParallelWriterParityTest).
 */
package com.ebremer.beakgraph.hdf5.writers.parallel;
