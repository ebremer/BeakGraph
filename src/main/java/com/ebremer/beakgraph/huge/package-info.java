/**
 * Disk-based construction of very large BeakGraphs.
 *
 * <p>The RAM writers ({@code com.ebremer.beakgraph.hdf5.writers}) hold every
 * quad, every dictionary node and every buffer in memory, which caps the size
 * of a buildable store at the heap. This package rebuilds the same pipeline on
 * disk:
 *
 * <ul>
 *   <li>{@link com.ebremer.beakgraph.huge.HugeHDF5Writer} - the public entry
 *       point (a {@code BeakGraphWriter}, drop-in alternative to
 *       {@code HDF5Writer});</li>
 *   <li>{@link com.ebremer.beakgraph.huge.HugeBuildPipeline} - parse once,
 *       spill (term, row) columns, external-sort, merge-dedup into
 *       dictionaries, sort-merge id joins, external-sort encoded quads for the
 *       GSPO/GPOS indexes;</li>
 *   <li>{@code Spill*} buffers - file-backed twins of the bit-packed /
 *       front-coded / primitive buffers, byte-identical on disk;</li>
 *   <li>{@link com.ebremer.beakgraph.huge.StreamingHdf5File} and friends - the
 *       backend seam for writing HDF5 datasets incrementally. The default
 *       backend is {@link com.ebremer.beakgraph.huge.NativeHdf5File}, the HDF
 *       Group's native library (https://github.com/HDFGroup/hdf5) via the
 *       official {@code hdf.hdf5lib} API - today the only implementation that
 *       supports chunk-at-a-time writing. Which jar supplies {@code hdf.hdf5lib}
 *       is a build-time choice: the JavaCPP preset with bundled natives by
 *       default, or the HDF Group's Java 25 FFM bindings with
 *       {@code -Dhdf5.ffm} (see the {@code hdf5-backend-*} profiles in
 *       pom.xml). When jHDF (https://github.com/jamesmudd/jhdf) gains
 *       chunked/streaming writes, a jHDF backend can be installed with
 *       {@link com.ebremer.beakgraph.huge.StreamingHdf5#setProvider} without
 *       touching writer code.</li>
 * </ul>
 *
 * <p>Files produced here are read by the existing jHDF-based readers
 * ({@code HDF5Reader}); datasets are CONTIGUOUS layout and attributes are
 * scalar 32/64-bit integers, exactly as those readers require.
 */
package com.ebremer.beakgraph.huge;
