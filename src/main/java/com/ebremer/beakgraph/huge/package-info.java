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
 *       backend seam for writing HDF5 datasets incrementally. The backend is
 *       {@link com.ebremer.beakgraph.huge.FalconHdf5File}: Falcon
 *       (https://github.com/ebremer/falcon), the pure-Java HDF5 library of the
 *       readers and the in-memory writers, which puts each piece of a dataset
 *       in the file as it is given. No native HDF5 library is involved, so
 *       these writers run wherever Java does, the GraalVM native binary
 *       included.
 *       Another backend can be installed with
 *       {@link com.ebremer.beakgraph.huge.StreamingHdf5#setProvider} without
 *       touching writer code.</li>
 * </ul>
 *
 * <p>Files produced here are read by the same readers as every other store
 * ({@code HDF5Reader}); datasets are CONTIGUOUS layout and attributes are
 * scalar 32/64-bit integers, exactly as those readers require.
 */
package com.ebremer.beakgraph.huge;
