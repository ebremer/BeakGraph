package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.BooleanConverter;
import com.beust.jcommander.validators.PositiveInteger;
import java.io.File;

/**
 *
 * @author erich
 */
public class Parameters {
    
    @Parameter(names = "-help", converter = BooleanConverter.class, help = true,
            description = "Print this usage text and exit")
    public boolean help = false;

    @Parameter(names = "-endpoint",
            description = "Serve instead of converting: a single .h5 file as a SPARQL endpoint at "
                        + "/rdf, or a directory served in full as W3C LWS storage (every .h5 in it "
                        + "answers SPARQL at its own URL; the metadata model is served at /rdf and "
                        + "written into the directory as beakgraph.ttl.gz on first start)",
            required = false)
    public File sparqlendpoint = null;
    
    @Parameter(names = "-port", description = "Set HTTP port when endpoint started", required = false)
    public int port = 8888;

    @Parameter(names = "-base",
            description = "Public base URL clients use to reach -endpoint, e.g. https://data.example.org/. "
                        + "Only needed behind a reverse proxy that does not send Forwarded/X-Forwarded-* "
                        + "headers: by default every response derives its links and IRIs from the request "
                        + "it answers. With -export: the base the store's document-relative IRIs "
                        + "(<>, <sib.png>) are resolved against; required for NT/NQ output of such a store",
            required = false)
    public String base = null;

    @Parameter(names = "-timeout",
            description = "Per-query wall-clock limit in seconds for -endpoint (0 = unlimited). "
                        + "A query over the limit is cancelled and answered with HTTP 503",
            required = false)
    public long timeout = 30;
    
    @Parameter(names = "-src", description = "Source Folder or File", required = false)
    public File src = null;

    @Parameter(names = "-dest", description = "Destination Folder or File", required = false)
    public File dest = null;
    
    @Parameter(names = {"-void"}, converter = BooleanConverter.class,
            description =
                """
                Generate the VoID/SD statistics graph (urn:x-beakgraph:void) using
                EXACT in-memory counting (RAM grows with distinct terms per graph).
                Without -void or -voidsketch, no statistics graph is written and
                readers fall back to a fixed join-reorder heuristic.
                Mutually exclusive with -voidsketch
                """)
    public boolean voidExact = false;

    @Parameter(names = {"-voidsketch"}, converter = BooleanConverter.class,
            description = "Generate the VoID/SD statistics graph with BOUNDED memory: exact up "
                        + "to 65536 distinct nodes per counter, then HyperLogLog estimates "
                        + "(~0.8% error, deterministic). Recommended for the disk writers "
                        + "(-method 1/4/5). Mutually exclusive with -void")
    public boolean voidSketch = false;

    @Parameter(names = {"-spatial"}, converter = BooleanConverter.class,
            description = "Build the Hilbert-curve spatial index for geo:wktLiteral geometry "
                        + "(adds the urn:x-beakgraph:Spatial graph and grid-tile graphs)")
    public boolean spatial = false;

    @Parameter(names = {"-features"}, converter = BooleanConverter.class,
            description = "With -spatial: also derive 2-D shape features (area, axes, ...) per geometry")
    public boolean features = false;

    @Parameter(names = {"-huge"}, converter = BooleanConverter.class,
            description = "Shorthand for \"-method 1\": the disk-based writer "
                        + "(com.ebremer.beakgraph.huge). An explicit -method takes precedence")
    public boolean huge = false;

    @Parameter(names = "-workdir",
            description = "Workspace directory for the disk-based writers' (-method 1/4/5, -huge) "
                        + "spill files; needs free space on the order of a few times the "
                        + "uncompressed source (default: each destination file's directory)",
            required = false)
    public File workdir = null;
    
    @Parameter(names = {"-merge"}, converter = BooleanConverter.class,
            description = "Merge ALL RDF sources found under -src (typically a directory tree) "
                        + "into ONE BeakGraph HDF5 file at -dest instead of one .h5 per source "
                        + "(if -dest is an existing directory, writes <dest>/merged.h5; an "
                        + "existing destination file is rebuilt). Blank nodes stay distinct per "
                        + "source document. Works with every -method")
    public boolean merge = false;

    @Parameter(names = "-method", validateWith = MethodValidator.class,
            description = "Conversion engine: 0 = sequential in-memory writer (default), "
                        + "1 = disk-based writer for sources too large for the heap "
                        + "(com.ebremer.beakgraph.huge; needs the native HDF5 backend, see -workdir), "
                        + "2 = multi-threaded in-memory writer "
                        + "(com.ebremer.beakgraph.hdf5.writers.parallel) on -cores threads, "
                        + "3 = ultra in-memory writer (com.ebremer.beakgraph.hdf5.writers.ultra): "
                        + "parallel parsing, radix-sorted packed-key indexes, and parallel index "
                        + "emission on -cores threads, "
                        + "4 = hugeUltra parallel DISK-based writer "
                        + "(com.ebremer.beakgraph.hdf5.writers.hugeUltra) for multi-billion-quad "
                        + "builds: bounded RAM like -method 1, but with radix-sorted bit-packed "
                        + "spill runs, background spilling, and concurrent pipeline stages on "
                        + "-cores threads (needs the native HDF5 backend; honors -workdir), "
                        + "5 = plaid (com.ebremer.beakgraph.hdf5.writers.plaid): method 4 plus "
                        + "PARALLEL MULTI-FILE INGEST - up to -cores source documents parse "
                        + "concurrently; the fastest option for -merge over many files")
    public int method = 0;

    @Parameter(names = "-cores", validateWith = PositiveInteger.class,
            description = "# of threads each -method 2/3/4/5 conversion may use (with "
                        + "-threads N, N conversions run at once, each capped at -cores)")
    public int cores = 4;

    @Parameter(names = "-export", validateWith = ExportFormatValidator.class,
            description = "Dump the BeakGraph(s) at -src back to RDF instead of converting: "
                        + "NT, NQ, JSON-LD, TTL, or TRIG. Output lands next to each .h5 with the "
                        + "same name and the format's extension. If TTL or NT is chosen but the "
                        + "store holds named graphs beyond the default graph, the format is "
                        + "upgraded to its quad form (TTL->TRIG, NT->NQ). BeakGraph-internal "
                        + "metadata graphs (VoID/spatial index) are not exported")
    public String export = null;

    @Parameter(names = {"-compress"}, converter = BooleanConverter.class,
            description = "gzip the -export output (adds .gz to the file name)")
    public boolean compress = false;

    @Parameter(names = "-verify",
            description = "Verify BeakGraph file integrity instead of converting: opens each "
                        + "file, loads the dictionaries and every index structure, and "
                        + "enumerates its graphs - catching truncated, partially copied, or "
                        + "otherwise damaged files before they are served. The path may be one "
                        + "file or a directory (scanned recursively for *.h5/*.hdf5). Prints one "
                        + "verdict line per file plus a summary; exit code 2 if any file is "
                        + "damaged. Add -deep for a data-level pass")
    public File verify = null;

    @Parameter(names = {"-deep"}, converter = BooleanConverter.class,
            description = "With -verify: additionally materialize every triple of every graph "
                        + "(resolving all terms through the dictionaries) and reconcile the "
                        + "totals against the index-derived counts. Reads through the bulk of "
                        + "each file - slower, but catches data-region corruption that the "
                        + "structural checks pass over")
    public boolean deep = false;

    @Parameter(names = {"-version","-v"}, converter = BooleanConverter.class,
            description = "Print the version and exit")
    public boolean version = false;

    @Parameter(names = {"-status"}, converter = BooleanConverter.class,
            description = "Show a progress bar (per-file mode) and end-of-run counters")
    public boolean status = false;

    @Parameter(names = "-threads", validateWith = PositiveInteger.class,
            description = "Number of per-file conversions run concurrently (each gets its own "
                        + "-cores budget, so total CPU is about threads x cores)")
    public int threads = 1;
}
