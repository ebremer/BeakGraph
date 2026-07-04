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
    
    @Parameter(names = "-help", converter = BooleanConverter.class, help = true)
    public boolean help = false;

    @Parameter(names = "-endpoint", description = "Start SPARQL Endpoint for -endpoint", required = false)
    public File sparqlendpoint = null;
    
    @Parameter(names = "-port", description = "Set HTTP port when endpoint started", required = false)
    public int port = 8888;
    
    @Parameter(names = "-src", description = "Source Folder or File", required = false)
    public File src = null;

    @Parameter(names = "-dest", description = "Destination Folder or File", required = false)
    public File dest = null;
    
    @Parameter(names = {"-spatial"}, converter = BooleanConverter.class)
    public boolean spatial = false;

    @Parameter(names = {"-features"}, converter = BooleanConverter.class)
    public boolean features = false;

    @Parameter(names = {"-huge"}, converter = BooleanConverter.class,
            description = "Shorthand for \"-method 1\": the disk-based writer "
                        + "(com.ebremer.beakgraph.huge). An explicit -method takes precedence")
    public boolean huge = false;

    @Parameter(names = "-workdir",
            description = "Workspace directory for -huge spill files; needs free space on the "
                        + "order of a few times the uncompressed source (default: each "
                        + "destination file's directory)", required = false)
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
                        + "-cores threads (needs the native HDF5 backend; honors -workdir)")
    public int method = 0;

    @Parameter(names = "-cores", validateWith = PositiveInteger.class,
            description = "# of threads each -method 2 or -method 3 conversion may use (with "
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

    @Parameter(names = {"-version","-v"}, converter = BooleanConverter.class)
    public boolean version = false;

    @Parameter(names = {"-status"}, converter = BooleanConverter.class)
    public boolean status = false;    
    
    @Parameter(names = "-threads", description = "# of Threads")
    public int threads = 1;
}
