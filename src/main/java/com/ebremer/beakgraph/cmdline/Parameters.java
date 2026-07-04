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
            description = "Use the disk-based writer (com.ebremer.beakgraph.huge): sorts and "
                        + "indexes on disk instead of RAM, for sources too large for the heap")
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
                        + "source document. Works with the default, -parallel, and -huge writers")
    public boolean merge = false;

    @Parameter(names = {"-parallel"}, converter = BooleanConverter.class,
            description = "Use the multi-threaded in-memory writer "
                        + "(com.ebremer.beakgraph.hdf5.writers.parallel): builds each file's "
                        + "dictionaries, columnar id lists, and GSPO/GPOS indexes concurrently "
                        + "on -cores threads. Ignored when -huge is set")
    public boolean parallel = false;

    @Parameter(names = "-cores", validateWith = PositiveInteger.class,
            description = "# of threads each -parallel conversion may use (with -threads N, "
                        + "N conversions run at once, each capped at -cores)")
    public int cores = 4;

    @Parameter(names = {"-version","-v"}, converter = BooleanConverter.class)
    public boolean version = false;

    @Parameter(names = {"-status"}, converter = BooleanConverter.class)
    public boolean status = false;    
    
    @Parameter(names = "-threads", description = "# of Threads")
    public int threads = 1;
}
