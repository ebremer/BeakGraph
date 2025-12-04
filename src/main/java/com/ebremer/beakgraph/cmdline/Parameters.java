package com.ebremer.beakgraph.cmdline;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.BooleanConverter;
import java.io.File;

/**
 *
 * @author erich
 */
public class Parameters {
    
    @Parameter(names = "-help", converter = BooleanConverter.class, help = true)
    public boolean help = false;

    @Parameter(names = "-endpoint", description = "Start SAPRQL Endpoint for -endpoint", required = false)
    public File sparqlendpoint = null;
    
    @Parameter(names = "-port", description = "Set HTTP port when endpoint started", required = false)
    public int port = 8888;
    
    @Parameter(names = "-src", description = "Source Folder or File", required = false)
    public File src = null;

    @Parameter(names = "-dest", description = "Destination Folder or File", required = false)
    public File dest = null;
    
    @Parameter(names = {"-spatial"}, converter = BooleanConverter.class)
    public boolean spatial = false;
    
    @Parameter(names = {"-version","-v"}, converter = BooleanConverter.class)
    public boolean version = false;

    @Parameter(names = {"-status"}, converter = BooleanConverter.class)
    public boolean status = false;    
    
    @Parameter(names = "-threads", description = "# of Threads")
    public int threads = 1;
    
}
