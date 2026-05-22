package com.ebremer.beakgraph;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 *
 * @author Erich Bremer
 */
public class Params {
    public static final String BG = ".BG";
    public static final String DICTIONARY = "dictionary";
    // HDF5 on-disk format version, written as the "formatVersion" attribute on
    // the .BG group. Bump on any change older readers cannot understand;
    // HDF5Reader rejects files whose version exceeds this. Files written before
    // versioning have no attribute and are treated as version 1.
    public static final int FORMAT_VERSION = 2;
    public static final String VERSION = "0.15.0";
    public static final int BLOCKSIZE = 64;
    public static final int SUPERBLOCKSIZE = 512;
    public static final String BGURN = "x-beakgraph";
    public static final String SPATIALSTRING = String.format("urn:%s:Spatial",BGURN);
    public static final Node SPATIAL = NodeFactory.createURI(SPATIALSTRING);
    public static final String VOIDSTRING = String.format("urn:%s:void", BGURN);
    public static final Node BGVOID = NodeFactory.createURI(VOIDSTRING);
    public static final short GRIDTILESIZE = 512;
    public static final int COMPRESSION_THRESHOLD = 64;
    
    private Params() {}
}
