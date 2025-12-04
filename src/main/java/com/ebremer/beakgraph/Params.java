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
    public static final String VERSION = "0.11.0";
    public static final int BLOCKSIZE = 64;
    public static final int SUPERBLOCKSIZE = 512;
    public static final String BGURN = "x-beakgraph";
    public static final String SPATIALSTRING = String.format("urn:%s:Spatial",BGURN);
    public static final Node SPATIAL = NodeFactory.createURI(SPATIALSTRING);
    public static final String VOIDSTRING = String.format("urn:%s:void", BGURN);
    public static final Node VOID = NodeFactory.createURI(VOIDSTRING);
    public static final short GRIDTILESIZE = 256;
    
    private Params() {}
}
