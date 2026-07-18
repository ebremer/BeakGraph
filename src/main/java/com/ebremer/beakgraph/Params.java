package com.ebremer.beakgraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
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
    //
    // v3: corrected the index rank/select directory (BB seeded so BB[k] =
    // ones-before-block-k) and widened the superblock entries to the bitmap length.
    // The query path now uses that directory for O(log n) select1, so files written
    // at v1/v2 (whose directory is incompatible) fall back to a linear select1 scan.
    //
    // v4: added the optional langDirs dataset beside langs/langTags (per-node
    // base direction for rdf:dirLangString literals; 0=none 1=ltr 2=rtl).
    // Absence means "no directions", so v3 files read unchanged; v4 files are
    // rejected by older builds via the gate above - without that, an old build
    // would silently reconstruct "x"@en--ltr as "x"@en (a different RDF term).
    public static final int FORMAT_VERSION = 4;
    // First format version whose rank/select directory is correct enough to drive
    // query navigation. Older files fall back to a linear select1 scan (slower but correct).
    public static final int RANK_DIRECTORY_MIN_VERSION = 3;
    // Sourced from the Maven build (beakgraph-version.properties is filtered with
    // ${project.version}), so the version lives in exactly one place: the POM.
    public static final String VERSION = loadVersion();
    public static final int BLOCKSIZE = 64;
    public static final int SUPERBLOCKSIZE = 512;
    public static final String BGURN = "x-beakgraph";
    public static final String SPATIALSTRING = String.format("urn:%s:Spatial",BGURN);
    public static final Node SPATIAL = NodeFactory.createURI(SPATIALSTRING);
    public static final String VOIDSTRING = String.format("urn:%s:void", BGURN);
    public static final Node BGVOID = NodeFactory.createURI(VOIDSTRING);
    public static final short GRIDTILESIZE = 512;
    public static final int COMPRESSION_THRESHOLD = 64;
    
    private static String loadVersion() {
        try (InputStream in = Params.class.getResourceAsStream("/beakgraph-version.properties")) {
            if (in != null) {
                Properties p = new Properties();
                p.load(in);
                String v = p.getProperty("version");
                if (v != null && !v.isBlank() && !v.startsWith("${")) {
                    return v;
                }
            }
        } catch (IOException ignore) {
            // fall through to the unknown sentinel
        }
        return "unknown";
    }

    private Params() {}
}
