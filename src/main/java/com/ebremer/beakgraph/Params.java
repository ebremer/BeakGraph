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
    // The same bump changed the dictionary ORDER of composite (cdt:List /
    // cdt:Map) literals from compareAlways value order to exact lexical order
    // (NodeComparator). Ids are comparator ranks, so a pre-v4 store holding
    // composite literals is laid out in an order this build's binary search
    // does not follow: lookups miss stored terms. HDF5Reader refuses such
    // files (CDT_LEXICAL_ORDER_MIN_VERSION); pre-v4 stores without composite
    // literals are unaffected and still open.
    //
    // v5: RDF 1.2 triple terms. Adds DataType.TRIPLE_TERM (ordinal 13) and the
    // optional tripleTerms component store in the literals section; triple
    // terms occupy that section's contiguous suffix of the object id space.
    // Absence means "no triple terms", so v3/v4 files read unchanged; the gate
    // converts an old build's would-be "Corrupt HDF5: Unknown DataType ordinal
    // 13" into the intended "Upgrade BeakGraph".
    public static final int FORMAT_VERSION = 5;
    // First format version whose dictionary orders composite (cdt) literals the
    // way the current NodeComparator does. Older files containing them are
    // refused at open (see the v4 note above).
    public static final int CDT_LEXICAL_ORDER_MIN_VERSION = 4;
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
    /** Every BeakGraph-internal graph name starts with this: the VoID statistics, the spatial index, the grid tiles. */
    public static final String INTERNAL_GRAPH_PREFIX = "urn:" + BGURN + ":";

    /**
     * Whether {@code g} names one of BeakGraph's own metadata graphs (VoID,
     * Spatial, or a grid tile {@code urn:x-beakgraph:grid:*}) rather than a
     * graph the user put in the store. Export and its "has named graphs"
     * decision used to exclude only VoID and Spatial, so a -spatial store's
     * thousands of tile graphs were dumped as user data and forced NT/TTL up
     * to NQ/TRIG.
     */
    public static boolean isInternalGraph(Node g) {
        return g != null && g.isURI() && g.getURI().startsWith(INTERNAL_GRAPH_PREFIX);
    }

    /**
     * Grid-tile graph IRI {@code urn:x-beakgraph:grid:{level}:{x}:{y}}. Built by
     * concatenation, never {@code String.format("%d")}: the Formatter localizes
     * digits under the JVM's FORMAT locale, and on ar/fa/bn-style installs
     * minted stored graph names with Arabic-Indic digits - a file that differed
     * by build machine and no longer matched the documented form.
     */
    public static Node gridGraph(int level, long x, long y) {
        return NodeFactory.createURI("urn:" + BGURN + ":grid:" + level + ':' + x + ':' + y);
    }

    /** Blank-node label {@code b} + id zero-padded to 20 ASCII digits (the documented {@code b%020d}). */
    public static String blankNodeLabel(long id) {
        return blankNodeLabel("b", id);
    }

    /** {@code prefix} + id zero-padded to 20 ASCII digits; locale-independent (see {@link #gridGraph}). */
    public static String blankNodeLabel(String prefix, long id) {
        String digits = Long.toString(id);
        StringBuilder sb = new StringBuilder(prefix.length() + 20);
        sb.append(prefix);
        for (int i = digits.length(); i < 20; i++) {
            sb.append('0');
        }
        return sb.append(digits).toString();
    }
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
