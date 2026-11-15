package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.store.StoreGroup;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;

/**
 * Generalized IndexReader that works with any index type (GSPO, GPOS, etc.)
 * Updated to expose raw BitPacked buffers for robust traversal.
 * @author Erich Bremer
 */
public class IndexReader {
    
    private final Index indexType;
    // A component is one of the letters G, S, P, O, and every iterator asks for
    // nine of these as it is built - once per input row of a join, once per
    // graph of a walk over the named graphs. Indexed by the letter itself:
    // the maps keyed by a boxed Character that stood here were a fifth of the
    // time of a subject lookup across 5,000 graphs.
    private static final int LETTERS = 128;
    private final BitPackedUnSignedLongBuffer[] bitmaps = new BitPackedUnSignedLongBuffer[LETTERS];
    private final BitPackedUnSignedLongBuffer[] ids = new BitPackedUnSignedLongBuffer[LETTERS];
    private final HDTBitmapDirectory[] componentDirectories = new HDTBitmapDirectory[LETTERS];
    private final char[] positions;
    private boolean directoryDatasets = false;
    
    public IndexReader(StoreGroup index, Index indexType, long formatVersion) {
        this.indexType = indexType;
        String indexName = indexType.name();
        this.positions = new char[4];
        for (int i = 0; i < 4; i++) {
            positions[i] = indexName.charAt(i);
        }
        // Only build the accelerated rank/select directory for files whose directory
        // layout is correct (v3+). For older files it is left absent, so the iterators
        // fall back to the linear (slower but correct) select1 scan over the bitmap.
        boolean directoryUsable = formatVersion >= Params.RANK_DIRECTORY_MIN_VERSION;
        for (int i = 1; i < 4; i++) {
            char component = positions[i];
            String suffix = String.valueOf(component).toLowerCase(java.util.Locale.ROOT);

            BitPackedUnSignedLongBuffer bitmap = loadBuffer(index, "B" + suffix);
            BitPackedUnSignedLongBuffer idBuffer = loadBuffer(index, "S" + suffix);

            BitPackedUnSignedLongBuffer sb = loadBuffer(index, "SB" + suffix);
            BitPackedUnSignedLongBuffer bb = loadBuffer(index, "BB" + suffix);

            // Every level's bitmap and id list have the same length (SPECIFICATIONS
            // §8.1); a file whose declared counts disagree used to open and then
            // read one column past its end inside a query (BG-81).
            if (bitmap != null && idBuffer != null && bitmap.getNumEntries() != idBuffer.getNumEntries()) {
                throw new IllegalStateException("index " + indexName + ": B" + suffix + " has " + bitmap.getNumEntries()
                        + " rows but S" + suffix + " has " + idBuffer.getNumEntries()
                        + " (every level's bitmap and id list have the same length)");
            }
            if (component >= LETTERS) {
                throw new IllegalStateException("index " + indexName + ": '" + component + "' is not a component letter");
            }
            bitmaps[component] = bitmap;
            ids[component] = idBuffer;
            if (sb != null && bb != null) {
                directoryDatasets = true;
            }

            if (directoryUsable && bitmap != null && idBuffer != null && sb != null && bb != null) {
                componentDirectories[component] = new HDTBitmapDirectory(sb, bb, bitmap);
            }
        }
    }
    
    private static BitPackedUnSignedLongBuffer loadBuffer(StoreGroup index, String name) {
        return StoreProfile.packed(index, name); // null when absent; a profile violation names the dataset
    }
    
    public HDTBitmapDirectory getDirectory(char component) {
        return (component < LETTERS) ? componentDirectories[component] : null;
    }

    /** True when the group carries rank/select directory datasets (SB and BB per component), whether or not the format version lets them be used. */
    public boolean hasDirectoryDatasets() {
        return directoryDatasets;
    }
    
    public BitPackedUnSignedLongBuffer getBitmapBuffer(char component) {
        return (component < LETTERS) ? bitmaps[component] : null;
    }
    
    public BitPackedUnSignedLongBuffer getIDBuffer(char component) {
        return (component < LETTERS) ? ids[component] : null;
    }

    public Index getIndexType() {
        return indexType;
    }
}
