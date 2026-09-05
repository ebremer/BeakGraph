package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.util.HashMap;
import java.util.Map;

/**
 * Generalized IndexReader that works with any index type (GSPO, GPOS, etc.)
 * Updated to expose raw BitPacked buffers for robust traversal.
 * @author Erich Bremer
 */
public class IndexReader {
    
    private final Index indexType;
    private final Map<Character, BitPackedUnSignedLongBuffer> bitmaps = new HashMap<>();
    private final Map<Character, BitPackedUnSignedLongBuffer> ids = new HashMap<>();
    private final Map<Character, HDTBitmapDirectory> componentDirectories = new HashMap<>();
    private final char[] positions;
    private boolean directoryDatasets = false;
    
    public IndexReader(Group index, Index indexType, long formatVersion) {
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

            bitmaps.put(component, bitmap);
            ids.put(component, idBuffer);
            if (sb != null && bb != null) {
                directoryDatasets = true;
            }

            if (directoryUsable && bitmap != null && idBuffer != null && sb != null && bb != null) {
                componentDirectories.put(component, new HDTBitmapDirectory(sb, bb, bitmap, idBuffer));
            }
        }
    }
    
    private BitPackedUnSignedLongBuffer loadBuffer(Group index, String name) {
        ContiguousDataset ds = (ContiguousDataset) index.getChild(name);
        if (ds == null) return null;
        long num = (Long) ds.getAttribute("numEntries").getData();
        int width = (Integer) ds.getAttribute("width").getData();
        return BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(ds), num, width);
    }
    
    public HDTBitmapDirectory getDirectory(char component) {
        return componentDirectories.get(component);
    }

    /** True when the group carries rank/select directory datasets (SB and BB per component), whether or not the format version lets them be used. */
    public boolean hasDirectoryDatasets() {
        return directoryDatasets;
    }
    
    public BitPackedUnSignedLongBuffer getBitmapBuffer(char component) {
        return bitmaps.get(component);
    }
    
    public BitPackedUnSignedLongBuffer getIDBuffer(char component) {
        return ids.get(component);
    }

    public Index getIndexType() {
        return indexType;
    }
}
