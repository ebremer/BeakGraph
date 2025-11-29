package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
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
    
    public IndexReader(Group index, Index indexType) {
        this.indexType = indexType;
        String indexName = indexType.name();
        this.positions = new char[4];
        for (int i = 0; i < 4; i++) {
            positions[i] = indexName.charAt(i);
        }
        for (int i = 1; i < 4; i++) {
            char component = positions[i];
            String suffix = String.valueOf(component).toLowerCase(); 
            
            BitPackedUnSignedLongBuffer bitmap = loadBuffer(index, "B" + suffix);
            BitPackedUnSignedLongBuffer idBuffer = loadBuffer(index, "S" + suffix);
            
            BitPackedUnSignedLongBuffer sb = loadBuffer(index, "SB" + suffix);
            BitPackedUnSignedLongBuffer bb = loadBuffer(index, "BB" + suffix);
            
            bitmaps.put(component, bitmap);
            ids.put(component, idBuffer);
            
            if (bitmap != null && idBuffer != null && sb != null && bb != null) {
                componentDirectories.put(component, new HDTBitmapDirectory(sb, bb, bitmap, idBuffer));
            }
        }
    }
    
    private BitPackedUnSignedLongBuffer loadBuffer(Group index, String name) {
        if (index.getChild(name) == null) return null;
        ContiguousDataset ds = (ContiguousDataset) index.getDatasetByPath(name);
        long num = (Long) ds.getAttribute("numEntries").getData();
        int width = (Integer) ds.getAttribute("width").getData();
        return new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), num, width);
    }
    
    public HDTBitmapDirectory getDirectory(char component) {
        return componentDirectories.get(component);
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
