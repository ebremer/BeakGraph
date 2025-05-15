package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.utils.UTIL;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 * @author Erich Bremer
 */
public class FCDReader {
    
    private final ByteBuffer buffer;
    private final ByteBuffer offsets;
    private final long blockSize;
    //private final long numBlocks;
    //private final long numEntries;
    
    public FCDReader(Group strings) {
        ContiguousDataset stringbuffer = (ContiguousDataset) strings.getChild("stringbuffer");
        ContiguousDataset off = (ContiguousDataset) strings.getChild("offsets");
        this.buffer = stringbuffer.getBuffer();
        this.offsets = off.getBuffer().order(ByteOrder.BIG_ENDIAN);
        this.blockSize = (int) strings.getAttribute("blockSize").getData();
        //this.numBlocks = (long) strings.getAttribute("numBlocks").getData();
        //this.numEntries = (long) strings.getAttribute("numEntries").getData();
    }
    
    public String get(long n) {
        long block = n / blockSize;
        int ha = (int) block;
        long address = offsets.getLong(ha*8);
        long offset = n % blockSize;
        buffer.position((int) address);
        if (offset==0) {
            return UTIL.readNullTerminatedString(buffer);
        }
        String base =  UTIL.readNullTerminatedString(buffer);
        UTIL.skipNullTerminatedStrings(buffer, (int) offset-1);
        int rr = (int) VByte.decodeSingle(buffer);        
        String frag = UTIL.readNullTerminatedString(buffer);
        return base.substring(0, rr) + frag;
    }
    
    public int locate(String x) {
        return -1;
    }
}
