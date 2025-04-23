package com.ebremer.beakgraph.v2;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import io.jhdf.object.datatype.BitField;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.BitSet;

public class WriteBytesExample {
    public static void main(String[] args) {
        byte[] bytes = new byte[256];       
        for (int i=0; i<256; i++) {
            bytes[i] = (byte) i;
        }
        try (WritableHdfFile hdfFile = HdfFile.write(Paths.get("/tcga/rdf.hdf5"))) {
            hdfFile.putDataset("YAY", new String[] {"1","2","3","4","5","6"});
            WritableGroup group = hdfFile.putGroup("byte_group");
            group.putAttribute("metadata", "this is really cool");
            group.putDataset("byte_dataset", bytes);           
            
            BitSet bitSet = new BitSet(1024);
            bitSet.set(0);
            bitSet.set(1);
            bitSet.set(1023);
            byte[] bitSetBytes = bitSet.toByteArray();   
            group.putDataset("smush", bitSetBytes);   
            
            BitField bf = new BitField(ByteBuffer.wrap(bitSetBytes));           
         //   group.putDataset("smushier", bf); 
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
