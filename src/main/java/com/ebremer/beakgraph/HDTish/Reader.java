package com.ebremer.beakgraph.HDTish;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;
import java.nio.ByteBuffer;

/**
 *
 * @author Erich Bremer
 */
public class Reader {
    
    public static void main(String[] args) {
        File src = new File("/data/dX.h5");
        try (HdfFile hdfFile = new HdfFile(src.toPath())) {
            ContiguousDataset dataset = (ContiguousDataset) hdfFile.getDatasetByPath("/HDT/objects/integers");
            byte[] ha = (byte[]) dataset.getData();
            System.out.println(ha.length);
            ByteBuffer bb = dataset.getBuffer();
            System.out.println(bb.getClass().toGenericString());
        }
    }
}
