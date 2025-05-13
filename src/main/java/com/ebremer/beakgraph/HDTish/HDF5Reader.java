package com.ebremer.beakgraph.HDTish;

import io.jhdf.HdfFile;
import io.jhdf.api.dataset.ContiguousDataset;
import java.io.File;
import java.util.stream.IntStream;

/**
 *
 * @author Erich Bremer
 */
public class HDF5Reader {
    
    public static void main(String[] args) {
        File src = new File("/data/dX.h5");
        try (HdfFile hdfFile = new HdfFile(src.toPath())) {
            ContiguousDataset dataset = (ContiguousDataset) hdfFile.getDatasetByPath("/HDT/objects/integers");
            long width = (Long) dataset.getAttribute("width").getData();
            System.out.println("BIT WIDTH = "+width);
            BitPackedReader bpr = new BitPackedReader(dataset.getBuffer(), (int) width);
            
            IntStream.range(0, 27).forEach(i->{
                System.out.println(i+" ==> "+bpr.readNthValue(i));
            });


        }
    }
}
