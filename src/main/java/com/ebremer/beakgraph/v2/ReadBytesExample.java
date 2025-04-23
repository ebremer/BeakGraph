package com.ebremer.beakgraph.v2;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import java.nio.file.Paths;

public class ReadBytesExample {
    public static void main(String[] args) {
        try (HdfFile hdfFile = new HdfFile(Paths.get("/tcga/rdf.hdf5"))) {
            Dataset dataset = hdfFile.getDatasetByPath("/byte_group/byte_dataset");
            if (dataset == null) {
                System.out.println("Dataset not found");
                return;
            }              
            Object data = dataset.getData();
            if (data instanceof byte[] byteData) {
                for (int i = 0; i < byteData.length; i++) {
                    System.out.printf("%02X ", byteData[i]);
                    if ((i + 1) % 16 == 0) {
                        System.out.println();
                    }
                }
                if (byteData.length % 16 != 0) {
                    System.out.println();
                }
            } else {
                System.out.println("Dataset is not a byte array");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
