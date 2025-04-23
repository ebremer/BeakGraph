package com.ebremer.beakgraph.v2;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import java.nio.file.Paths;

public class ReadSingleByteExample {
    public static void main(String[] args) {
        String filePath = "/tcga/rdf.hdf5";
        try (HdfFile hdfFile = new HdfFile(Paths.get(filePath))) {
            Dataset dataset = hdfFile.getDatasetByPath("/byte_group/byte_dataset");            
            System.out.println(dataset.getData().getClass().toGenericString());            
            System.out.println(dataset.getData(new long[]{0}, new int[]{1}).getClass().toGenericString());
            byte[] bb = (byte[]) dataset.getData(new long[]{42}, new int[]{1});
            System.out.println("THIS --> "+bb[0]);
        } catch (Exception e) {
            System.err.println("An error occurred while reading the file:");
            e.printStackTrace();
        }
    }
}