package com.ebremer.beakgraph.features;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.Lang;
import org.apache.jena.query.Dataset;
import java.io.FileOutputStream;
import java.io.OutputStream;
import org.apache.jena.query.DatasetFactory;

public class NQToTriQ {
    public static void main(String[] args) throws Exception {
        IO.println("Reading...");
        String in = "D:\\beakgraph\\dest\\compressed\\TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.nq";
        String out = in.replace(".nq", ".triq");
        Dataset ds = DatasetFactory.create();
        RDFDataMgr.read(ds, in, Lang.NQUADS);
        IO.println("Writing...");
        try (OutputStream os = new FileOutputStream(out)) {
            RDFDataMgr.write(os, ds, Lang.TRIG);
        }
    }
}