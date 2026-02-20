package com.ebremer.beakgraph.tests;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.turbo.Spatial;
import java.io.File;
import java.io.IOException;
import org.apache.jena.sys.JenaSystem;

public class Test4 {

    public static void main(String[] args) {
        JenaSystem.init();
        Spatial.init();
        File dest = new File("D:\\beakgraph\\dest\\compressed\\TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.h5");
        File dumpDir = new File("D:\\beakgraph\\dump");
        if (!dumpDir.exists()) dumpDir.mkdirs();
        try (HDF5Reader reader = new HDF5Reader(dest)) {
            BeakGraph bg = new BeakGraph(reader);
            bg.getReader().getDictionary().streamPredicates().forEach(n->IO.println(n));
        } catch (IOException ex) {
            System.getLogger(Test4.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } catch (Throwable ex) {
            IO.println(ex.getMessage());
        }
    }
}