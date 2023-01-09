/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.extra;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

/**
 *
 * @author erich
 */
public class MemTest {
    public static void main(String[] args) {
        System.gc();
        long start = Runtime.getRuntime().freeMemory();
        System.out.println("RAM   : "+(Runtime.getRuntime().totalMemory())/1024L/1024L);
        System.out.println("BEGIN : "+(start)/1024L/1024L);
        Model m = RDFDataMgr.loadModel("/data2/halcyon/seg/TCGA-3C-AALI-01Z-00-DX1.F6E9A5DF-D8FB-45CF-B4BD-C6B76294C291.ttl",Lang.TURTLE);
        System.out.println(m.size());
        long end = Runtime.getRuntime().freeMemory();
        System.out.println("RAM   : "+(Runtime.getRuntime().totalMemory())/1024L/1024L);
        System.out.println("END   : "+end/1024L/1024L);
        System.out.println((start-end)/1024L/1024L);
        System.out.println(m.size());
    }
    
}
