/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.extra;

import com.ebremer.beakgraph.rdf.BeakGraph;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.jena.sys.JenaSystem;

/**
 *
 * @author erich
 */
public class Test {
    
    public static void main(String[] args) throws URISyntaxException, IOException {
        JenaSystem.init();
        //URI uri = new URI("file:///D:/data2/halcyon/hm.zip");
        URI uri = new URI("file:///D:/HalcyonStorage/neo/x.zip");
        BeakGraph g = new BeakGraph(uri);
    }
    
}
