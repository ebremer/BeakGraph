package com.ebremer.beakgraph.arrow;

import java.util.HashMap;
import org.apache.jena.graph.Triple;
//import org.apache.jena.tdb.store.NodeId;

/**
 *
 * @author erich
 */
public class Exp {
    
    public static void main(String[] args) {
  //      NodeId n;
        Triple sds;
        Integer i = 5;
        String w = "booooo";
        Long z = 44343L;
        Class x = i.getClass();
        System.out.println(i.getClass().equals(Integer.class));
        System.out.println(z.getClass().equals(Integer.class));
        HashMap<Class,String> ha = new HashMap<>();
        ha.put(i.getClass(), "this is an integer");
        ha.put(z.getClass(), "this is an Long");
        ha.put(w.getClass(), "this is an String");
        ha.forEach((k,v)->{
            System.out.println(k+"  "+v);
        });  
    }
    
}
