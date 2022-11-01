/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.arrow;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 *
 * @author erich
 */
public class Pair {
    private String i;
    private IntVector s;
    private ValueVector o;
            
    public Pair(String i, IntVector s, ValueVector o) {
        this.i = i;
        this.s = s;
        this.o = o;
    }
    
    public void validate() {
        switch (i) {
            case "SO":
                break;
            case "OS":
                break;
            default: throw new Error("What kind of sort is : "+i);
        }
    }
    
}
