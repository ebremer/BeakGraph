package com.ebremer.beakgraph.core;

/**
 *
 * @author Erich Bremer
 */
public abstract class AbstractDictionary implements Dictionary {
    private String name = "";
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
}
