package com.ebremer.beakgraph.rdf;

import org.apache.jena.rdf.model.Resource;

/**
 *
 * @author erich
 */
public enum DataType {
    INTEGER, LONG, FLOAT, STRING, RESOURCE;
        
    public static DataType getDataType(Class clazz) {
        if (clazz == Integer.class) {
            return INTEGER;
        } else if (clazz == Long.class) {
            return LONG;
        } else if (clazz == Float.class) {
            return FLOAT;
        } else if (clazz == String.class) {
            return STRING;
        } else if ((clazz == Resource.class)||(clazz == org.apache.jena.rdf.model.impl.ResourceImpl.class)) {
            return RESOURCE;
        } else {
            throw new Error("Can't handle this type :"+clazz);
        }
    }
    
    public static DataType getType(String dt) {
        return
            switch (dt) {
                case "I" -> INTEGER;
                case "L" -> LONG;
                case "F" -> FLOAT;
                case "S" -> STRING;
                case "R" -> RESOURCE;
                default -> throw new Error("WTH is : " + dt);
            };
    }
}
