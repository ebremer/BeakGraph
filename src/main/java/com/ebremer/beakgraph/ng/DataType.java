package com.ebremer.beakgraph.ng;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author erich
 */
public enum DataType {
    INTEGER, LONG, FLOAT, STRING, RESOURCE, DOUBLE;
        
    public static DataType getDataType(Class clazz) {
        if (clazz == Integer.class) {
            return INTEGER;
        } else if (clazz == Long.class) {
            return LONG;
        } else if (clazz == Float.class) {
            return FLOAT;
        } else if (clazz == Double.class) {
            return DOUBLE;
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
                case "D" -> DOUBLE;
                case "S" -> STRING;
                case "R" -> RESOURCE;
                default -> throw new Error("WTH is : " + dt);
            };
    }
    
    public static String getTypeFromIRI(String iri) {
        if ( iri.equals(XSD.xint.getURI())) return "I";
        if ( iri.equals(XSD.xlong.getURI())) return "L";
        if ( iri.equals(XSD.xfloat.getURI())) return "F";
        if ( iri.equals(XSD.xdouble.getURI())) return "D";
        if ( iri.equals(XSD.xstring.getURI())) return "S";
        if ( iri.equals(XSD.anyURI.getURI())) return "R";
        throw new Error("WTH is : " + iri);
    }
}
