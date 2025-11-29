package com.ebremer.halcyon.hilbert;

import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.graph.impl.LiteralLabel;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class WKTDatatype extends BaseDatatype {

    // The standard GeoSPARQL URI for WKT literals
    public static final String URI = "http://www.opengis.net/ont/geosparql#wktLiteral";
    
    // Singleton instance (Jena prefers singletons for Datatypes)
    // FIX: Changed "WktDatatype" to "WKTDatatype" below
    public static final WKTDatatype INSTANCE = new WKTDatatype();

    private WKTDatatype() {
        super(URI);
    }

    /**
     * Parse the Lexical Form (String) into a Java Object (JTS Geometry).
     * GeoSPARQL literals can look like: "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(10 20)"
     * @param lexicalForm
     * @return 
     */
    @Override
    public Object parse(String lexicalForm) throws DatatypeFormatException {
        if (lexicalForm == null || lexicalForm.isEmpty()) {
            return null;
        }

        try {
            // 1. Handle GeoSPARQL CRS Prefix (e.g., <http://...>)
            // We strip the URI to get the raw WKT for JTS.
            // In a full implementation, you might want to store the SRID in the Geometry's userData.
            String cleanWkt = lexicalForm;
            if (lexicalForm.startsWith("<")) {
                int endUri = lexicalForm.indexOf(">");
                if (endUri > -1) {
                    cleanWkt = lexicalForm.substring(endUri + 1).trim();
                }
            }

            // 2. Parse using JTS
            WKTReader reader = new WKTReader();
            return reader.read(cleanWkt);

        } catch (ParseException e) {
            throw new DatatypeFormatException(
                lexicalForm, 
                this, 
                "Invalid WKT format: " + e.getMessage()
            );
        }
    }

    /**
     * Converts the Java Object (JTS Geometry) back into a String.
     * @param value
     * @return 
     */
    @Override
    public String unparse(Object value) {
        if (value instanceof Geometry geometry) {
            WKTWriter writer = new WKTWriter();
            return writer.write(geometry);
        }
        return value.toString();
    }

    /**
     * Validate that the string is correct WKT without creating the heavy object.
     * @param lexicalForm
     * @return 
     */
    @Override
    public boolean isValid(String lexicalForm) {
        try {
            parse(lexicalForm);
            return true;
        } catch (DatatypeFormatException e) {
            return false;
        }
    }
    
    /**
     * Comparison logic for SPARQL FILTERs (optional but recommended).
     * @param value1
     * @param value2
     * @return 
     */
    @Override
    public boolean isEqual(LiteralLabel value1, LiteralLabel value2) {
        // Parse both to Geometries and check equality (topological equality)
        if (value1.getDatatype() == this && value2.getDatatype() == this) {
            try {
                Geometry g1 = (Geometry) value1.getValue();
                Geometry g2 = (Geometry) value2.getValue();
                return g1.equals(g2); // JTS topological equality
            } catch (Exception e) {
                return false;
            }
        }
        return super.isEqual(value1, value2);
    }
}