package com.ebremer.halcyon.hilbert;

import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class WKTDatatype extends BaseDatatype {

    public static final String URI = "http://www.opengis.net/ont/geosparql#wktLiteral";
    public static final WKTDatatype INSTANCE = new WKTDatatype();

    static {
        // Self-register on first touch: without this, the parse/equality machinery
        // below was inert - TypeMapper handed out a generic datatype for
        // geo:wktLiteral and nothing ever consulted this class.
        org.apache.jena.datatypes.TypeMapper.getInstance().registerDatatype(INSTANCE);
    }

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
            String cleanWkt = lexicalForm;
            if (lexicalForm.startsWith("<")) {
                int endUri = lexicalForm.indexOf(">");
                if (endUri > -1) {
                    cleanWkt = lexicalForm.substring(endUri + 1).trim();
                }
            }
            WKTReader reader = new WKTReader();
            return reader.read(cleanWkt);
        } catch (ParseException | RuntimeException e) {
            // JTS throws IllegalArgumentException - not ParseException - for
            // structurally invalid geometry (e.g. a two-point ring), and RIOT
            // validates every wktLiteral through this method now that the
            // datatype is registered. Anything escaping here turns one bad
            // literal into a fatal abort of the whole parse; wrapping it makes
            // it the ill-typed-literal warning it should be.
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
    
    // NOTE: no isEqual override. An earlier version compared wktLiterals by JTS
    // topological equality, which changes RDF *term* equality - sameTerm, HashSet
    // membership, and the writer's dictionary deduplication would silently
    // collapse lexically distinct geometries (and pay a JTS parse per equality
    // check). Term equality stays lexical (the BaseDatatype default); geometric
    // comparison belongs in filter functions like geof:sfIntersects.
}
