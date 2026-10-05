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


    /**
     * Registers the datatype with Jena's TypeMapper (idempotent). Called from
     * the JVM entry points (BeakGraph, HDF5Reader, the writers' parse), so
     * registration is deterministic and complete BEFORE any document is
     * parsed. It used to happen as a side effect of the first reference to
     * {@link #INSTANCE} - on a spatial worker thread, halfway through a
     * streaming RIOT parse - so the same run validated some wktLiterals
     * through this class and others through a generic datatype.
     */
    public static void register() {
        // A class static initializer may run this before anything touched Jena;
        // the TypeMapper instance only exists after JenaSystem.init().
        org.apache.jena.sys.JenaSystem.init();
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

    private static final java.util.regex.Pattern WKT_HEAD = java.util.regex.Pattern.compile(
            "^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION|LINEARRING)\\b.*",
            java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.DOTALL);

    /**
     * A cheap syntactic check, as the docstring always claimed: RIOT calls it
     * for EVERY wktLiteral when literal checking is on (the Turtle/TriG
     * default), and the former implementation did a full JTS parse per call
     * on top of the parses the spatial index itself performs. Optional CRS
     * prefix, a WKT geometry keyword, balanced parentheses. Structural
     * validity is established by {@link #parse} where the geometry is needed.
     */
    @Override
    public boolean isValid(String lexicalForm) {
        if (lexicalForm == null || lexicalForm.isBlank()) {
            return false;
        }
        String wkt = lexicalForm.trim();
        if (wkt.startsWith("<")) {
            int end = wkt.indexOf('>');
            if (end < 0) {
                return false;
            }
            wkt = wkt.substring(end + 1).trim();
        }
        if (!WKT_HEAD.matcher(wkt).matches()) {
            return false;
        }
        int depth = 0;
        for (int i = 0; i < wkt.length(); i++) {
            char c = wkt.charAt(i);
            if (c == '(') depth++;
            else if (c == ')' && --depth < 0) return false;
        }
        return depth == 0;
    }
    
    // NOTE: no isEqual override. An earlier version compared wktLiterals by JTS
    // topological equality, which changes RDF *term* equality - sameTerm, HashSet
    // membership, and the writer's dictionary deduplication would silently
    // collapse lexically distinct geometries (and pay a JTS parse per equality
    // check). Term equality stays lexical (the BaseDatatype default); geometric
    // comparison belongs in filter functions like geof:sfIntersects.
}
