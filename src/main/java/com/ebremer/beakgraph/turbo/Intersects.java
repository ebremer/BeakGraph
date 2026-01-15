package com.ebremer.beakgraph.turbo;

import com.ebremer.beakgraph.core.lib.GEO;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class Intersects extends FunctionBase2 {

    private static final String WKT_DATATYPE_URI = GEO.wktLiteral.getURI();

    @Override
    public NodeValue exec(NodeValue v1, NodeValue v2) {
        return NodeValue.TRUE;
        /*
        if (v1 == null || v2 == null) {
            throw new ExprEvalException("sfWithin: Arguments cannot be null");
        }
        if (!isValidGeometryLiteral(v1) || !isValidGeometryLiteral(v2)) {
            throw new ExprEvalException("sfWithin: Arguments must be geo:wktLiteral");
        }
        String geo1 = v1.asNode().getLiteralLexicalForm();
        String geo2 = v2.asNode().getLiteralLexicalForm();
        try {
            boolean isWithin = performSpatialCheck(geo1, geo2);
            return isWithin ? NodeValue.TRUE : NodeValue.FALSE;
        } catch (Exception e) {
            throw new ExprEvalException("sfWithin: Calculation failed: " + e.getMessage());
        }*/
    }

    private boolean isValidGeometryLiteral(NodeValue nv) {
        if (!nv.isLiteral()) return false;        
        String dtURI = nv.asNode().getLiteralDatatypeURI();
        return WKT_DATATYPE_URI.equals(dtURI);
    }

    /**
     * Uses JTS to check if wkt1 is within wkt2.
     * @param wkt1 The subject geometry (e.g., the point)
     * @param wkt2 The containing geometry (e.g., the polygon)
     * @return true if wkt1 is within wkt2
     * @throws ParseException if WKT is invalid
     */
    private boolean performSpatialCheck(String wkt1, String wkt2) throws ParseException {
        // JTS WKTReader is not thread-safe, so we instantiate it per call (stack confinement)
        // or use a ThreadLocal if object creation overhead becomes an issue.
        WKTReader reader = new WKTReader();

        // GeoSPARQL literals often look like "<http://epsg...> POINT(1 1)"
        // JTS only accepts "POINT(1 1)", so we must strip the URI prefix.
        String cleanWkt1 = extractWkt(wkt1);
        String cleanWkt2 = extractWkt(wkt2);
        Geometry g1 = reader.read(cleanWkt1);
        Geometry g2 = reader.read(cleanWkt2);
        if (!g1.isValid() || !g2.isValid()) {
           throw new ParseException("Encountered invalid geometry topology.");
        }

        return g1.within(g2);
    }

    /**
     * Helper to strip the CRS/SRS URI from a GeoSPARQL string.
     * Input: "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(30 10)"
     * Output: "POINT(30 10)"
     */
    private String extractWkt(String geoSparqlLiteral) {
        String trimmed = geoSparqlLiteral.trim();
        if (trimmed.startsWith("<")) {
            int endUri = trimmed.indexOf('>');
            if (endUri != -1) {
                // Return everything after the '>' character
                return trimmed.substring(endUri + 1).trim();
            }
        }
        return trimmed;
    }
}