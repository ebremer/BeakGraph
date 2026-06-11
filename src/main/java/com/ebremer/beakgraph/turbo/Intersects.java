package com.ebremer.beakgraph.turbo;

import com.ebremer.ns.GEO;
import java.util.List;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.query.QueryBuildException;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * JTS-backed implementation of geof:sfIntersects. This is the fallback for any
 * sfIntersects the Hilbert index rewrite does not capture, so it must answer
 * honestly - the previous version returned TRUE unconditionally, which made
 * every uncaptured spatial filter match everything.
 */
public class Intersects extends FunctionBase {

    private static final String WKT_DATATYPE_URI = GEO.wktLiteral.getURI();

    @Override
    public void checkBuild(String uri, ExprList args) {
        if (( args.size() < 2 ) || ( args.size() > 3 )) {
            throw new QueryBuildException("Function '" + Lib.className(this) + "' takes two or three arguments");
        }
    }

    @Override
    public NodeValue exec(List<NodeValue> args) {
        NodeValue v1 = args.get(0);
        NodeValue v2 = args.get(1);
        if (v1 == null || v2 == null) {
            throw new ExprEvalException("sfIntersects: arguments cannot be null");
        }
        if (!isValidGeometryLiteral(v1) || !isValidGeometryLiteral(v2)) {
            throw new ExprEvalException("sfIntersects: arguments must be geo:wktLiteral");
        }
        try {
            boolean intersects = performSpatialCheck(
                v1.asNode().getLiteralLexicalForm(),
                v2.asNode().getLiteralLexicalForm());
            return intersects ? NodeValue.TRUE : NodeValue.FALSE;
        } catch (Exception e) {
            throw new ExprEvalException("sfIntersects: " + e.getMessage());
        }
    }

    private boolean isValidGeometryLiteral(NodeValue nv) {
        if (!nv.isLiteral()) return false;
        String dtURI = nv.asNode().getLiteralDatatypeURI();
        return WKT_DATATYPE_URI.equals(dtURI);
    }

    /**
     * Uses JTS to check whether the two WKT geometries intersect (share at
     * least one point) - the geof:sfIntersects relation.
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
        Geometry g1 = repaired(reader.read(cleanWkt1));
        Geometry g2 = repaired(reader.read(cleanWkt2));

        return g1.intersects(g2);
    }

    /**
     * Topologically invalid geometry (self-intersecting rings - a data reality
     * in pathology exports) must not make this function error out: the build
     * deliberately indexes such geometries, and throwing here made Jena drop
     * the row for every candidate whose stored WKT is invalid - an indexed
     * geometry that no sfIntersects query could ever return. GeometryFixer
     * preserves the point set (a bowtie becomes its two triangles), unlike
     * buffer(0), which discards zero-area parts and lines/points.
     */
    private static Geometry repaired(Geometry g) {
        return g.isValid() ? g : GeometryFixer.fix(g);
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