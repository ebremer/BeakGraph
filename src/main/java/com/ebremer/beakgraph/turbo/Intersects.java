package com.ebremer.beakgraph.turbo;

import com.ebremer.ns.GEO;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.query.QueryBuildException;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * JTS-backed implementation of geof:sfIntersects. This is the verification
 * stage behind the Hilbert index (and the fallback for any sfIntersects the
 * index rewrite does not capture), so it must answer honestly - the previous
 * version returned TRUE unconditionally, which made every uncaptured spatial
 * filter match everything.
 * <p>
 * It is evaluated once per candidate row. The query region is a constant, so
 * parsing, validating and repairing it per row dominated whole-slide queries
 * (10^5-10^6 candidates against a polygon with thousands of vertices). Each
 * thread keeps a small LRU of lexical form -&gt; prepared, repaired geometry;
 * the constant stays hot and is prepared once, and candidates are tested with
 * {@code PreparedGeometry.intersects}. The cache is per thread because a JTS
 * PreparedGeometry builds its indexes lazily and is not safe to share.
 * <p>
 * CRS: BeakGraph compares raw coordinates in one Cartesian CRS (its domain is
 * slide/pixel space). A {@code <crs>} prefix is stripped, never transformed.
 * A literal without a prefix takes the CRS of the other operand (data
 * exported with an explicit prefix is routinely queried without one); two
 * literals that BOTH name a CRS and disagree are reported as an evaluation
 * error (the row is dropped) instead of being compared as if they shared
 * axes.
 */
public class Intersects extends FunctionBase {
    private static final String WKT_DATATYPE_URI = GEO.wktLiteral.getURI();
    /** GeoSPARQL's default CRS (informational: an unprefixed literal adopts the other operand's CRS). */
    static final String DEFAULT_CRS = "http://www.opengis.net/def/crs/OGC/1.3/CRS84";
    private static final int CACHE_SIZE = 8;
    // JTS >= 1.19: WKTReader.read() keeps its tokenizer local, so one instance
    // is safe to share as long as no setter is called after construction.
    private static final WKTReader READER = new WKTReader();
    private static final ThreadLocal<LinkedHashMap<String, Prepared>> CACHE = ThreadLocal.withInitial(() ->
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Prepared> eldest) {
                    return size() > CACHE_SIZE;
                }
            });

    /** A parsed, repaired geometry with its explicit CRS (null when unprefixed) and its prepared form. */
    record Prepared(String crs, Geometry geometry, PreparedGeometry prepared) {}

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
        } catch (ExprEvalException e) {
            throw e;
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
     * Whether the two WKT geometries share at least one point - the
     * geof:sfIntersects relation - in one common CRS.
     */
    static boolean performSpatialCheck(String wkt1, String wkt2) throws ParseException {
        Prepared g1 = prepared(wkt1);
        Prepared g2 = prepared(wkt2);
        if (g1.crs() != null && g2.crs() != null && !g1.crs().equals(g2.crs())) {
            throw new ExprEvalException("sfIntersects: CRS mismatch <" + g1.crs() + "> vs <" + g2.crs()
                    + ">: BeakGraph compares raw coordinates and performs no CRS transformation");
        }
        // The second argument is the query constant in the index rewrite and in
        // the usual FILTER(geof:sfIntersects(?w, "...")) shape; its prepared
        // form does the work. Both are cached, so either order stays cheap.
        return g2.prepared().intersects(g1.geometry());
    }

    private static Prepared prepared(String literal) throws ParseException {
        LinkedHashMap<String, Prepared> cache = CACHE.get();
        Prepared p = cache.get(literal);
        if (p == null) {
            String trimmed = literal.trim();
            String crs = null;
            String wkt = trimmed;
            if (trimmed.startsWith("<")) {
                int endUri = trimmed.indexOf('>');
                if (endUri != -1) {
                    crs = trimmed.substring(1, endUri).trim();
                    wkt = trimmed.substring(endUri + 1).trim();
                }
            }
            Geometry g = repaired(READER.read(wkt));
            p = new Prepared(crs, g, PreparedGeometryFactory.prepare(g));
            cache.put(literal, p);
        }
        return p;
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
}
