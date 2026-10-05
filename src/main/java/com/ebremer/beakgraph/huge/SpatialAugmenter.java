package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.features.MajorMinor;
import com.ebremer.beakgraph.features.pyradiomics.Gen2DFeatures;
import static com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder.HILBERT_CELL_NS;
import static com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder.MAX_INDEX_CELLS;
import static com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder.MAX_INDEX_SCALE;
import com.ebremer.beakgraph.utils.ImageTools;
import com.ebremer.halcyon.hilbert.HilbertSpace;
import com.ebremer.halcyon.hilbert.PolygonScaler;
import com.ebremer.halcyon.hilbert.WKTDatatype;
import com.ebremer.ns.GEO;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spatial index + derived-feature quad generation for EVERY writer engine:
 * the in-memory builders
 * ({@link com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder#addSpatial})
 * delegate here, so there is one implementation of addSpatial /
 * addSpatialScales / addSpatialIndexCells / generateGridURNs / addFeatures
 * to keep in step with SpatialIndexIterator's floor snapping and clamping
 * (a byte-identical second copy used to live in the RAM builder, BG-296).
 * The tuning constants stay on PositionalDictionaryWriterBuilder, where the
 * query side imports them. Stateless and thread-safe: instances of the JTS
 * readers/factories are created per call.
 *
 * @author Erich Bremer
 */
public final class SpatialAugmenter {

    private static final Logger logger = LoggerFactory.getLogger(SpatialAugmenter.class);

    /** hal:asWKT0..14, generated instead of hand-enumerated (same URIs as the RAM writer). */
    private static final Node[] asWKT = new Node[15];
    static {
        for (int i = 0; i < asWKT.length; i++) {
            asWKT[i] = NodeFactory.createURI("https://halcyon.is/ns/asWKT" + i);
        }
    }

    private final boolean features;

    /** The pyramid generator applied to one polygonal part; a hook so a failing part can be simulated (BG-372). */
    interface PartScaler {
        Polygon[] scale(Polygon part);
    }

    private final PartScaler scaler;

    public SpatialAugmenter(boolean features) {
        this(features, PolygonScaler::toPolygons);
    }

    SpatialAugmenter(boolean features, PartScaler scaler) {
        this.features = features;
        this.scaler = scaler;
    }

    public static boolean isGeoLiteral(Quad quad) {
        Node o = quad.getObject();
        return o.isLiteral() && GEO.wktLiteral.getURI().equals(o.getLiteralDatatypeURI());
    }

    /**
     * Generates all derived quads for one geometry-carrying quad. A bad
     * geometry never aborts the build - it is logged and skipped, exactly like
     * the RAM writer.
     */
    public ArrayList<Quad> addSpatial(Quad quad) {
        final ArrayList<Quad> qqq = new ArrayList<>();
        // The GeoSPARQL-standard "<crs-uri> WKT" form must be indexed too: strip the
        // prefix once here so the parser and the scaler both see plain WKT
        // (previously such geometries failed the parse and were silently dropped).
        String wkt = ImageTools.stripCrs(quad.getObject().getLiteralLexicalForm());
        if (features) {
            // Same containment as the geometry block below: feature generation runs
            // inside the spatial task, so anything escaping here feeds Future.get()
            // and fails the whole write over one bad geometry.
            try {
                addFeatures(qqq, quad, wkt);
            } catch (Exception ex) {
                logger.warn("Failed to generate features for {}: {}", quad.getSubject(), ex.toString());
            }
        }
        // Everything geometry-related sits inside the catch-all below: a single bad
        // geometry must never abort the build (these tasks feed Future.get(), whose
        // ExecutionException would otherwise fail the whole write).
        try {
            // Index EVERY leaf, at any depth: each polygon (MULTIPOLYGON members,
            // a MULTIPOLYGON nested in a GEOMETRYCOLLECTION, collections inside
            // collections) gets its own pyramid and corner entries, and every
            // non-areal leaf (POINT, LINESTRING) is indexed via its expanded
            // envelope - PER LEAF, not only as a fallback when no polygon exists.
            // Indexing only the first part, only direct members, or only
            // non-polygonal members when no polygon existed each left geometry
            // silently unfindable by variable-subject queries (BG-371).
            Geometry g = new WKTReader().read(wkt);
            if (g.isEmpty()) {
                return qqq;
            }
            List<Polygon> parts = ImageTools.spatialParts(g);
            // The recall-safe Hilbert cells of EVERY part first: they never
            // depend on a pyramid, so a part whose pyramid step fails cannot
            // cost a LATER part its cells - one catch around the whole loop
            // used to drop them, leaving the geometry unfindable by the very
            // parts that were fine, while logging a mere "Skipping" (BG-372).
            for (Polygon part : parts) {
                addSpatialIndexCells(qqq, quad, part);
            }
            for (int i = 0; i < parts.size(); i++) {
                try {
                    addSpatialScales(qqq, quad, wkt, scaler.scale(parts.get(i)));
                } catch (RuntimeException ex) {
                    logger.warn("Skipping the scaled pyramid for part {}/{} of {} (its index cells are kept): {} ({})",
                            i + 1, parts.size(), quad.getSubject(), ex.toString(), abbrevWkt(wkt));
                }
            }
        } catch (Exception ex) {
            // Expected data condition (pathology exports contain degenerate
            // geometries such as two-point rings): one line per skip, no stack -
            // a slide can contain thousands of these. Only WKT parsing and the
            // part walk reach here, before anything was emitted for the literal.
            logger.warn("Skipping spatial indexing for {} (no cells or pyramid written for this literal): {} ({})",
                    quad.getSubject(), ex.toString(), abbrevWkt(wkt));
        }
        return qqq;
    }

    /**
     * Largest tile count one pyramid level may enumerate for one geometry.
     * The tile walk is O(extent^2 / tile^2) with a JTS intersection per tile:
     * a projected-CRS outline spanning 1e7 units meant ~4e8 tiles at level 0
     * (hours), a corrupt 1e9 vertex ~4e12 (never), each hit allocating a
     * quad - one literal could hang or OOM the build (BG-93). Levels over
     * the budget are skipped (logged once per geometry); the coarser levels
     * that fit are still emitted, and the Hilbert index cells - which are
     * bounded by construction - keep the geometry findable by sfIntersects.
     */
    public static final long MAX_GRID_TILES = 65_536;

    private void addSpatialScales(ArrayList<Quad> qqq, Quad quad, String wkt, Polygon[] scales) {
        if (scales == null) {
            return;
        }
        final String[] wktScales = PolygonScaler.toWKT(scales);
        int skipped = 0;
        long widest = 0;
        for (int s = 0; s < Math.min(scales.length, asWKT.length); s++) {
            List<Node> tiles = generateGridURNs(scales[s], s);
            if (tiles == null) {
                skipped++;
                widest = Math.max(widest, tileCount(scales[s]));
                tiles = List.of();
            }
            try {
                for (int ii = 0; ii < tiles.size(); ii++) {
                    qqq.add(Quad.create(tiles.get(ii), quad.getSubject(), asWKT[s],
                            NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)));
                }
            } catch (Exception ex) {
                logger.error("Failed to add spatial tile quads for {}", abbrevWkt(wkt), ex);
            }
            try {
                qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), asWKT[s],
                        NodeFactory.createLiteralDT(wktScales[s], WKTDatatype.INSTANCE)));
            } catch (Exception ex) {
                logger.error("Failed to add scaled WKT quad for {}", abbrevWkt(wkt), ex);
            }
        }
        if (skipped > 0) {
            logger.warn("Skipping the {} finest tile pyramid level(s) for {}: up to {} tiles exceeds {} ({})",
                    skipped, quad.getSubject(), widest, MAX_GRID_TILES, abbrevWkt(wkt));
        }
    }

    /** Tiles the polygon's envelope spans at {@code Params.GRIDTILESIZE}; saturates instead of overflowing. */
    private static long tileCount(Polygon polygon) {
        Envelope env = polygon.getEnvelopeInternal();
        double cellSize = Params.GRIDTILESIZE;
        long nx = (long) Math.floor(env.getMaxX() / cellSize) - (long) Math.floor(env.getMinX() / cellSize) + 1;
        long ny = (long) Math.floor(env.getMaxY() / cellSize) - (long) Math.floor(env.getMinY() / cellSize) + 1;
        if (nx <= 0 || ny <= 0) {
            return Long.MAX_VALUE; // an axis wider than a long: over any budget
        }
        return (nx > Long.MAX_VALUE / ny) ? Long.MAX_VALUE : nx * ny;
    }

    /**
     * Emits this part's recall-safe spatial index entries: the Hilbert indices of
     * every whole cell covering its bbox, at the coarsest scale where that cover
     * is at most {@code MAX_INDEX_CELLS} cells. Cell coordinates use floor
     * snapping - the query side MUST snap identically or shared cells are missed.
     */
    private void addSpatialIndexCells(ArrayList<Quad> qqq, Quad quad, Polygon part) {
        Envelope env = part.getEnvelopeInternal();
        if (env.isNull()) {
            // Belt and braces: spatialParts already drops empty polygons; an
            // empty part's null envelope would clamp onto cell 0 (BG-375).
            return;
        }
        // The Hilbert domain is [0, 2^31), so bboxes are CLAMPED into it - never
        // skipped, never allowed to alias (the curve masks out-of-range bits).
        // Clamping is a monotone projection applied identically on the query
        // side, so overlapping boxes still overlap after it and recall is
        // preserved; out-of-domain geometry just indexes coarsely at the domain
        // edge cells (false positives there are killed by the sfIntersects
        // verification on the exact original WKT). Skipping fully-negative
        // geometry instead made it unfindable by ANY query.
        long minX = HilbertSpace.clampToDomain((long) Math.floor(env.getMinX()));
        long maxX = HilbertSpace.clampToDomain((long) Math.floor(env.getMaxX()));
        long minY = HilbertSpace.clampToDomain((long) Math.floor(env.getMinY()));
        long maxY = HilbertSpace.clampToDomain((long) Math.floor(env.getMaxY()));
        int s = 0;
        while (s < MAX_INDEX_SCALE && cellCount(minX, maxX, minY, maxY, s) > MAX_INDEX_CELLS) {
            s++;
        }
        long cell = 1L << s;
        Node pred = NodeFactory.createURI(HILBERT_CELL_NS + s);
        HashSet<Long> cells = new HashSet<>();
        for (long x = Math.floorDiv(minX, cell); x <= Math.floorDiv(maxX, cell); x++) {
            for (long y = Math.floorDiv(minY, cell); y <= Math.floorDiv(maxY, cell); y++) {
                cells.add(HilbertSpace.hc.index(new long[]{x, y}));
            }
        }
        for (Long c : cells) {
            qqq.add(Quad.create(Params.SPATIAL, quad.getSubject(), pred,
                    NodeFactory.createLiteralByValue((long) c)));
        }
    }

    private static long cellCount(long minX, long maxX, long minY, long maxY, int s) {
        long cell = 1L << s;
        long nx = Math.floorDiv(maxX, cell) - Math.floorDiv(minX, cell) + 1;
        long ny = Math.floorDiv(maxY, cell) - Math.floorDiv(minY, cell) + 1;
        return nx * ny;
    }

    /** The tile graphs this level's polygon intersects, or null when the level is over {@link #MAX_GRID_TILES}. */
    private List<Node> generateGridURNs(Polygon polygon, int resolutionLevel) {
        if (tileCount(polygon) > MAX_GRID_TILES) {
            return null;
        }
        List<Node> intersectingURNs = new ArrayList<>();
        Envelope env = polygon.getEnvelopeInternal();
        double cellSize = Params.GRIDTILESIZE;
        long minTileX = (long) Math.floor(env.getMinX() / cellSize);
        long maxTileX = (long) Math.floor(env.getMaxX() / cellSize);
        long minTileY = (long) Math.floor(env.getMinY() / cellSize);
        long maxTileY = (long) Math.floor(env.getMaxY() / cellSize);
        GeometryFactory gf = polygon.getFactory();
        // One prepared geometry per level: the per-tile test is then an
        // indexed predicate instead of a full JTS relate.
        PreparedGeometry prepared = PreparedGeometryFactory.prepare(polygon);
        for (long x = minTileX; x <= maxTileX; x++) {
            double tileMinX = x * cellSize;
            double tileMaxX = tileMinX + cellSize;
            for (long y = minTileY; y <= maxTileY; y++) {
                double tileMinY = y * cellSize;
                double tileMaxY = tileMinY + cellSize;
                Envelope tileEnv = new Envelope(tileMinX, tileMaxX, tileMinY, tileMaxY);
                if (env.intersects(tileEnv)) {
                    Polygon tilePoly = (Polygon) gf.toGeometry(tileEnv);
                    if (prepared.intersects(tilePoly)) {
                        intersectingURNs.add(Params.gridGraph(resolutionLevel, x, y));
                    }
                }
            }
        }
        return intersectingURNs;
    }

    // Takes the CRS-stripped WKT computed once in addSpatial: re-reading the raw
    // lexical form here made JTS throw on every "<crs-uri> WKT"-form literal, so
    // CRS-prefixed geometries silently got no derived features while identical
    // unprefixed ones did.
    private void addFeatures(ArrayList<Quad> qqq, Quad quad, String wkt) {
        Node geo = quad.getSubject();
        Gen2DFeatures.generate(qqq, geo, wkt);
        MajorMinor.add(qqq, geo, wkt);
    }

    private static String abbrevWkt(String wkt) {
        return (wkt != null && wkt.length() > 200) ? wkt.substring(0, 200) + "..." : wkt;
    }
}
