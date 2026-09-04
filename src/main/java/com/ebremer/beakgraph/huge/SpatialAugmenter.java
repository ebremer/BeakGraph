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
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spatial index + derived-feature quad generation for the huge writer: a
 * faithful port of the spatial half of
 * {@link com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder}
 * (addSpatial / addSpatialScales / addSpatialIndexCells / generateGridURNs /
 * addFeatures), sharing that class's public tuning constants so the two writers
 * index geometry identically. Stateless and thread-safe: instances of the JTS
 * readers/factories are created per call, matching the original.
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

    public SpatialAugmenter(boolean features) {
        this.features = features;
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
        String wkt = ImageTools.stripCrs(quad.getObject().getLiteralLexicalForm());
        if (features) {
            try {
                addFeatures(qqq, quad, wkt);
            } catch (Exception ex) {
                logger.warn("Failed to generate features for {}: {}", quad.getSubject(), ex.toString());
            }
        }
        try {
            Geometry g = new WKTReader().read(wkt);
            if (g.isEmpty()) {
                return qqq;
            }
            // Index EVERY polygonal part, and every non-areal member via its
            // expanded envelope (see the RAM writer for the full rationale).
            List<Polygon> parts = new ArrayList<>(ImageTools.wktToPolygons(wkt));
            GeometryFactory gf = new GeometryFactory();
            for (int i = 0; i < g.getNumGeometries(); i++) {
                Geometry member = g.getGeometryN(i);
                if (!(member instanceof Polygonal) && !member.isEmpty()) {
                    Envelope env = member.getEnvelopeInternal();
                    env.expandBy(0.5);
                    parts.add((Polygon) gf.toGeometry(env));
                }
            }
            for (Polygon part : parts) {
                addSpatialIndexCells(qqq, quad, part);
                addSpatialScales(qqq, quad, wkt, PolygonScaler.toPolygons(part));
            }
        } catch (Exception ex) {
            logger.warn("Skipping spatial indexing for {}: {} ({})",
                    quad.getSubject(), ex.toString(), abbrevWkt(wkt));
        }
        return qqq;
    }

    private void addSpatialScales(ArrayList<Quad> qqq, Quad quad, String wkt, Polygon[] scales) {
        if (scales == null) {
            return;
        }
        final String[] wktScales = PolygonScaler.toWKT(scales);
        for (int s = 0; s < Math.min(scales.length, asWKT.length); s++) {
            List<Node> tiles = generateGridURNs(scales[s], s);
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
    }

    private void addSpatialIndexCells(ArrayList<Quad> qqq, Quad quad, Polygon part) {
        Envelope env = part.getEnvelopeInternal();
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

    private List<Node> generateGridURNs(Polygon polygon, int resolutionLevel) {
        List<Node> intersectingURNs = new ArrayList<>();
        Envelope env = polygon.getEnvelopeInternal();
        double cellSize = Params.GRIDTILESIZE;
        long minTileX = (long) Math.floor(env.getMinX() / cellSize);
        long maxTileX = (long) Math.floor(env.getMaxX() / cellSize);
        long minTileY = (long) Math.floor(env.getMinY() / cellSize);
        long maxTileY = (long) Math.floor(env.getMaxY() / cellSize);
        GeometryFactory gf = polygon.getFactory();
        for (long x = minTileX; x <= maxTileX; x++) {
            double tileMinX = x * cellSize;
            double tileMaxX = tileMinX + cellSize;
            for (long y = minTileY; y <= maxTileY; y++) {
                double tileMinY = y * cellSize;
                double tileMaxY = tileMinY + cellSize;
                Envelope tileEnv = new Envelope(tileMinX, tileMaxX, tileMinY, tileMaxY);
                if (env.intersects(tileEnv)) {
                    Polygon tilePoly = (Polygon) gf.toGeometry(tileEnv);
                    if (polygon.intersects(tilePoly)) {
                        intersectingURNs.add(Params.gridGraph(resolutionLevel, x, y));
                    }
                }
            }
        }
        return intersectingURNs;
    }

    private void addFeatures(ArrayList<Quad> qqq, Quad quad, String wkt) {
        Node geo = quad.getSubject();
        Gen2DFeatures.generate(qqq, geo, wkt);
        MajorMinor.add(qqq, geo, wkt);
    }

    private static String abbrevWkt(String wkt) {
        return (wkt != null && wkt.length() > 200) ? wkt.substring(0, 200) + "..." : wkt;
    }
}
