package com.ebremer.beakgraph.tests;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.turbo.Spatial;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Random;
import javax.imageio.ImageIO;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sys.JenaSystem;

public class Test3 {

    public static void main(String[] args) {
        JenaSystem.init();
        Spatial.init();

        File dest = new File("D:\\beakgraph\\dest\\compressed\\TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.h5");
        File dumpDir = new File("D:\\beakgraph\\dump_random");
        if (!dumpDir.exists()) dumpDir.mkdirs();

        int tileSize = 255;
        int imageWidth = 110000;
        int imageHeight = 90000;
        int numTilesToTest = 1000; 

        int maxTilesX = imageWidth / tileSize;
        int maxTilesY = imageHeight / tileSize;

        // Global Metrics
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;

        // Hit Metrics (Tiles with data)
        int hitCount = 0;
        long hitTotalNs = 0;

        // Miss Metrics (Empty tiles)
        int missCount = 0;
        long missTotalNs = 0;

        Random rand = new Random();

        try ( BeakGraph bg = BG.getBeakGraph(dest) ) {
            Dataset ds = bg.getDataset();
            System.out.println("Starting Comparative Benchmark (Hits vs Misses)...");

            for (int i = 1; i <= numTilesToTest; i++) {
                int x = rand.nextInt(maxTilesX) * tileSize;
                int y = rand.nextInt(maxTilesY) * tileSize;
                
                long tileStart = System.nanoTime();
                boolean wasHit = processTile(ds, x, y, tileSize, tileSize, dumpDir);
                long duration = System.nanoTime() - tileStart;

                // Update Globals
                minTime = Math.min(minTime, duration);
                maxTime = Math.max(maxTime, duration);

                // Update Categorized Stats
                if (wasHit) {
                    hitCount++;
                    hitTotalNs += duration;
                } else {
                    missCount++;
                    missTotalNs += duration;
                }

                if (i % 100 == 0) {
                    printDetailedStats(i, hitCount, hitTotalNs, missCount, missTotalNs, minTime, maxTime, "CURRENT PROGRESS");
                }
            }

            printDetailedStats(numTilesToTest, hitCount, hitTotalNs, missCount, missTotalNs, minTime, maxTime, "FINAL RESULTS");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void printDetailedStats(int total, int hits, long hitNs, int misses, long missNs, long min, long max, String title) {
        double hitAvg = (hits > 0) ? (hitNs / (double) hits) / 1_000_000.0 : 0;
        double missAvg = (misses > 0) ? (missNs / (double) misses) / 1_000_000.0 : 0;
        double hitRate = (hits / (double) total) * 100.0;

        System.out.println("\n================ " + title + " ================");
        System.out.println(String.format("Total Tiles      : %d", total));
        System.out.println(String.format("Hit Rate         : %.2f%%", hitRate));
        System.out.println("--------------------------------------------------");
        System.out.println(String.format("AVG Time (HITS)  : %.2f ms (%d tiles)", hitAvg, hits));
        System.out.println(String.format("AVG Time (MISS)  : %.2f ms (%d tiles)", missAvg, misses));
        System.out.println("--------------------------------------------------");
        System.out.println(String.format("Global MIN Time  : %.2f ms", min / 1_000_000.0));
        System.out.println(String.format("Global MAX Time  : %.2f ms", max / 1_000_000.0));
        System.out.println("==================================================\n");
    }

    private static boolean processTile(Dataset ds, int x, int y, int width, int height, File dumpDir) throws Exception {
        String polygonWkt = Test.createPolygonString(x, y, width, height);
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select distinct ?geo ?wkt
            where {
                ?geo geo:asWKT ?wkt
                FILTER( geof:sfIntersects( ?wkt, ?polygon ) )
            }
            """
        );
        pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
        pss.setNsPrefix("geof", "http://www.opengis.net/def/function/geosparql/");
        pss.setParam("polygon", ResourceFactory.createTypedLiteral(
            polygonWkt,
            TypeMapper.getInstance().getSafeTypeByName("http://www.opengis.net/ont/geosparql#wktLiteral")
        ));

        try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
            ResultSet rs = qexec.execSelect();
            if (rs.hasNext()) {
                BufferedImage bi = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
                Graphics2D g2d = bi.createGraphics();
                g2d.setColor(Color.RED);
                g2d.setStroke(new BasicStroke(1));

                while (rs.hasNext()) {
                    QuerySolution qs = rs.next();
                    String foundWkt = qs.getLiteral("wkt").getString();
                    Polygon poly = Test.parseWktToPolygon(foundWkt, x, y);
                    g2d.drawPolygon(poly);
                }

                g2d.dispose();
                File outputfile = new File(dumpDir, "tile-" + x + "-" + y + ".png");
                ImageIO.write(bi, "png", outputfile);
                return true; 
            }
        }
        return false;
    }
}