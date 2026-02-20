package com.ebremer.beakgraph.tests;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.GEO;
import com.ebremer.beakgraph.turbo.Spatial;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Random;
import javax.imageio.ImageIO;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sys.JenaSystem;

public class GridBenchmark {

    private static final int GRID_SIZE = 256; // Your defined grid tile size
    private static final int SCALE = 0;        // Highest resolution

    public static void main(String[] args) {
        JenaSystem.init();
        Spatial.init();

        File dest = new File("D:\\beakgraph\\dest\\compressed\\TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.h5");
        File dumpDir = new File("D:\\beakgraph\\dump_grid");
        if (!dumpDir.exists()) dumpDir.mkdirs();

        int imageWidth = 110000;
        int imageHeight = 90000;
        int numTilesToTest = 1000; 

        // Global Metrics
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        int hitCount = 0;
        long hitTotalNs = 0;
        int missCount = 0;
        long missTotalNs = 0;

        Random rand = new Random();

        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            Dataset ds = bg.getDataset();
            System.out.println("Starting Grid-Targeted Benchmark (Scale " + SCALE + ")...");

            for (int i = 1; i <= numTilesToTest; i++) {
                // Pick a random point and align it to your grid
                int gridX = rand.nextInt(imageWidth / GRID_SIZE);
                int gridY = rand.nextInt(imageHeight / GRID_SIZE);
                
                // Actual coordinate for WKT parsing later
                int worldX = gridX * GRID_SIZE;
                int worldY = gridY * GRID_SIZE;

                long tileStart = System.nanoTime();
                boolean wasHit = processGridTile(ds, SCALE, gridX, gridY, worldX, worldY, dumpDir);
                long duration = System.nanoTime() - tileStart;

                minTime = Math.min(minTime, duration);
                maxTime = Math.max(maxTime, duration);

                if (wasHit) {
                    hitCount++;
                    hitTotalNs += duration;
                } else {
                    missCount++;
                    missTotalNs += duration;
                }

                if (i % 100 == 0) {
                    printStats(i, hitCount, hitTotalNs, missCount, missTotalNs, minTime, maxTime, "PROGRESS");
                }
            }
            printStats(numTilesToTest, hitCount, hitTotalNs, missCount, missTotalNs, minTime, maxTime, "FINAL RESULTS");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static boolean processGridTile(Dataset ds, int scale, int gx, int gy, int wx, int wy, File dumpDir) throws Exception {
        // Construct the specific Named Graph URI
        String graphURI = String.format("urn:x-beakgraph:grid:%d:%d:%d", scale, gx, gy);
        
        // No GeoSPARQL functions needed - we are just dumping the contents of the bucket
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            SELECT ?wkt WHERE {
                #?geo geo:asWKT ?original
                GRAPH ?graph { ?geo <https://halcyon.is/ns/asWKT0> ?wkt }
            }
            """
        );
        pss.setIri("graph", graphURI);
        pss.setNsPrefix("geo", GEO.NS);
       // IO.println(pss.toString());
        try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
            ResultSet rs = qexec.execSelect();
            if (rs.hasNext()) {
                BufferedImage bi = new BufferedImage(GRID_SIZE, GRID_SIZE, BufferedImage.TYPE_INT_ARGB);
                Graphics2D g2d = bi.createGraphics();
                g2d.setColor(Color.BLUE); // Different color to distinguish from GeoSPARQL test

                while (rs.hasNext()) {
                    QuerySolution qs = rs.next();
                    String foundWkt = qs.getLiteral("wkt").getString();
                    // Assumes Test.parseWktToPolygon handles relative offsets correctly
                    try {
                        Polygon poly = Test.parseWktToPolygon(foundWkt, wx, wy);
                        g2d.drawPolygon(poly);
                    } catch (NumberFormatException ex) {
                        IO.println(graphURI+"  "+foundWkt);
                    }
                }    
                g2d.dispose();
                File outputfile = new File(dumpDir, "grid-" + scale + "-" + gx + "-" + gy + ".png");
                ImageIO.write(bi, "png", outputfile);
                return true;                 
            }
        }
        return false;
    }

    private static void printStats(int total, int hits, long hitNs, int misses, long missNs, long min, long max, String title) {
        double hitAvg = (hits > 0) ? (hitNs / (double) hits) / 1_000_000.0 : 0;
        double missAvg = (misses > 0) ? (missNs / (double) misses) / 1_000_000.0 : 0;
        System.out.println("\n--- " + title + " ---");
        System.out.println(String.format("Targeted Tiles: %d | Hits: %d | Misses: %d", total, hits, misses));
        System.out.println(String.format("Avg Hit: %.2f ms | Avg Miss: %.2f ms", hitAvg, missAvg));
        System.out.println(String.format("Min/Max: %.2f / %.2f ms", min/1e6, max/1e6));
    }
}