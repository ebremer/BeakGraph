package com.ebremer.beakgraph.tests;

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

public class Test2 {

    public static void main(String[] args) {
        JenaSystem.init();
        Spatial.init();

        File dest = new File("D:\\beakgraph\\dest\\compressed\\TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.h5");
        File dumpDir = new File("D:\\beakgraph\\dump_random");
        if (!dumpDir.exists()) dumpDir.mkdirs();

        int tileSize = 255;
        int imageWidth = 110000;
        int imageHeight = 90000;
        int numTilesToTest = 1000; // Increased for long-running benchmark

        // Metrics Tracking
        long totalStartTime = System.currentTimeMillis();
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        long totalProcessingTimeNs = 0;

        Random rand = new Random();

        try (HDF5Reader reader = new HDF5Reader(dest)) {
            BeakGraph bg = new BeakGraph(reader);
            Dataset ds = bg.getDataset();

            System.out.println("Starting benchmark for " + numTilesToTest + " random tiles...");

            for (int i = 1; i <= numTilesToTest; i++) {
                int x = rand.nextInt(imageWidth - tileSize);
                int y = rand.nextInt(imageHeight - tileSize);
                
                long tileStart = System.nanoTime();
                processTile(ds, x, y, tileSize, tileSize, dumpDir);
                long duration = System.nanoTime() - tileStart;

                // Update Stats
                totalProcessingTimeNs += duration;
                minTime = Math.min(minTime, duration);
                maxTime = Math.max(maxTime, duration);

                // Show statistics every 1000 tiles
                if (i % 100 == 0) {
                    printStats(i, totalProcessingTimeNs, minTime, maxTime, "CURRENT PROGRESS");
                }
            }

            printStats(numTilesToTest, totalProcessingTimeNs, minTime, maxTime, "FINAL RESULTS");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Formats and prints the performance metrics to the console.
     */
    private static void printStats(int count, long totalNs, long minNs, long maxNs, String title) {
        double avgMs = (totalNs / (double) count) / 1_000_000.0;
        double minMs = minNs / 1_000_000.0;
        double maxMs = maxNs / 1_000_000.0;

        System.out.println("\n--- " + title + " (" + count + " tiles) ---");
        System.out.println(String.format("Average Time : %.2f ms", avgMs));
        System.out.println(String.format("Min Time     : %.2f ms", minMs));
        System.out.println(String.format("Max Time     : %.2f ms", maxMs));
        System.out.println("------------------------------------------");
    }

    private static void processTile(Dataset ds, int x, int y, int width, int height, File dumpDir) throws Exception {
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
        pss.setParam("polygon", ResourceFactory.createTypedLiteral(
            polygonWkt,
            TypeMapper.getInstance().getSafeTypeByName("http://www.opengis.net/ont/geosparql#wktLiteral")
        ));
        pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
        pss.setNsPrefix("geof", "http://www.opengis.net/def/function/geosparql/");

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
                File outputfile = new File(dumpDir, "rand-tile-" + x + "-" + y + ".png");
                ImageIO.write(bi, "png", outputfile);
            }
        }
    }
}