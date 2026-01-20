package com.ebremer.beakgraph.tests;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.turbo.Spatial;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

public class Test {

    public static String createPolygonString(int x, int y, int width, int height) {
        return String.format("POLYGON((%d %d, %d %d, %d %d, %d %d, %d %d))",
            x, y, 
            x + width, y, 
            x + width, y + height, 
            x, y + height, 
            x, y
        );
    }

    /**
     * Simple helper to extract coordinates from WKT POLYGON strings
     * and convert them to relative tile coordinates.
     * @param wkt
     * @param offsetX
     * @param offsetY
     * @return 
     */
    public static Polygon parseWktToPolygon(String wkt, int offsetX, int offsetY) {
        Pattern pattern = Pattern.compile("(\\d+)\\s+(\\d+)");
        Matcher matcher = pattern.matcher(wkt);
        List<Integer> xPoints = new ArrayList<>();
        List<Integer> yPoints = new ArrayList<>();
        
        while (matcher.find()) {
            xPoints.add(Integer.parseInt(matcher.group(1)) - offsetX);
            yPoints.add(Integer.parseInt(matcher.group(2)) - offsetY);
        }
        
        return new Polygon(
            xPoints.stream().mapToInt(i -> i).toArray(),
            yPoints.stream().mapToInt(i -> i).toArray(),
            xPoints.size()
        );
    }

    public static void main(String[] args) {
        JenaSystem.init();
        Spatial.init();
        int ccc = 0;
        File dest = new File("D:\\beakgraph\\dest\\compressed\\TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.h5");
        File dumpDir = new File("D:\\beakgraph\\dump");
        if (!dumpDir.exists()) dumpDir.mkdirs();
        int tileSize = 255;
        int imageWidth = 110000;
        int imageHeight = 90000;
        try (HDF5Reader reader = new HDF5Reader(dest)) {
            BeakGraph bg = new BeakGraph(reader);
            Dataset ds = bg.getDataset();
            for (int y = 0; y < imageHeight; y += tileSize) {
                IO.println("Y : " + y);
                for (int x = 0; x < imageWidth; x += tileSize) {
                  //  IO.println("X : " + x);
                    int width = Math.min(tileSize, imageWidth - x);
                    int height = Math.min(tileSize, imageHeight - y);
                    String polygonWkt = createPolygonString(x, y, width, height);
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
                   // IO.println(pss.toString());                          
                    try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
                        ResultSet rs = qexec.execSelect();
                        if (rs.hasNext()) {
                           // IO.println("Polygons Detected!");
                            BufferedImage bi = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
                            Graphics2D g2d = bi.createGraphics();
                            
                            // Optional: Fill background or keep transparent
                            // g2d.setColor(Color.WHITE);
                            // g2d.fillRect(0, 0, width, height);

                            g2d.setColor(Color.RED); // Color for the geometries
                            g2d.setStroke(new BasicStroke(1));

                            int count = 0;
                            while (rs.hasNext()) {
                                QuerySolution qs = rs.next();
                                String foundWkt = qs.getLiteral("wkt").getString();
                                
                                // Parse and Draw the polygon
                                Polygon poly = parseWktToPolygon(foundWkt, x, y);
                                g2d.drawPolygon(poly); 
                                // use g2d.fillPolygon(poly) if you want solid shapes
                                
                                count++;
                            }

                            g2d.dispose();
                            File outputfile = new File(dumpDir, "image-" + x + "-" + y + ".png");
                            ImageIO.write(bi, "png", outputfile);
                            
//                            System.out.println("Saved tile: " + outputfile.getName() + " (" + count + " shapes)");
                        }
                        
                        ccc++;
                        if ((ccc % 100) == 0) System.out.println("Tiles processed: " + ccc);
                    } catch (Exception ex) {
                       ex.printStackTrace();
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }
}