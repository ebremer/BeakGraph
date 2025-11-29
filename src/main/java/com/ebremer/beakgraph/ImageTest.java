package com.ebremer.beakgraph;

import static com.ebremer.beakgraph.Params.GRIDTILESIZE;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.EXIF;
import com.ebremer.beakgraph.core.lib.GEOF;
import com.ebremer.beakgraph.core.lib.HAL;
import com.ebremer.beakgraph.hdf5.jena.NumScale;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.turbo.Spatial;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import javax.imageio.ImageIO;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SchemaDO;
import com.ebremer.beakgraph.utils.ImageTools;
import java.awt.Color;
import java.awt.Graphics2D;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

/**
 *
 * @author Erich Bremer
 */
public class ImageTest {
    
    public static Polygon createPolygon() {
        GeometryFactory GF = new GeometryFactory();
        Coordinate[] coords = {
            new Coordinate( 50,  50),
            new Coordinate(210,  60),
            new Coordinate(230, 140),
            new Coordinate(180, 220),
            new Coordinate( 90, 200),
            new Coordinate( 40, 130),
            new Coordinate( 50,  50)  // closing vertex (required by JTS)
        };

        LinearRing shell = GF.createLinearRing(coords);
        return GF.createPolygon(shell, null);  // no interior holes
    }
    
    public static void main(String[] args) {
        Spatial.init();
        File src = new File("/beakgraph/dest/dXX.h5");
        if (!src.exists()) {
            System.err.println("File not found: " + src.getAbsolutePath());
            return;
        }
        
        try (HDF5Reader reader = new HDF5Reader(src)) {
            BeakGraph bg = new BeakGraph( reader, null, null );
            BGDatasetGraph dsg = new BGDatasetGraph(bg);
            Dataset ds = DatasetFactory.wrap(dsg);
            //ds.listNames().forEachRemaining(ng->IO.println(ng));
            ArrayList<Polygon> list = new ArrayList<>();
            ParameterizedSparqlString pss = new ParameterizedSparqlString(
                """
                select *
                where {
                    ?image a sdo:ImageObject .
                    ?image exif:width ?width .                   
                    ?image exif:height ?height .
                }
                limit 10
                """
            );
            pss.setNsPrefix("exif", EXIF.NS);
            pss.setNsPrefix("sdo", SchemaDO.NS);
            int width = 0;
            int height = 0;
            try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
                ResultSet results = qexec.execSelect();
                if (results.hasNext()) {
                    QuerySolution qs = results.next();
                    width = qs.get("width").asLiteral().getInt();
                    height = qs.get("height").asLiteral().getInt();
                }
            }
            
            
            ParameterizedSparqlString pss3 = new ParameterizedSparqlString(
                """
                select distinct ?geo ?wkt
                where {
                    graph ?spatial {
                        ?range hal:low0 ?low .
                        ?hilbert hal:hasRange0 ?range .
                        ?geo hal:asHilbert0 ?hilbert .
                        filter (?low >= 15324676096)
                        filter (?low <= 15324741633)
                    }
                    ?geo geo:asWKT ?wkt
                }
                #limit 10
                """
            );            
            int a = 256;
            int b = 128;
            a = 312;
            b = 99;
            int scale = 10;
            int offX = GRIDTILESIZE*a;
            int offY = GRIDTILESIZE*b;
            int size = 255;
            int numscales = NumScale.calculateScaleStepsLog(width, height, size);
            IO.println("OFFSET : "+offX+" "+offY+" "+(offX+size)+" "+(offY+size));
            
            long start = System.nanoTime();
            ParameterizedSparqlString pss4 = new ParameterizedSparqlString(
                """
                select *
                where {
                    graph ?spatial {
                        <urn:x-beakgraph:Spatial/0/?a/?b/> rdfs:member ?geo .
                    }
                    ?geo geo:asWKT ?wkt
                }
                limit 100
                """
            ); 
            pss4.setLiteral("a", a);
            pss4.setLiteral("b", b);
            
            ParameterizedSparqlString pss5 = new ParameterizedSparqlString(
                """
                select *
                where {
                    ?geo geo:asWKT ?wkt
                    FILTER( geof:sfIntersects(?wkt, ?poly^^geo:wktLiteral) )
                }
                """
            );
            
            String poly = String.format("POLYGON((%d %d, %d %d, %d %d, %d %d, %d %d))", offX, offY, offX+size, offY, offX+size, offY+size, offX, offY+size, offX, offY);
            pss5.setLiteral("poly", poly);
            IO.println(pss5.toString());
//#BIND( geo:buffer(?tGeom, 10) AS ?targetBuff ) 

            pss = pss5;
            pss.setNsPrefix("hal", "https://halcyon.is/ns/");
            pss.setNsPrefix("rdfs", RDFS.getURI());
            pss.setNsPrefix("exif", EXIF.NS);
            pss.setNsPrefix("sdo", SchemaDO.NS);
            pss.setNsPrefix("geof", GEOF.NS);
            pss.setNsPrefix("uom", HAL.NS);
            //pss.setNsPrefix("sf", "http://www.opengis.net/def/function/sf/");
            pss.setIri("spatial", Params.SPATIALSTRING);
            pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");

            start = System.nanoTime();            
            try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
                ResultSet results = qexec.execSelect();
                //ResultSetFormatter.out(System.out, results);
                while (results.hasNext()) {
                    QuerySolution qs = results.next();
                    Polygon p = ImageTools.wktToPolygon(qs.get("wkt").asLiteral().getString()); 
                    list.add(p);
                }
                long end = System.nanoTime();
                double delta = end - start;
                delta = delta / 1_000_000d;
                IO.println("FINISH : "+delta);
                System.out.println("POLYGONS FOUND : "+list.size());
            }
            BufferedImage image = new BufferedImage(scale*GRIDTILESIZE, scale*GRIDTILESIZE, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = image.createGraphics();
            g2d.setColor(Color.WHITE);
            g2d.fillRect(0, 0, scale*GRIDTILESIZE, scale*GRIDTILESIZE);
            g2d.dispose();
            ImageTools.drawPolygonsOnImage(list, image, Color.red, offX, offY);
            File outputFile = new File("/beakgraph/wow.png");
            ImageIO.write(image, "png", outputFile);
        } catch (IOException ex) {
            System.getLogger(ImageTest.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } catch (Exception ex) {
            System.getLogger(ImageTest.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }    
}
