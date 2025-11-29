package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.EXIF;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
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
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SchemaDO;
import com.ebremer.beakgraph.utils.ImageTools;
import java.awt.Color;
import org.locationtech.jts.geom.Polygon;

/**
 *
 * @author Erich Bremer
 */
public class ImageTest {
    
    public static void main(String[] args) {
        File src = new File("D:\\data\\beakgraph\\dest\\dXX.h5");
        if (!src.exists()) {
            System.err.println("File not found: " + src.getAbsolutePath());
            return;
        }
        long start = System.nanoTime();
        try (HDF5Reader reader = new HDF5Reader(src)) {
            BeakGraph bg = new BeakGraph( reader, null, null );
            BGDatasetGraph dsg = new BGDatasetGraph(bg);
            Dataset ds = DatasetFactory.wrap(dsg);
            ds.listNames().forEachRemaining(ng->IO.println(ng));
            ParameterizedSparqlString  pss = new ParameterizedSparqlString(
                """
                select ?wkt
                where {
                    ?range hal:low ?low
                    filter ( ?low < 4000000 )
                    ?hilbert hal:hasRange ?range .
                    ?s hal:asHilbert0 ?hilbert .
                    ?s geo:asWKT ?wkt
                }
                limit 10
                """
            );
            ArrayList<Polygon> list = new ArrayList<>();
            int MaxX = Integer.MIN_VALUE;
            int MaxY = Integer.MIN_VALUE;
            int MinX = Integer.MAX_VALUE;
            int MinY = Integer.MAX_VALUE;
            ParameterizedSparqlString pss2 = new ParameterizedSparqlString(
                """
                select *
                where {
                    ?image a sdo:ImageObject .
                    ?image exif:width ?width .                   
                    ?image exif:height ?height .
                }
                limit 100
                """
            );
            ParameterizedSparqlString pss3 = new ParameterizedSparqlString(
                """
                select *
                where {
                    ?s hal:asHilbert8 ?hilbert .
                    ?hilbert hal:hasRange ?range .
                    ?range hal:low ?low; hal:high ?high
                }
                #order by ?low
                limit 100
                """
            );            
            
            pss = pss3;
            pss.setNsPrefix("hal", "https://halcyon.is/ns/");
            pss.setNsPrefix("rdfs", RDFS.getURI());
            pss.setNsPrefix("exif", EXIF.NS);
            pss.setNsPrefix("sdo", SchemaDO.NS);
            pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
            
            
            long end = System.nanoTime();
            double delta = end - start;
            delta = delta / 1000000d;
            IO.println("A : "+delta);
            
            IO.println("SPARQL:\n"+pss.toString());
            start = System.nanoTime();
            try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
                ResultSet results = qexec.execSelect();
                ResultSetFormatter.out(System.out, results);
                /*
                while (results.hasNext()) {
                    QuerySolution qs = results.next();
                    Polygon p = ImageTools.wktToPolygon(qs.get("wkt").asLiteral().getString()); 
                    MaxX = (int) Math.max(MaxX, p.getEnvelopeInternal().getMaxX());
                    MaxY = (int) Math.max(MaxY, p.getEnvelopeInternal().getMaxY());
                    MinX = (int) Math.min(MinX, p.getEnvelopeInternal().getMinX());
                    MinY = (int) Math.min(MinY, p.getEnvelopeInternal().getMinY());
                    list.add(p);
                }*/
                end = System.nanoTime();
                delta = end - start;
                delta = delta / 1000000d;
                IO.println("B : "+delta);
            }
            /*
            list.forEach(p->{
                IO.println(p);
            });
            BufferedImage image = new BufferedImage(MaxX-MinX, MaxY-MinY, BufferedImage.TYPE_INT_RGB);
            ImageTools.drawPolygonsOnImage(list, image, Color.red);
            File outputFile = new File("/data/wow.png");
            boolean success = ImageIO.write(image, "png", outputFile);
            */
        } catch (IOException ex) {
            System.getLogger(ImageTest.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } catch (Exception ex) {
            System.getLogger(ImageTest.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }
    
}
/*

                    #filter (?low <10000)
                    #?s hal:asHilbert1 ?hilbert .
                    #?hilbert hal:hasRange ?range 
                    
                    
                                        #filter ((?low >= 3391593900) && (?low <= 3391593999))
                                        #filter ((?low > 1847600570) && (?low <= 1847600572))
                                        #?range hal:high ?high
                                    
                                    #?member rdfs:member ?yay .
                                    #?yay a geo:Feature;
                                    #?s a geo:FeatureCollection;
                                    #    ?p ?o
                                    #?wow hal:classification ?classification
                                    #geo:hasGeometry [ geo:asWKT ?wkt ]
                                }
                                #order by ?low

*/