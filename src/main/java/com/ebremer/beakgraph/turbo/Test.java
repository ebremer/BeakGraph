package com.ebremer.beakgraph.turbo;

import com.ebremer.beakgraph.ng.BGDatasetGraph;
import com.ebremer.beakgraph.ng.BeakGraph;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.jena.sparql.pfunction.PropertyFunctionRegistry;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 *
 * @author erich
 */
public class Test {
    private static final Server server = new Server(8080);
    
    public static void main(String[] args) throws IOException {      
        File file = new File("D:\\tcga\\cvpr-data\\zip\\brca\\TCGA-E2-A1B1-01Z-00-DX1.7C8DF153-B09B-44C7-87B8-14591E319354.zip");
        BeakGraph bg = new BeakGraph(file.toURI());
        Dataset ds = DatasetFactory.wrap(new BGDatasetGraph(bg));
        Model mm = ModelFactory.createDefaultModel();
        mm.add(ds.getDefaultModel());
        try (FileOutputStream fos = new FileOutputStream("D:\\tcga\\cvpr-data\\yay.ttl")) {
            RDFDataMgr.write(fos, mm, Lang.TURTLE);
        }
        FunctionRegistry.get().put("https://www.ebremer.com/space/ns/sfWithin", Within.class);
        Triple ha;
        final PropertyFunctionRegistry reg = PropertyFunctionRegistry.chooseRegistry(ARQ.getContext());
        reg.put("https://www.ebremer.com/space/ns/sfWithin", Within.class);
        //reg.put("https://www.ebremer.com/space/ns/sfWithin", new ExamplePropertyFunctionFactory());
        PropertyFunctionRegistry.set(ARQ.getContext(), reg);
        
        Thread thread = new Thread(() -> {
            try {                
                ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
                context.setContextPath("/");
                context.addServlet(new ServletHolder(new SE()), "/hello");
                server.setHandler(context);
                server.start();
                server.join();        
            } catch (Exception ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        thread.start();
        try {
            Thread.sleep(0);
        } catch (InterruptedException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }
        //Dataset ds = DatasetFactory.create();
        
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
        """
            select ?feature ?area
            where {
                ?feature geo:hasGeometry ?geometry .
                ?geometry geo:asWKT ?wkt
                bind(hal:area(?wkt) as ?area
            } limit 100
        """
        );
        
        /*
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
        """
            select * #?gridType ?width ?height ?tileSizeX ?tileSizeY
            where {
                ?grid a ?GridType; exif:height ?height; exif:width ?width; hal:tileSizeX ?tileSizeX; hal:tileSizeY ?tileSizeY; hal:scale ?scale . 
                ?scale exif:width ?width; exif:height ?height; hal:scaleIndex ?index
            }
            limit 10
        """
        );*/
        
        /*
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select *           
            where {
               # service ?se {
                graph <https://www.ebremer.com/halcyon/ns/grid/0/26/105> {
                    ?feature geo:hasGeometry ?geometry .
                    #?geometry hal:asHilbert ?hilbert . #; geo:asWKT ?wkt .
                    #?hilbert hal:hasRange ?range .
                    #?range hal:low ?low; hal:high ?high
                    ?geometry spc:sfWithin ?o
                }
                #values (?se) { (<http://localhost:8080/hello>) (<http://localhost:8080/hello>) (<http://localhost:8080/hello>) (<http://localhost:8080/hello>) (<http://localhost:8080/hello>)}
            } order by ?low ?high
            """
        );
*/
        pss.setNsPrefix("hal", "https://www.ebremer.com/halcyon/ns/");
        pss.setNsPrefix("spc", "https://www.ebremer.com/space/ns/");
        pss.setNsPrefix("exif", "http://www.w3.org/2003/12/exif/ns#");
        pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
        pss.setNsPrefix("xmls", "http://www.w3.org/2001/XMLSchema#");
        pss.setNsPrefix("prov", "http://www.w3.org/ns/prov#");
      //  pss.setIri("feature", "_:h2147052816");
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),ds);
        ResultSet rs = qe.execSelect();
        ResultSetFormatter.out(System.out, rs);
        System.out.println("Done.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (server != null && server.isStarted()) {
                try {
                    server.stop();
                    System.out.println("Jetty server stopped.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));
        System.exit(0);
}
}
