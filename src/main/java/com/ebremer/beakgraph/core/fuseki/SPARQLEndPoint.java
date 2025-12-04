package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import com.ebremer.beakgraph.turbo.Spatial;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.DatasetGraph;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * SPARQL Endpoint using Apache Jena Fuseki
 * @author erich
 */
public class SPARQLEndPoint {
    private static SPARQLEndPoint sep = null;
    private static FusekiServer server = null;

    private SPARQLEndPoint(Parameters params) throws IOException {
        System.out.println("Starting Fuseki SPARQL Endpoint...");
        Spatial.init();
        BeakGraph bg = BeakGraphPool.getPool().borrowObject(params.sparqlendpoint.toURI());
        Dataset ds = bg.getDataset();
        
        // Build and configure the Fuseki server
        server = FusekiServer.create()
                .add("/rdf", ds)  // SPARQL endpoint at /rdf
                .port(params.port)       // Server port
                .loopback(false)  // Allow external connections (not just localhost)
                .build();
        
        // Add custom /sparql endpoint for web page
        Server jettyServer = server.getJettyServer();
        ServletContextHandler context = (ServletContextHandler) jettyServer.getHandler();
        
        // Create servlet to serve the SPARQL web interface
        ServletHolder sparqlPageHolder = new ServletHolder("sparql-page", new SparqlWebPageServlet());
        context.addServlet(sparqlPageHolder, "/sparql");
        context.addServlet(sparqlPageHolder, "/sparql/*");

        server.start();
        
        System.out.println("Fuseki server started successfully!");
        System.out.println("SPARQL Web Interface: http://localhost:8888/sparql");
        System.out.println("SPARQL Query endpoint: http://localhost:8888/rdf/query");
        System.out.println("SPARQL Update endpoint: http://localhost:8888/rdf/update");
        System.out.println("Dataset endpoint: http://localhost:8888/rdf/data");
        System.out.println("SPARQL Graph Store: http://localhost:8888/rdf");
    }

    /**
     * Get the singleton SPARQL endpoint instance
     * @param params
     * @return SPARQLEndPoint instance
     * @throws java.io.IOException
     */
    public static SPARQLEndPoint getSPARQLEndPoint(Parameters params) throws IOException {
        if (sep == null) {
            sep = new SPARQLEndPoint(params);
        }
        return sep;
    }

    /**
     * Get the dataset for direct manipulation
     * @return DatasetGraph instance
     */
    public DatasetGraph getDataset() {
        return server.getDataAccessPointRegistry()
                .get("/rdf")
                .getDataService()
                .getDataset();
    }

    /**
     * Shutdown the Fuseki server
     */
    public void shutdown() {
        if (server != null) {
            System.out.println("Shutting down Fuseki server...");
            server.stop();
            System.out.println("Fuseki server stopped.");
        }
    }

    /**
     * Check if server is running
     * @return true if server is running
     */
    public boolean isRunning() {
        return server != null;
    }
    
    /**
     * Servlet to serve SPARQL web interface from resources
     */
    private static class SparqlWebPageServlet extends HttpServlet {
        
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) 
                throws IOException {
            
            String pathInfo = req.getPathInfo();
            String resourcePath;
            
            // Default to index.html if no specific file requested
            if (pathInfo == null || pathInfo.equals("/") || pathInfo.isEmpty()) {
                resourcePath = "/META-INF/sparql/index.html";
            } else {
                resourcePath = "/META-INF/sparql" + pathInfo;
            }
            
            // Try to load the resource
            InputStream is = getClass().getResourceAsStream(resourcePath);
            
            if (is == null) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND, 
                    "Resource not found: " + resourcePath);
                return;
            }
            
            // Set content type based on file extension
            String contentType = getContentType(resourcePath);
            resp.setContentType(contentType);
            
            // Stream the resource to response
            try (InputStream input = is; 
                 OutputStream output = resp.getOutputStream()) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = input.read(buffer)) != -1) {
                    output.write(buffer, 0, bytesRead);
                }
            }
        }
        
        private String getContentType(String path) {
            if (path.endsWith(".html") || path.endsWith(".htm")) {
                return "text/html";
            } else if (path.endsWith(".css")) {
                return "text/css";
            } else if (path.endsWith(".js")) {
                return "application/javascript";
            } else if (path.endsWith(".json")) {
                return "application/json";
            } else if (path.endsWith(".png")) {
                return "image/png";
            } else if (path.endsWith(".jpg") || path.endsWith(".jpeg")) {
                return "image/jpeg";
            } else if (path.endsWith(".svg")) {
                return "image/svg+xml";
            } else {
                return "application/octet-stream";
            }
        }
    }
}
