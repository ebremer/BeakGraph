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
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.jena.sys.JenaSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPARQLEndPoint {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLEndPoint.class);
    private static SPARQLEndPoint sep = null;
    private static FusekiServer server = null;
    
    static {
        JenaSystem.init();
        Spatial.init();
        JenaShaper.init();
    }    

    private SPARQLEndPoint(Parameters params) throws IOException {
        System.out.println("Starting Fuseki SPARQL Endpoint...");
        BeakGraph bg;
        try {
            bg = BeakGraphPool.getPool().borrowObject(params.sparqlendpoint.toURI());
            Dataset ds = bg.getDataset();
            
            // Build and configure the Fuseki server
            server = FusekiServer.create()
                .add("/rdf", ds)
                // Add the filter to the Fuseki Builder
                .addFilter("/*", new ProfileInterceptorFilter())
                .port(params.port)
                .loopback(false)
                .build();
                
            Server jettyServer = server.getJettyServer();
            ServletContextHandler context = (ServletContextHandler) jettyServer.getHandler();

            ServletHolder sparqlPageHolder = new ServletHolder("sparql-page", new SparqlWebPageServlet());
            context.addServlet(sparqlPageHolder, "/sparql");
            context.addServlet(sparqlPageHolder, "/sparql/*");
            
            server.start();        
            System.out.println("Fuseki server started successfully!");
            System.out.println("SPARQL Query endpoint: http://localhost:" + params.port + "/rdf/query");
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    public static SPARQLEndPoint getSPARQLEndPoint(Parameters params) throws IOException {
        if (sep == null) sep = new SPARQLEndPoint(params);
        return sep;
    }

    public DatasetGraph getDataset() {
        return server.getDataAccessPointRegistry().get("/rdf").getDataService().getDataset();
    }

    public void shutdown() {
        if (server != null) server.stop();
    }

    public boolean isRunning() { return server != null; }
    
// ========================================================================
    // JETTY THREADLOCAL INTERCEPTOR
    // ========================================================================
    private static class ProfileInterceptorFilter implements Filter {
        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) 
                throws IOException, ServletException {
            
            HttpServletRequest req = (HttpServletRequest) request;
            String acceptHeader = req.getHeader("Accept");

            boolean isProfileRequested = acceptHeader != null 
                    && acceptHeader.contains("application/ld+json") 
                    && acceptHeader.contains("profile=user-profile");

            if (isProfileRequested) {
                System.out.println("Jetty Filter: Profile requested! Activating Frame.");
                
                // Flip the secret switch for this specific HTTP thread
                JenaShaper.USE_PROFILE.set(true);
                
                // Strip the profile from the header so Fuseki handles it effortlessly
                HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(req) {
                    @Override
                    public String getHeader(String name) {
                        if ("Accept".equalsIgnoreCase(name)) return "application/ld+json";
                        return super.getHeader(name);
                    }
                    @Override
                    public Enumeration<String> getHeaders(String name) {
                        if ("Accept".equalsIgnoreCase(name)) {
                            return Collections.enumeration(Collections.singletonList("application/ld+json"));
                        }
                        return super.getHeaders(name);
                    }
                };
                
                try {
                    chain.doFilter(wrapper, response);
                } finally {
                    // MUST clean up the thread switch after response is sent!
                    JenaShaper.USE_PROFILE.remove();
                }
                return;
            }

            // Normal requests flow through cleanly
            try {
                chain.doFilter(request, response);
            } finally {
                JenaShaper.USE_PROFILE.remove();
            }
        }
    }

    // ========================================================================
    // SPARQL WEB INTERFACE SERVLET
    // ========================================================================
    private static class SparqlWebPageServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {            
            String pathInfo = req.getPathInfo();
            String resourcePath = (pathInfo == null || pathInfo.equals("/") || pathInfo.isEmpty()) 
                    ? "/META-INF/sparql/index.html" : "/META-INF/sparql" + pathInfo;            
            
            InputStream is = getClass().getResourceAsStream(resourcePath);            
            if (is == null) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Resource not found: " + resourcePath);
                return;
            }            
            
            resp.setContentType(getContentType(resourcePath));            
            try (InputStream input = is; OutputStream output = resp.getOutputStream()) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = input.read(buffer)) != -1) output.write(buffer, 0, bytesRead);
            }
        }
        
        private String getContentType(String path) {
            if (path.endsWith(".html") || path.endsWith(".htm")) return "text/html";
            if (path.endsWith(".css")) return "text/css";
            if (path.endsWith(".js")) return "application/javascript";
            if (path.endsWith(".json")) return "application/json";
            return "application/octet-stream";
        }
    }
}