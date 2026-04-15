package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import com.ebremer.beakgraph.turbo.Spatial;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sys.JenaSystem;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.file.*;
import java.util.Collections;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;

public class SPARQLEndPoint {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLEndPoint.class);
    private static SPARQLEndPoint sep = null;
    private static FusekiServer server = null;
    private static String BASE_URL;
    private Model lwsModel;
    private Path storageRoot = null;

    static {
        JenaSystem.init();
        Spatial.init();
        JenaShaper.init();
    }

    private SPARQLEndPoint(Parameters params) throws Exception {
        System.out.println("Starting Fuseki SPARQL Endpoint...");

        Path endpointPath = params.sparqlendpoint.toPath().normalize().toAbsolutePath();
        Dataset ds;

        if (Files.isDirectory(endpointPath)) {
            System.out.println("Directory mode (LWS) – metadata becomes default graph");
            storageRoot = endpointPath;
            Path ttlGzFile = endpointPath.resolve("beakgraph.ttl.gz");
            if (Files.exists(ttlGzFile)) {
                try (InputStream is = new GZIPInputStream(Files.newInputStream(ttlGzFile))) {
                    lwsModel = ModelFactory.createDefaultModel();
                    RDFDataMgr.read(lwsModel, is, RDFFormat.TURTLE.getLang());
                    System.out.println("Loaded LWS metadata from " + ttlGzFile);
                } catch (Exception ex) {
                    logger.error("Failed to load beakgraph.ttl.gz", ex);
                    lwsModel = ModelFactory.createDefaultModel();
                }
            } else {
                logger.warn("beakgraph.ttl.gz not found – empty metadata");
                lwsModel = ModelFactory.createDefaultModel();
            }
            ds = DatasetFactory.create(lwsModel);
        } else {
            System.out.println("Single-file mode (HDF5)");
            BeakGraph bg = BeakGraphPool.getPool().borrowObject(params.sparqlendpoint.toURI());
            ds = bg.getDataset();

            Path parent = endpointPath.getParent();
            if (parent != null) {
                Path ttlGzFile = parent.resolve("beakgraph.ttl.gz");
                if (Files.exists(ttlGzFile)) {
                    try (InputStream is = new GZIPInputStream(Files.newInputStream(ttlGzFile))) {
                        lwsModel = ModelFactory.createDefaultModel();
                        RDFDataMgr.read(lwsModel, is, RDFFormat.TURTLE.getLang());
                        System.out.println("Loaded LWS metadata from " + ttlGzFile);
                    } catch (Exception ex) {
                        logger.error("Failed to load beakgraph.ttl.gz", ex);
                        lwsModel = ModelFactory.createDefaultModel();
                    }
                }
            }
            if (lwsModel == null) lwsModel = ModelFactory.createDefaultModel();
        }

        server = FusekiServer.create()
                .add("/rdf", ds)
                .addFilter("/*", new ProfileInterceptorFilter())
                .port(params.port)
                .loopback(false)
                .build();

        Server jettyServer = server.getJettyServer();
        ServletContextHandler context = (ServletContextHandler) jettyServer.getHandler();

        BASE_URL = "http://localhost:" + params.port + "/";
        LWSStorageServlet.setBase(BASE_URL);
        LWSStorageServlet.setStorageRoot(storageRoot);

        ServletHolder sparqlPageHolder = new ServletHolder("sparql-page", new SparqlWebPageServlet());
        context.addServlet(sparqlPageHolder, "/sparql");
        context.addServlet(sparqlPageHolder, "/sparql/*");

        ServletHolder lwsHolder = new ServletHolder("lws-storage", new LWSStorageServlet(lwsModel));
        context.addServlet(lwsHolder, "/*");

        server.start();
        System.out.println("Fuseki server started successfully!");
        System.out.println("SPARQL: http://localhost:" + params.port + "/rdf/query");
        System.out.println("LWS: " + BASE_URL);
    }

    public static SPARQLEndPoint getSPARQLEndPoint(Parameters params) throws Exception {
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

    private static class ProfileInterceptorFilter implements Filter {
        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
            HttpServletRequest req = (HttpServletRequest) request;
            String acceptHeader = req.getHeader("Accept");
            boolean isProfileRequested = acceptHeader != null && acceptHeader.contains("application/ld+json") && acceptHeader.contains("profile=user-profile");
            if (isProfileRequested) {
                JenaShaper.USE_PROFILE.set(true);
                HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(req) {
                    @Override public String getHeader(String name) { return "Accept".equalsIgnoreCase(name) ? "application/ld+json" : super.getHeader(name); }
                    @Override public Enumeration<String> getHeaders(String name) { return "Accept".equalsIgnoreCase(name) ? Collections.enumeration(Collections.singletonList("application/ld+json")) : super.getHeaders(name); }
                };
                try { chain.doFilter(wrapper, response); } finally { JenaShaper.USE_PROFILE.remove(); }
                return;
            }
            try { chain.doFilter(request, response); } finally { JenaShaper.USE_PROFILE.remove(); }
        }

        @Override public void init(FilterConfig filterConfig) {}
        @Override public void destroy() {}
    }

    private static class SparqlWebPageServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;

        @Override protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String pathInfo = req.getPathInfo();
            String resourcePath = (pathInfo == null || pathInfo.equals("/") || pathInfo.isEmpty()) ? "/META-INF/sparql/index.html" : "/META-INF/sparql" + pathInfo;
            InputStream is = getClass().getResourceAsStream(resourcePath);
            if (is == null) { resp.sendError(HttpServletResponse.SC_NOT_FOUND); return; }
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
