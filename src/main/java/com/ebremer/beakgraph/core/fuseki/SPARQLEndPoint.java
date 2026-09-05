package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.beakgraph.lws.LWSMetadataRefresher;
import com.ebremer.beakgraph.turbo.Spatial;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sys.JenaSystem;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.file.*;
import java.util.Collections;
import java.util.Locale;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;

public class SPARQLEndPoint {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLEndPoint.class);
    private static SPARQLEndPoint sep = null;
    private static FusekiServer server = null;
    private static String BASE_URL;
    private Model lwsModel;
    private Path storageRoot = null;
    /** Directory mode only: keeps the served metadata in step with the files on disk. */
    private LWSMetadataRefresher refresher;
    private BeakGraph singleFileGraph;
    private final Dataset dataset;

    static {
        JenaSystem.init();
        Spatial.init();
        JenaShaper.init();
    }

    private SPARQLEndPoint(Parameters params) throws Exception {
        logger.info("Starting Fuseki SPARQL Endpoint...");

        Path endpointPath = params.sparqlendpoint.toPath().normalize().toAbsolutePath();
        Dataset ds;

        if (Files.isDirectory(endpointPath)) {
            logger.info("Directory mode (LWS) – metadata becomes default graph");
            storageRoot = endpointPath;
            Path ttlGzFile = endpointPath.resolve(LWSMetadataGenerator.CACHE_FILE_NAME);
            if (Files.exists(ttlGzFile)) {
                try (InputStream is = new GZIPInputStream(Files.newInputStream(ttlGzFile))) {
                    lwsModel = ModelFactory.createDefaultModel();
                    RDFDataMgr.read(lwsModel, is, RDFFormat.TURTLE.getLang());
                    logger.info("Loaded LWS metadata from {}", ttlGzFile);
                } catch (Exception ex) {
                    logger.error("Failed to load " + LWSMetadataGenerator.CACHE_FILE_NAME, ex);
                    lwsModel = ModelFactory.createDefaultModel();
                }
            } else {
                logger.info("{} not found – generating LWS metadata from {}",
                        LWSMetadataGenerator.CACHE_FILE_NAME, endpointPath);
                try {
                    lwsModel = LWSMetadataGenerator.generateLWSModel(endpointPath);
                } catch (Exception ex) {
                    logger.error("Failed to generate LWS metadata for " + endpointPath, ex);
                    lwsModel = ModelFactory.createDefaultModel();
                }
                try {
                    LWSMetadataGenerator.writeModelToGZ(lwsModel, ttlGzFile);
                    logger.info("Generated and cached LWS metadata to {}", ttlGzFile);
                } catch (Exception ex) {
                    // A read-only served directory: serve the freshly generated
                    // model anyway (it used to be discarded for an EMPTY one,
                    // so every LWS path answered 404 with only a log line).
                    logger.warn("Serving the generated LWS metadata without caching it: cannot write {} ({})",
                            ttlGzFile, ex.toString());
                }
            }
            // The tree changes underneath a running server (files copied in,
            // replaced, deleted) and a cached beakgraph.ttl.gz may predate changes
            // made while the server was down. The refresher validates the cache
            // now, re-scans on a fixed interval, and is consulted whenever a request
            // names a path the snapshot does not know. Fuseki and the servlet both
            // read the CURRENT snapshot: the graph handed to Fuseki delegates every
            // operation to whatever model the refresher holds at that moment.
            refresher = new LWSMetadataRefresher(endpointPath, lwsModel);
            if (refresher.refreshIfChanged()) {
                logger.info("Cached LWS metadata was out of date with {}; regenerated", endpointPath);
            }
            long refreshSeconds = Long.getLong("beakgraph.lws.refresh.seconds", 30L);
            refresher.start(refreshSeconds);
            logger.info("LWS metadata refresh: {} plus on-demand checks for unknown paths",
                    refreshSeconds > 0 ? "every " + refreshSeconds + "s" : "periodic scan disabled");
            ds = DatasetFactory.wrap(DatasetGraphFactory.wrap(new CurrentModelGraph(refresher)));
        } else {
            logger.info("Single-file mode (HDF5)");
            // A single-file endpoint serves one graph for the whole server lifetime, so open
            // it directly and close it on shutdown - rather than borrowing it from the pool and
            // never returning it (which leaks the handle and permanently ties up a pool slot).
            singleFileGraph = BG.getBeakGraph(params.sparqlendpoint);
            ds = singleFileGraph.getDataset();

            Path parent = endpointPath.getParent();
            if (parent != null) {
                Path ttlGzFile = parent.resolve("beakgraph.ttl.gz");
                if (Files.exists(ttlGzFile)) {
                    try (InputStream is = new GZIPInputStream(Files.newInputStream(ttlGzFile))) {
                        lwsModel = ModelFactory.createDefaultModel();
                        RDFDataMgr.read(lwsModel, is, RDFFormat.TURTLE.getLang());
                        logger.info("Loaded LWS metadata from {}", ttlGzFile);
                    } catch (Exception ex) {
                        logger.error("Failed to load beakgraph.ttl.gz", ex);
                        lwsModel = ModelFactory.createDefaultModel();
                    }
                }
            }
            if (lwsModel == null) lwsModel = ModelFactory.createDefaultModel();
        }
        this.dataset = ds;

        boolean singleFile = !Files.isDirectory(endpointPath);
        var serverBuilder = FusekiServer.create()
                .addFilter("/*", new ProfileInterceptorFilter())
                .port(params.port)
                .loopback(false);
        if (!singleFile) {
            // Directory mode: /rdf serves the LWS metadata model (absolute IRIs).
            // Single-file mode registers /rdf below via HDF5SparqlServlet instead,
            // so document-relative IRIs in the HDF5 data get resolved.
            // READ-ONLY (allowUpdate=false): the two-argument add() registers
            // SPARQL Update and Graph Store PUT/POST/DELETE as well, and `ds` is
            // the very model LWSStorageServlet uses as its allow-list of servable
            // files - an unauthenticated client could have rewritten it. This
            // endpoint provides read access only, by design.
            serverBuilder.add("/rdf", ds, false);
        }
        server = serverBuilder.build();

        Server jettyServer = server.getJettyServer();
        ServletContextHandler context = (ServletContextHandler) jettyServer.getHandler();

        BASE_URL = "http://localhost:" + params.port + "/";
        // Advertised links and IRI resolution use the base each client actually
        // reaches the server on - derived per request, with a reverse proxy's
        // Forwarded / X-Forwarded-* headers honoured - unless -base pins a public
        // URL for a proxy that does not forward the original host. A fixed
        // localhost base sent every remote client's next/up/linkset links to its
        // own loopback while the server deliberately listens on all interfaces.
        String publicBase = publicBase(params.base);
        LWSStorageServlet.setBase(publicBase);
        LWSStorageServlet.honourForwardedHeaders(jettyServer);
        LWSStorageServlet.setStorageRoot(storageRoot);

        ServletHolder sparqlPageHolder = new ServletHolder("sparql-page", new SparqlWebPageServlet());
        context.addServlet(sparqlPageHolder, "/sparql");
        context.addServlet(sparqlPageHolder, "/sparql/*");

        ServletHolder lwsHolder = new ServletHolder("lws-storage",
                refresher != null ? new LWSStorageServlet(refresher) : new LWSStorageServlet(lwsModel));
        context.addServlet(lwsHolder, "/*");

        if (singleFile) {
            // Serve /rdf ourselves so document-relative IRIs in the HDF5 file are
            // resolved (against the file's own URI) instead of leaking out raw,
            // matching the behaviour of the LWS .h5 SPARQL path.
            // Resolution base is the SERVED URL, never the local file URI: resolving
            // stored-relative IRIs against endpointPath.toUri() sent every client
            // file:///<absolute-server-path>/... IRIs - full filesystem disclosure.
            ServletHolder rdfHolder = new ServletHolder("hdf5-sparql", new HDF5SparqlServlet(ds));
            context.addServlet(rdfHolder, "/rdf");
            context.addServlet(rdfHolder, "/rdf/*");
        }

        server.start();
        logger.info("Fuseki server started successfully!");
        logger.info("SPARQL: http://localhost:{}/rdf/query", params.port);
        logger.info("LWS: {}", BASE_URL);
        if (publicBase != null) {
            logger.info("Public base URL (from -base): {}", publicBase);
        } else {
            logger.info("Public base URL: derived from each request (Forwarded/X-Forwarded-* honoured; -base overrides)");
        }
    }

    /**
     * Normalizes a {@code -base} value: null or blank means "derive per request";
     * anything else must be an absolute http(s) URL and is returned ending with '/'.
     */
    static String publicBase(String configured) {
        if (configured == null || configured.isBlank()) {
            return null;
        }
        String b = configured.strip();
        java.net.URI u;
        try {
            u = new java.net.URI(b);
        } catch (java.net.URISyntaxException e) {
            throw new IllegalArgumentException("-base is not a valid URL: " + configured, e);
        }
        String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase(Locale.ROOT);
        if (!(scheme.equals("http") || scheme.equals("https")) || u.getHost() == null) {
            throw new IllegalArgumentException("-base must be an absolute http(s) URL such as https://data.example.org/ (got " + configured + ")");
        }
        return b.endsWith("/") ? b : b + "/";
    }

    // synchronized so the lazy init is atomic: two concurrent callers must not each build
    // a SPARQLEndPoint (which would start two Fuseki servers on the same port and fail).
    public static synchronized SPARQLEndPoint getSPARQLEndPoint(Parameters params) throws Exception {
        if (sep == null) {
            sep = new SPARQLEndPoint(params);
        }
        return sep;
    }

    public DatasetGraph getDataset() {
        // The dataset this endpoint serves, held directly: the Fuseki registry
        // only knows "/rdf" in directory mode (single-file mode serves /rdf via
        // its own servlet), so the old registry lookup NPE'd in single-file mode.
        return dataset.asDatasetGraph();
    }

    public void shutdown() {
        if (server != null) server.stop();
        if (refresher != null) {
            refresher.close();
            refresher = null;
        }
        if (singleFileGraph != null) {
            singleFileGraph.close();
            singleFileGraph = null;
        }
    }

    /** Graph view that delegates every operation to the refresher's current metadata snapshot. */
    private static final class CurrentModelGraph extends GraphWrapper {
        private final LWSMetadataRefresher refresher;

        CurrentModelGraph(LWSMetadataRefresher refresher) {
            super(refresher.current().getGraph());
            this.refresher = refresher;
        }

        @Override
        public Graph get() {
            return refresher.current().getGraph();
        }
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
            String resourcePath;
            if (pathInfo == null || pathInfo.equals("/") || pathInfo.isEmpty()) {
                resourcePath = "/META-INF/sparql/index.html";
            } else if (pathInfo.equals("/beakgraph.png")) {
                // The logo ships exactly once, at the classpath root - an identical
                // copy under META-INF/sparql used to double the jar by 1.6 MB.
                resourcePath = "/beakgraph.png";
            } else {
                // pathInfo is attacker-influenced and already URL-decoded: reject
                // dot-segments (and backslashes) before splicing it into a classpath
                // lookup, so an encoded "/../.." can never escape /META-INF/sparql -
                // regardless of the container's URI-compliance mode or whether the
                // classpath is a jar or exploded directories.
                if (pathInfo.contains("..") || pathInfo.indexOf('\\') >= 0) {
                    resp.sendError(HttpServletResponse.SC_NOT_FOUND);
                    return;
                }
                resourcePath = "/META-INF/sparql" + pathInfo;
            }
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
            if (path.endsWith(".png")) return "image/png";
            return "application/octet-stream";
        }
    }

    /**
     * SPARQL endpoint for single-file (HDF5) mode. Runs queries through
     * {@link BGSparqlService} so document-relative IRIs are resolved against the
     * .h5 file's own URI, instead of Fuseki serving the dataset raw.
     */
    private static class HDF5SparqlServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;
        private final transient Dataset ds;

        HDF5SparqlServlet(Dataset ds) {
            this.ds = ds;
        }

        @Override protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            handle(req, resp);
        }

        @Override protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            handle(req, resp);
        }

        private void handle(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String queryStr;
            try {
                queryStr = BGSparqlService.extractQuery(req);
            } catch (BGSparqlService.QueryBodyTooLargeException e) {
                resp.sendError(413, e.getMessage());
                return;
            }
            if (queryStr == null || queryStr.isBlank()) {
                resp.sendError(400, "No SPARQL query provided");
                return;
            }
            // The resolution base follows the request (or -base): /rdf and
            // /rdf/query both resolve against <live base>/rdf, matching the LWS
            // path, so result IRIs are dereferenceable from wherever the client is.
            BGSparqlService.execute(ds, queryStr, LWSStorageServlet.liveBase(req) + "rdf",
                    req.getHeader("Accept"), resp, BGSparqlService.extractDatasetDescription(req));
        }
    }
}
