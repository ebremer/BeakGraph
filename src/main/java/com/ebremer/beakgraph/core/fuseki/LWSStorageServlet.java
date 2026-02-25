package com.ebremer.beakgraph.core.fuseki;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import jakarta.servlet.http.*;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
public class LWSStorageServlet extends HttpServlet {
    private static String BASE;
    private static Path STORAGE_ROOT;
    private final Model MODEL;
    private static final String HTTP_ROOT = "http://localhost:8888/HalcyonStorage";
    private static final Resource LWS_CONTAINER = ResourceFactory.createResource("https://www.w3.org/ns/lws#Container");
    private static final Property LWS_ITEMS = ResourceFactory.createProperty("https://www.w3.org/ns/lws#items");
    private static final Property AS_MEDIA_TYPE = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#mediaType");
    private static final Property SCHEMA_SIZE = ResourceFactory.createProperty("https://schema.org/size");
    private static final Property AS_UPDATED = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#updated");
    public LWSStorageServlet(Model model) {
        this.MODEL = model;
    }
    public static void setBase(String b) {
        BASE = b;
    }
 
    public static void setStorageRoot(Path root) {
        STORAGE_ROOT = root;
    }
    private boolean isHDF5(Path file) {
        String name = file.getFileName().toString().toLowerCase();
        return name.endsWith(".h5");
    }
    private boolean isSparqlRequest(HttpServletRequest req) {
        String method = req.getMethod();
        String ct = req.getContentType();
        if ("POST".equals(method) && ct != null && ct.toLowerCase().startsWith("application/sparql-query")) return true;
        return "GET".equals(method) && req.getParameter("query") != null;
    }
    private void handleSparqlQuery(HttpServletRequest req, HttpServletResponse resp, Path h5File) throws IOException {
        String queryStr = null;
        if ("GET".equals(req.getMethod())) {
            queryStr = req.getParameter("query");
        } else if ("POST".equals(req.getMethod())) {
            try (InputStream in = req.getInputStream()) {
                queryStr = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        if (queryStr == null || queryStr.isBlank()) {
            resp.sendError(400, "No SPARQL query provided");
            return;
        }
        BeakGraph bg = null;
        URI fileUri = h5File.toUri();
        try {
            Query query = QueryFactory.create(queryStr);
            bg = BeakGraphPool.getPool().borrowObject(fileUri);
            Dataset ds = bg.getDataset();
            try (QueryExecution qexec = QueryExecution.dataset(ds).query(query).build()) {
                String accept = req.getHeader("Accept") != null ? req.getHeader("Accept").toLowerCase() : "";
                if (query.isSelectType()) {
                    ResultSet rs = qexec.execSelect();
                    if (accept.contains("json")) {
                        resp.setContentType("application/sparql-results+json");
                        ResultSetFormatter.outputAsJSON(resp.getOutputStream(), rs);
                    } else if (accept.contains("csv")) {
                        resp.setContentType("text/csv");
                        ResultSetFormatter.outputAsCSV(resp.getOutputStream(), rs);
                    } else {
                        resp.setContentType("application/sparql-results+xml");
                        ResultSetFormatter.outputAsXML(resp.getOutputStream(), rs);
                    }
                } else if (query.isAskType()) {
                    boolean b = qexec.execAsk();
                    resp.setContentType("application/sparql-results+json");
                    resp.getWriter().write("{\"boolean\":" + b + "}");
                } else if (query.isConstructType() || query.isDescribeType()) {
                    Model m = query.isConstructType() ? qexec.execConstruct() : qexec.execDescribe();
                    if (accept.contains("json")) {
                        resp.setContentType("application/ld+json");
                        RDFDataMgr.write(resp.getOutputStream(), m, RDFFormat.JSONLD);
                    } else if (accept.contains("turtle")) {
                        resp.setContentType("text/turtle");
                        RDFDataMgr.write(resp.getOutputStream(), m, RDFFormat.TURTLE);
                    } else {
                        resp.setContentType("application/rdf+xml");
                        RDFDataMgr.write(resp.getOutputStream(), m, RDFFormat.RDFXML);
                    }
                }
            }
        } catch (Exception ex) {
            resp.sendError(400, "Query error: " + ex.getMessage());
        } finally {
            if (bg != null) BeakGraphPool.getPool().returnObject(fileUri, bg);
        }
    }
    private String getParentURI(String resourceURI) {
        if (resourceURI.equals(HTTP_ROOT) || resourceURI.equals(HTTP_ROOT + "/")) return null;
        int lastSlash = resourceURI.lastIndexOf('/');
        if (lastSlash > HTTP_ROOT.length() - 1) return resourceURI.substring(0, lastSlash);
        return HTTP_ROOT;
    }
    private String getLinksetURI(String resourceURI) {
        return resourceURI.endsWith("/") ? resourceURI.substring(0, resourceURI.length()-1) + ".meta" : resourceURI + ".meta";
    }
    private void serveLinkset(HttpServletResponse resp, String resourceURI) throws IOException {
        Resource r = MODEL.getResource(resourceURI);
        if (!MODEL.containsResource(r)) {
            resp.sendError(404, "Resource not found: " + resourceURI);
            return;
        }
        String typeHref = r.hasProperty(RDF.type, LWS_CONTAINER)
            ? "https://www.w3.org/ns/lws#Container"
            : "https://www.w3.org/ns/lws#DataResource";
        String relations = "";
        String parent = getParentURI(resourceURI);
        if (parent != null) {
            relations += """
                "up": [{"href": "%s"}],
                """.formatted(parent);
        }
        if (r.hasProperty(AS_MEDIA_TYPE)) {
            String media = r.getProperty(AS_MEDIA_TYPE).getString().replace("\\","\\\\").replace("\"","\\\"");
            relations += """
                "mediaType": [{"href": "%s"}],
                """.formatted(media);
        }
        if (r.hasProperty(SCHEMA_SIZE)) {
            String size = r.getProperty(SCHEMA_SIZE).getString();
            relations += """
                "size": [{"href": "%s"}],
                """.formatted(size);
        }
        if (r.hasProperty(AS_UPDATED)) {
            String updated = r.getProperty(AS_UPDATED).getString();
            relations += """
                "updated": [{"href": "%s"}],
                """.formatted(updated);
        }
        if (relations.endsWith(",\n")) relations = relations.substring(0, relations.length() - 2);
        String json = """
            {
              "linkset": [
                {
                  "anchor": "%s",
                  "type": [{"href": "%s"}]%s
                }
              ]
            }
            """.formatted(resourceURI, typeHref, relations.isEmpty() ? "" : ",\n" + relations);
        resp.setContentType("application/linkset+json");
        resp.setCharacterEncoding("UTF-8");
        resp.getWriter().write(json);
    }
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String reqPath = req.getRequestURI();
        if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        if (reqPath.endsWith("/")) reqPath = reqPath.substring(0, reqPath.length()-1);
        if (reqPath.endsWith(".meta")) {
            String basePath = reqPath.substring(0, reqPath.length() - 5);
            if (basePath.startsWith("HalcyonStorage")) {
                basePath = basePath.substring("HalcyonStorage".length());
                if (basePath.startsWith("/")) basePath = basePath.substring(1);
            }
            String baseResourceURI = basePath.isEmpty() ? HTTP_ROOT : HTTP_ROOT + "/" + basePath;
            serveLinkset(resp, baseResourceURI);
            return;
        }
        if (reqPath.equals("description")) {
            resp.setContentType("application/ld+json");
            resp.setHeader("Link", "<" + BASE + "description>; rel=\"storageDescription\"");
            resp.setHeader("Link", "<" + getLinksetURI(BASE + "description") + ">; rel=\"linkset\"; type=\"application/linkset+json\"");
            resp.setHeader("Vary", "Accept");
            resp.getWriter().write("{\"@context\":\"https://www.w3.org/ns/lws/v1\",\"@id\":\"" + BASE + "\",\"type\":\"Storage\",\"service\":[{\"type\":\"StorageDescription\",\"serviceEndpoint\":\"" + BASE + "description\"},{\"type\":\"SparqlService\",\"serviceEndpoint\":\"" + BASE + "sparql\"}]}");
            return;
        }
        if (reqPath.startsWith("HalcyonStorage")) {
            reqPath = reqPath.substring("HalcyonStorage".length());
            if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        }
        String resourceURI = reqPath.isEmpty() ? HTTP_ROOT : HTTP_ROOT + "/" + reqPath;
        Resource r = MODEL.getResource(resourceURI);
        if (!MODEL.containsResource(r)) {
            resp.sendError(404, "Resource not found: " + resourceURI);
            return;
        }
        // SPARQL on .h5 files now checked FIRST (fixes Jena Accept header triggering metadata instead of query)
        if (STORAGE_ROOT != null) {
            Path localFile = STORAGE_ROOT.resolve(reqPath);
            if (Files.exists(localFile) && !Files.isDirectory(localFile) && isHDF5(localFile) && isSparqlRequest(req)) {
                handleSparqlQuery(req, resp, localFile);
                return;
            }
        }
        String linksetURI = getLinksetURI(resourceURI);
        resp.addHeader("Link", "<" + linksetURI + ">; rel=\"linkset\"; type=\"application/linkset+json\"");
        resp.setHeader("Vary", "Accept");
        boolean isContainer = r.hasProperty(RDF.type, LWS_CONTAINER);
        String accept = req.getHeader("Accept") != null ? req.getHeader("Accept").toLowerCase() : "";
        String formatParam = req.getParameter("format");
        boolean forceTurtle = "turtle".equalsIgnoreCase(formatParam);
        boolean forceJsonLd = "jsonld".equalsIgnoreCase(formatParam);
        boolean wantHtml = (accept.contains("text/html") || accept.isEmpty()) && !forceTurtle && !forceJsonLd;
        boolean wantTurtle = accept.contains("turtle") || forceTurtle;
        boolean wantJson = (accept.contains("ld+json") || accept.contains("json")) || forceJsonLd;
        if (isContainer && wantHtml) {
            resp.setContentType("text/html; charset=utf-8");
            try (PrintWriter out = resp.getWriter()) {
                out.println("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>LWS Storage – /" + (reqPath.isEmpty() ? "" : reqPath) + "</title>");
                out.println("<style>body{font-family:sans-serif;margin:40px} ul{list-style:none;padding:0} a{color:#0066cc}</style></head><body>");
                out.println("<h1><img src=\"/sparql/beakgraph.png\" width=\"100\"> LWS Container: /" + (reqPath.isEmpty() ? "" : reqPath) + "</h1>");
                out.println("<p><a href=\"" + BASE + "description\">Storage Description</a> | ");
                out.println("<a href=\"?format=turtle\">Turtle</a> | <a href=\"?format=jsonld\">JSON-LD</a> | ");
                out.println("<a href=\"/sparql/index.html\" target=\"_blank\">SPARQL Endpoint</a></p><hr>");
                List<Resource> items = r.listProperties(LWS_ITEMS).mapWith(Statement::getResource).toList();
                if (items.isEmpty()) {
                    out.println("<p><em>Empty container.</em></p>");
                } else {
                    out.println("<ul>");
                    for (Resource item : items) {
                        String name = item.getURI().substring(HTTP_ROOT.length());
                        if (name.isEmpty()) name = "(root)";
                        String link = name.startsWith("/") ? name : "/" + name;
                        out.printf("<li><a href=\"%s\">%s</a></li>%n", link, name);
                    }
                    out.println("</ul>");
                }
                out.println("</body></html>");
            }
            return;
        }
        if (wantTurtle || wantJson) {
            Model out = ModelFactory.createDefaultModel();
            Resource httpR = out.createResource(BASE + (reqPath.isEmpty() ? "" : reqPath));
            r.listProperties().forEachRemaining(s -> {
                RDFNode obj = s.getObject();
                if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                    String newURI = BASE + obj.asResource().getURI().substring(HTTP_ROOT.length());
                    httpR.addProperty(s.getPredicate(), out.createResource(newURI));
                } else {
                    httpR.addProperty(s.getPredicate(), obj);
                }
            });
            if (isContainer) {
                List<Resource> items = r.listProperties(LWS_ITEMS).mapWith(Statement::getResource).toList();
                String pageStr = req.getParameter("page");
                if (pageStr != null) {
                    int page = Integer.parseInt(pageStr);
                    int size = 20;
                    int total = items.size();
                    int start = (page-1)*size;
                    if (start >= total) { resp.sendError(404); return; }
                    List<Resource> paged = items.subList(start, Math.min(start+size, total));
                    Resource pageR = out.createResource(BASE + (reqPath.isEmpty() ? "" : reqPath) + (reqPath.isEmpty() ? "" : "/") + "?page=" + page);
                    r.listProperties().forEachRemaining(s -> { if (!s.getPredicate().equals(LWS_ITEMS)) pageR.addProperty(s.getPredicate(), s.getObject()); });
                    pageR.addProperty(RDF.type, ResourceFactory.createResource("https://www.w3.org/ns/lws#ContainerPage"));
                    pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#first"), BASE + (reqPath.isEmpty() ? "" : reqPath) + "?page=1");
                    int pages = (total + size - 1) / size;
                    pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#last"), BASE + (reqPath.isEmpty() ? "" : reqPath) + "?page=" + pages);
                    if (page > 1) pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#prev"), BASE + (reqPath.isEmpty() ? "" : reqPath) + "?page=" + (page-1));
                    if (page < pages) pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#next"), BASE + (reqPath.isEmpty() ? "" : reqPath) + "?page=" + (page+1));
                    for (Resource it : paged) {
                        String itHttp = BASE + it.getURI().substring(HTTP_ROOT.length());
                        pageR.addProperty(LWS_ITEMS, out.createResource(itHttp));
                        it.listProperties().forEachRemaining(st -> {
                            RDFNode obj = st.getObject();
                            if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                                String newURI = BASE + obj.asResource().getURI().substring(HTTP_ROOT.length());
                                out.getResource(itHttp).addProperty(st.getPredicate(), out.createResource(newURI));
                            } else {
                                out.getResource(itHttp).addProperty(st.getPredicate(), obj);
                            }
                        });
                    }
                } else {
                    for (Resource it : items) {
                        String itHttp = BASE + it.getURI().substring(HTTP_ROOT.length());
                        it.listProperties().forEachRemaining(st -> {
                            RDFNode obj = st.getObject();
                            if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                                String newURI = BASE + obj.asResource().getURI().substring(HTTP_ROOT.length());
                                out.getResource(itHttp).addProperty(st.getPredicate(), out.createResource(newURI));
                            } else {
                                out.getResource(itHttp).addProperty(st.getPredicate(), obj);
                            }
                        });
                    }
                }
            }
            resp.setContentType(wantTurtle ? "text/turtle" : "application/ld+json");
            RDFDataMgr.write(resp.getOutputStream(), out, wantTurtle ? RDFFormat.TURTLE : RDFFormat.JSONLD);
            return;
        }
        if (isContainer) { resp.sendError(406); return; }
        if (STORAGE_ROOT == null) { resp.sendError(500, "Storage root not set"); return; }
        Path localFile = STORAGE_ROOT.resolve(reqPath);
        if (!Files.exists(localFile) || Files.isDirectory(localFile)) { resp.sendError(404); return; }
        if (isHDF5(localFile) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, localFile);
            return;
        }
        String media = r.getProperty(AS_MEDIA_TYPE).getString();
        resp.setContentType(media);
        resp.setContentLengthLong(Files.size(localFile));
        Files.copy(localFile, resp.getOutputStream());
    }
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String reqPath = req.getRequestURI();
        if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        if (reqPath.endsWith("/")) reqPath = reqPath.substring(0, reqPath.length()-1);
        if (reqPath.startsWith("HalcyonStorage")) {
            reqPath = reqPath.substring("HalcyonStorage".length());
            if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        }
        String resourceURI = reqPath.isEmpty() ? HTTP_ROOT : HTTP_ROOT + "/" + reqPath;
        Resource r = MODEL.getResource(resourceURI);
        if (!MODEL.containsResource(r)) {
            resp.sendError(404, "Resource not found");
            return;
        }
        boolean isContainer = r.hasProperty(RDF.type, LWS_CONTAINER);
        if (isContainer) { resp.sendError(406); return; }
        if (STORAGE_ROOT == null) { resp.sendError(500); return; }
        Path localFile = STORAGE_ROOT.resolve(reqPath);
        if (!Files.exists(localFile) || Files.isDirectory(localFile)) { resp.sendError(404); return; }
        if (isHDF5(localFile) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, localFile);
            return;
        }
        resp.sendError(405, "Method not allowed");
    }
    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }
}
