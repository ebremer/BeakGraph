package com.ebremer.beakgraph.core.fuseki;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import com.ebremer.ns.LWS;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import jakarta.servlet.http.*;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonWriter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class LWSStorageServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static String BASE;
    private static Path STORAGE_ROOT;
    private final transient Model MODEL;
    private static final String HTTP_ROOT = LWSMetadataGenerator.CANONICAL_BASE;
    private static final Resource LWS_CONTAINER = LWS.Container;
    private static final Property LWS_ITEMS = LWS.items;
    private static final Property AS_MEDIA_TYPE = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#mediaType");
    private static final Property SCHEMA_SIZE = ResourceFactory.createProperty("https://schema.org/size");
    private static final Property AS_UPDATED = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#updated");
    private static final Logger logger = LoggerFactory.getLogger(LWSStorageServlet.class);
    
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
        if ("POST".equals(method) && ct != null) {
            String ctl = ct.toLowerCase();
            if (ctl.startsWith("application/sparql-query")) return true;
            if (ctl.startsWith("application/x-www-form-urlencoded") && req.getParameter("query") != null) return true;
      }
      return "GET".equals(method) && req.getParameter("query") != null;
  }
    private void handleSparqlQuery(HttpServletRequest req, HttpServletResponse resp, Path h5File) throws IOException {
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
        URI fileUri = h5File.toUri();
        BeakGraph bg = null;
        boolean healthy = true;
        try {
            bg = BeakGraphPool.getPool().borrowObject(fileUri);
            // resolve document-relative IRIs against the URL this .h5 is served from
            healthy = BGSparqlService.execute(bg.getDataset(), queryStr,
                    req.getRequestURL().toString(), req.getHeader("Accept"), resp);
        } catch (BGSparqlService.QueryExecutionFailedException ex) {
            // Failure after the response committed: nothing may touch the response
            // now. Rethrow (after the finally invalidates the reader) so the
            // container aborts the connection instead of finishing a truncated
            // 200 body as if it were complete.
            healthy = false;
            throw ex;
        } catch (Exception ex) {
            // Reaching here means infrastructure failure (pool, file). Log it,
            // don't echo internals to the client.
            healthy = false;
            logger.error("SPARQL query handling failed for {}", h5File, ex);
            if (!resp.isCommitted()) {
                resp.sendError(500, "Query execution failed");
            }
        } finally {
            if (bg != null) {
                if (healthy) {
                    BeakGraphPool.getPool().returnObject(fileUri, bg);
                } else {
                    // Never re-issue an instance that just failed: with only
                    // returnObject here, a broken-but-open reader circulated
                    // forever, 500ing every request for its file.
                    try {
                        BeakGraphPool.getPool().invalidateObject(fileUri, bg);
                    } catch (Exception invalidateEx) {
                        logger.warn("Failed to invalidate pooled BeakGraph for {}", fileUri, invalidateEx);
                    }
                }
            }
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
            ? LWS.Container.getURI()
            : LWS.DataResource.getURI();
        String up = getParentURI(resourceURI);
        String media = r.hasProperty(AS_MEDIA_TYPE) ? r.getProperty(AS_MEDIA_TYPE).getString() : null;
        String size = r.hasProperty(SCHEMA_SIZE) ? r.getProperty(SCHEMA_SIZE).getString() : null;
        String updated = r.hasProperty(AS_UPDATED) ? r.getProperty(AS_UPDATED).getString() : null;
        resp.setContentType("application/linkset+json");
        resp.setCharacterEncoding("UTF-8");
        // anchor/up are advertised on the LIVE base, never the canonical one.
        resp.getWriter().write(linksetJson(toLiveUri(resourceURI), typeHref,
                up == null ? null : toLiveUri(up), media, size, updated));
    }

    /**
     * Build an RFC 9264 linkset+json document. Every value is emitted through jakarta.json, so a
     * quote/backslash/newline in a URI or media type is escaped rather than breaking (or injecting
     * into) the JSON. Null relation values are omitted.
     */
    static String linksetJson(String anchor, String typeHref, String up,
                              String mediaType, String size, String updated) {
        JsonObjectBuilder link = Json.createObjectBuilder()
            .add("anchor", anchor)
            .add("type", hrefArray(typeHref));
        if (up != null)        link.add("up", hrefArray(up));
        if (mediaType != null) link.add("mediaType", hrefArray(mediaType));
        if (size != null)      link.add("size", hrefArray(size));
        if (updated != null)   link.add("updated", hrefArray(updated));
        JsonObject root = Json.createObjectBuilder()
            .add("linkset", Json.createArrayBuilder().add(link))
            .build();
        return writeJson(root);
    }

    /** The LWS service-description document (application/ld+json). */
    static String descriptionJson(String base) {
        JsonObject doc = Json.createObjectBuilder()
            .add("@context", "https://www.w3.org/ns/lws/v1")
            .add("@id", base)
            .add("type", "Storage")
            .add("service", Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "StorageDescription")
                    .add("serviceEndpoint", base + "description"))
                .add(Json.createObjectBuilder()
                    .add("type", "SparqlService")
                    .add("serviceEndpoint", base + "sparql")))
            .build();
        return writeJson(doc);
    }

    /** A {@code [{"href": <href>}]} array, the shape each linkset relation uses. */
    private static JsonArrayBuilder hrefArray(String href) {
        return Json.createArrayBuilder().add(Json.createObjectBuilder().add("href", href));
    }

    private static String writeJson(JsonObject obj) {
        StringWriter sw = new StringWriter();
        try (JsonWriter w = Json.createWriter(sw)) {
            w.writeObject(obj);
        }
        return sw.toString();
    }

    /**
     * Minimal HTML escaping for text and attribute contexts. Request paths and
     * on-disk filenames are attacker-influenced and end up in the container
     * listing - unescaped they are a stored-XSS sink.
     */
    static String escapeHtml(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&#39;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Percent-encode a path for use in an href (keeps '/', encodes spaces, quotes, etc.). */
    static String encodeHref(String path) {
        try {
            return new java.net.URI(null, null, path, null).toASCIIString();
        } catch (java.net.URISyntaxException e) {
            return java.net.URLEncoder.encode(path, StandardCharsets.UTF_8);
        }
    }

    /**
     * Decode the raw request URI into the literal path the metadata model and the
     * filesystem use. Servlet spec: {@code getRequestURI()} is UNDECODED, while the
     * model holds raw filenames - comparing them directly made every resource whose
     * name needs percent-encoding (spaces, '#', non-ASCII) permanently 404, including
     * via this servlet's own encoded listing links. Returns null for a syntactically
     * invalid URI (callers answer 400).
     */
    static String decodePath(String rawRequestUri) {
        try {
            String p = new java.net.URI(rawRequestUri).getPath();
            return p == null ? "" : p;
        } catch (java.net.URISyntaxException e) {
            return null;
        }
    }

    /**
     * Map a canonical-model URI (rooted at {@link LWSMetadataGenerator#CANONICAL_BASE})
     * onto the live serving base. {@code base} ends with '/' and the canonical
     * remainder starts with '/', so naive concatenation minted double-slash URIs that
     * 404 when dereferenced; the canonical host/port also leaked into Link headers and
     * linksets whenever the server ran on a non-default port.
     */
    static String toLiveUri(String canonicalUri, String base) {
        String rest = canonicalUri.substring(HTTP_ROOT.length());
        while (rest.startsWith("/")) {
            rest = rest.substring(1);
        }
        return base + rest;
    }

    private String toLiveUri(String canonicalUri) {
        return toLiveUri(canonicalUri, BASE);
    }

    /** Container page URI on the live base; reqPath carries no leading slash and BASE ends with '/'. */
    private String pageUri(String reqPath, int page) {
        return BASE + reqPath + "?page=" + page;
    }

    /**
     * Whether the (lower-cased) Accept header admits an HTML representation.
     * RFC 9110: {@code *}{@code /*} and {@code text/*} match every/any text
     * representation - curl's default {@code Accept: *}{@code /*} must get a
     * page, not a 406.
     */
    static boolean acceptsHtmlRepresentation(String lowerAccept) {
        return lowerAccept.isEmpty()
                || lowerAccept.contains("text/html")
                || lowerAccept.contains("text/*")
                || lowerAccept.contains("*/*");
    }

    /**
     * Whether a statement object may be sent to clients. The metadata model links
     * every resource to its on-disk location via {@code owl:sameAs <file:///...>};
     * those server-local URIs must never leave the server.
     */
    static boolean exposableToClient(RDFNode obj) {
        return !(obj.isURIResource() && obj.asResource().getURI().startsWith("file:"));
    }

    /**
     * Resolve a request path against the storage root, rejecting anything that escapes it via "..",
     * an absolute path, etc. Returns null when {@code root} is null or the path would leave the root.
     */
    static Path resolveWithin(Path root, String reqPath) {
        if (root == null) {
            return null;
        }
        Path base = root.toAbsolutePath().normalize();
        Path resolved = base.resolve(reqPath).normalize();
        if (!resolved.startsWith(base)) {
            return null;                     // lexical check: blocks ".." and absolute paths
        }
        // Defence in depth: if the target exists, resolve symlinks and re-check containment so a
        // symlink planted inside the storage root cannot point outside it.
        try {
            if (Files.exists(resolved) && !resolved.toRealPath().startsWith(base.toRealPath())) {
                return null;
            }
        } catch (IOException e) {
            return null;                     // cannot verify -> refuse
        }
        return resolved;
    }
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Stop browsers from MIME-sniffing served content into something executable.
        resp.setHeader("X-Content-Type-Options", "nosniff");
        String reqPath = decodePath(req.getRequestURI());
        if (reqPath == null) { resp.sendError(400, "Malformed request URI"); return; }
        if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        if (reqPath.endsWith("/")) reqPath = reqPath.substring(0, reqPath.length()-1);
        if (reqPath.endsWith(".meta")) {
            String basePath = reqPath.substring(0, reqPath.length() - 5);
            if (basePath.startsWith("HalcyonStorage")) {
                basePath = basePath.substring("HalcyonStorage".length());
                if (basePath.startsWith("/")) basePath = basePath.substring(1);
            }
            if (basePath.equals("description")) {
                // The /description document advertises this linkset itself; it is
                // not in the metadata model, so answer it directly instead of 404.
                resp.setContentType("application/linkset+json");
                resp.setCharacterEncoding("UTF-8");
                resp.getWriter().write(linksetJson(BASE + "description",
                        LWS.MetadataResource.getURI(), BASE, null, null, null));
                return;
            }
            String baseResourceURI = basePath.isEmpty() ? HTTP_ROOT : HTTP_ROOT + "/" + basePath;
            serveLinkset(resp, baseResourceURI);
            return;
        }
        if (reqPath.equals("description")) {
            resp.setContentType("application/ld+json");
            resp.setHeader("Link", "<" + BASE + "description>; rel=\"storageDescription\"");
            // addHeader, not setHeader: a second setHeader replaces the first Link
            // header, which silently dropped the storageDescription link.
            resp.addHeader("Link", "<" + getLinksetURI(BASE + "description") + ">; rel=\"linkset\"; type=\"application/linkset+json\"");
            resp.setHeader("Vary", "Accept");
            resp.getWriter().write(descriptionJson(BASE));
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
        Path h5Candidate = resolveWithin(STORAGE_ROOT, reqPath);
        if (h5Candidate != null && Files.exists(h5Candidate) && !Files.isDirectory(h5Candidate)
                && isHDF5(h5Candidate) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, h5Candidate);
            return;
        }
        String linksetURI = getLinksetURI(toLiveUri(resourceURI));
        resp.addHeader("Link", "<" + linksetURI + ">; rel=\"linkset\"; type=\"application/linkset+json\"");
        resp.setHeader("Vary", "Accept");
        boolean isContainer = r.hasProperty(RDF.type, LWS_CONTAINER);
        String accept = req.getHeader("Accept") != null ? req.getHeader("Accept").toLowerCase() : "";
        String formatParam = req.getParameter("format");
        boolean forceTurtle = "turtle".equalsIgnoreCase(formatParam);
        boolean forceJsonLd = "jsonld".equalsIgnoreCase(formatParam);
        boolean wantHtml = acceptsHtmlRepresentation(accept) && !forceTurtle && !forceJsonLd;
        boolean wantTurtle = accept.contains("turtle") || forceTurtle;
        boolean wantJson = (accept.contains("ld+json") || accept.contains("json")) || forceJsonLd;
        if (isContainer && wantHtml) {
            resp.setContentType("text/html; charset=utf-8");
            // The request path and item names come from the URL / the filesystem;
            // escape them for HTML and percent-encode hrefs (stored-XSS sink).
            String safePath = escapeHtml(reqPath);
            try (PrintWriter out = resp.getWriter()) {
                out.println("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>LWS Storage – /" + safePath + "</title>");
                out.println("<style>body{font-family:sans-serif;margin:40px} ul{list-style:none;padding:0} a{color:#0066cc}</style></head><body>");
                out.println("<h1><img src=\"/sparql/beakgraph.png\" width=\"100\"> Linked Web Storage: /" + safePath + "</h1>");
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
                        out.printf("<li><a href=\"%s\">%s</a></li>%n",
                                escapeHtml(encodeHref(link)), escapeHtml(name));
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
                if (!exposableToClient(obj)) return; // never leak file:/// server paths
                if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                    httpR.addProperty(s.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI())));
                } else {
                    httpR.addProperty(s.getPredicate(), obj);
                }
            });
            if (isContainer) {
                List<Resource> items = r.listProperties(LWS_ITEMS).mapWith(Statement::getResource).toList();
                String pageStr = req.getParameter("page");
                if (pageStr != null) {
                    int page;
                    try {
                        page = Integer.parseInt(pageStr.trim());
                    } catch (NumberFormatException e) {
                        resp.sendError(400, "Invalid 'page' parameter: " + pageStr);
                        return;
                    }
                    if (page < 1) { resp.sendError(400, "'page' must be >= 1"); return; }
                    int size = 20;
                    int total = items.size();
                    long start = (long)(page-1) * size;   // long: a huge page must not overflow to a negative index
                    if (start >= total) { resp.sendError(404); return; }
                    int startIdx = (int) start;
                    List<Resource> paged = items.subList(startIdx, Math.min(startIdx+size, total));
                    // Self and first/last/prev/next all use the same URI shape,
                    // and the navigation links are RESOURCES (they are page URIs,
                    // and were emitted as plain literals with a divergent shape).
                    Resource pageR = out.createResource(pageUri(reqPath, page));
                    r.listProperties().forEachRemaining(s -> {
                        if (!s.getPredicate().equals(LWS_ITEMS) && exposableToClient(s.getObject())) {
                            pageR.addProperty(s.getPredicate(), s.getObject());
                        }
                    });
                    pageR.addProperty(RDF.type, LWS.ContainerPage);
                    pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#first"), out.createResource(pageUri(reqPath, 1)));
                    int pages = (total + size - 1) / size;
                    pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#last"), out.createResource(pageUri(reqPath, pages)));
                    if (page > 1) pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#prev"), out.createResource(pageUri(reqPath, page-1)));
                    if (page < pages) pageR.addProperty(ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#next"), out.createResource(pageUri(reqPath, page+1)));
                    for (Resource it : paged) {
                        String itHttp = toLiveUri(it.getURI());
                        pageR.addProperty(LWS_ITEMS, out.createResource(itHttp));
                        it.listProperties().forEachRemaining(st -> {
                            RDFNode obj = st.getObject();
                            if (!exposableToClient(obj)) return; // never leak file:/// server paths
                            if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                                out.getResource(itHttp).addProperty(st.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI())));
                            } else {
                                out.getResource(itHttp).addProperty(st.getPredicate(), obj);
                            }
                        });
                    }
                } else {
                    for (Resource it : items) {
                        String itHttp = toLiveUri(it.getURI());
                        it.listProperties().forEachRemaining(st -> {
                            RDFNode obj = st.getObject();
                            if (!exposableToClient(obj)) return; // never leak file:/// server paths
                            if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                                out.getResource(itHttp).addProperty(st.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI())));
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
        Path localFile = resolveWithin(STORAGE_ROOT, reqPath);
        if (localFile == null) { resp.sendError(403, "Forbidden"); return; }
        if (!Files.exists(localFile) || Files.isDirectory(localFile)) { resp.sendError(404); return; }
        if (isHDF5(localFile) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, localFile);
            return;
        }
        Statement mediaStmt = r.getProperty(AS_MEDIA_TYPE);
        String media = (mediaStmt != null) ? mediaStmt.getString() : null;
        if (media == null || media.isBlank()) {
            // Resource metadata didn't declare a media type; probe the file, else fall back.
            media = Files.probeContentType(localFile);
            if (media == null) media = "application/octet-stream";
        }
        // Conditional GET support: the store is read-only between writes, so
        // size+mtime make a stable validator.
        long size = Files.size(localFile);
        long lastModified = Files.getLastModifiedTime(localFile).toMillis();
        String etag = "\"" + size + "-" + lastModified + "\"";
        resp.setHeader("ETag", etag);
        resp.setDateHeader("Last-Modified", lastModified);
        String ifNoneMatch = req.getHeader("If-None-Match");
        long ifModifiedSince = req.getDateHeader("If-Modified-Since");
        if (etag.equals(ifNoneMatch)
                || (ifNoneMatch == null && ifModifiedSince >= 0 && lastModified / 1000 <= ifModifiedSince / 1000)) {
            resp.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return;
        }
        resp.setContentType(media);
        // Stored bytes are served as a download: with nosniff above, this keeps an
        // HTML/SVG file someone placed under the root from rendering in this origin.
        String filename = localFile.getFileName().toString().replace("\"", "");
        resp.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        resp.setContentLengthLong(size);
        // HEAD gets the same headers without the body (and without the file copy).
        if (!"HEAD".equals(req.getMethod())) {
            Files.copy(localFile, resp.getOutputStream());
        }
    }
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setHeader("X-Content-Type-Options", "nosniff");
        logger.info("LWS {} {} ct={} query-param={} bodyLen={}",
        req.getMethod(), req.getRequestURI(), req.getContentType(),
        req.getParameter("query") != null, req.getContentLengthLong());
        String reqPath = decodePath(req.getRequestURI());
        if (reqPath == null) { resp.sendError(400, "Malformed request URI"); return; }
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
        // 405, not 406: POST to a container is an unsupported METHOD, not a
        // content-negotiation failure.
        if (isContainer) { resp.sendError(405, "Method not allowed"); return; }
        if (STORAGE_ROOT == null) { resp.sendError(500); return; }
        Path localFile = resolveWithin(STORAGE_ROOT, reqPath);
        if (localFile == null) { resp.sendError(403, "Forbidden"); return; }
        if (!Files.exists(localFile) || Files.isDirectory(localFile)) { resp.sendError(404); return; }
        if (isHDF5(localFile) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, localFile);
            return;
        }
        resp.sendError(405, "Method not allowed");
    }
    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Same routing and headers as GET; the file-serving branch checks the
        // method and skips the body copy for HEAD.
        doGet(req, resp);
    }
}
