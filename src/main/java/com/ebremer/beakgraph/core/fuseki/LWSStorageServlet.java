package com.ebremer.beakgraph.core.fuseki;

import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class LWSStorageServlet extends HttpServlet {
    private static String BASE;
    private static Path STORAGE_ROOT;
    private final Model MODEL;

    private static final String HTTP_ROOT = "http://localhost:8888/HalcyonStorage";
    private static final Resource LWS_CONTAINER = ResourceFactory.createResource("https://www.w3.org/ns/lws#Container");
    private static final Property LWS_ITEMS = ResourceFactory.createProperty("https://www.w3.org/ns/lws#items");
    private static final Property AS_MEDIA_TYPE = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#mediaType");

    public LWSStorageServlet(Model model) { this.MODEL = model; }

    public static void setBase(String b) { BASE = b; }
    public static void setStorageRoot(Path root) { STORAGE_ROOT = root; }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String reqPath = req.getRequestURI();
        if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        if (reqPath.endsWith("/")) reqPath = reqPath.substring(0, reqPath.length()-1);

        if (reqPath.equals("description")) {
            resp.setContentType("application/ld+json");
            resp.setHeader("Link", "<" + BASE + "description>; rel=\"storageDescription\"");
            resp.setHeader("Vary", "Accept");
            resp.getWriter().write("{\"@context\":\"https://www.w3.org/ns/lws/v1\",\"@id\":\"" + BASE + "\",\"type\":\"Storage\",\"service\":[{\"type\":\"StorageDescription\",\"serviceEndpoint\":\"" + BASE + "description\"}]}");
            return;
        }

        // strip HalcyonStorage prefix if present so /HalcyonStorage/nasa/... becomes "nasa/..."
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

        boolean isContainer = r.hasProperty(RDF.type, LWS_CONTAINER);

        String accept = req.getHeader("Accept") != null ? req.getHeader("Accept").toLowerCase() : "";
        boolean wantHtml = accept.contains("text/html") || accept.isEmpty();
        boolean wantTurtle = accept.contains("turtle");
        boolean wantJson = accept.contains("ld+json") || accept.contains("json");

        resp.setHeader("Link", "<" + BASE + "description>; rel=\"storageDescription\"");
        resp.setHeader("Vary", "Accept");

        if (isContainer && wantHtml) {
            resp.setContentType("text/html; charset=utf-8");
            try (PrintWriter out = resp.getWriter()) {
                out.println("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>LWS Storage – /" + (reqPath.isEmpty() ? "" : reqPath) + "</title>");
                out.println("<style>body{font-family:sans-serif;margin:40px} ul{list-style:none;padding:0} a{color:#0066cc}</style></head><body>");
                out.println("<h1>🌐 LWS Container: /" + (reqPath.isEmpty() ? "" : reqPath) + "</h1>");
                out.println("<p><a href=\"" + BASE + "description\">Storage Description</a> | ");
                out.println("<a href=\"?format=turtle\">Turtle</a> | <a href=\"?format=jsonld\">JSON-LD</a></p><hr>");
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

        String media = r.getProperty(AS_MEDIA_TYPE).getString();
        resp.setContentType(media);
        resp.setContentLengthLong(Files.size(localFile));
        Files.copy(localFile, resp.getOutputStream());
    }

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }
}