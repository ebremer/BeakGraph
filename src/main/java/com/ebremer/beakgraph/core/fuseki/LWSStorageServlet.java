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
    private static final String HTTP_BASE = "https://localhost:8888/";
    private static final String FILE_BASE = "file:///D:/HalcyonStorage/";
    private static final String LWS_NS = "https://www.w3.org/ns/lws#";
    private static final Model MODEL = loadModel(); // static model with provided triples

    private static Model loadModel() {
        Model m = ModelFactory.createDefaultModel();
        // Populate with provided Turtle data (or load from string/file)
        // m.read(new StringReader("""PREFIX ... [all triples]"""), null, "TURTLE");
        return m;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String path = req.getRequestURI().equals("/") ? "" : req.getRequestURI().substring(1);
        String modelURI = FILE_BASE + path;
        String httpURI = HTTP_BASE + path;
        Resource r = MODEL.getResource(modelURI);

        boolean isContainer = r.hasProperty(RDF.type, MODEL.getResource(LWS_NS + "Container"));

        String accept = req.getHeader("Accept");
        boolean wantJsonLD = accept != null && (accept.contains("application/ld+json") || accept.contains("application/lws+json"));
        boolean wantTurtle = accept != null && accept.contains("text/turtle");

        resp.setHeader("Link", "<" + HTTP_BASE + ">; rel=\"storageDescription\"");
        resp.setHeader("Vary", "Accept");

        if (isContainer) {
            resp.setContentType(wantTurtle ? "text/turtle" : "application/ld+json");
            if (wantTurtle) {
                RDFDataMgr.write(resp.getOutputStream(), MODEL.listStatements(r, null, (RDFNode)null).toModel(), RDFFormat.TURTLE);
            } else {
                RDFDataMgr.write(resp.getOutputStream(), MODEL.listStatements(r, null, (RDFNode)null).toModel(), RDFFormat.JSONLD);
            }
            return;
        }

        // DataResource: serve file
        String filePathStr = modelURI.replace("file:///", "");
        File file = new File(filePathStr);
        if (!file.exists()) { resp.sendError(404); return; }

        String mediaType = r.getProperty(MODEL.getProperty("https://www.w3.org/ns/activitystreams#", "mediaType")).getString();
        resp.setContentType(mediaType);
        resp.setContentLengthLong(file.length());
        Files.copy(file.toPath(), resp.getOutputStream());
    }

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp); // headers only
    }
}