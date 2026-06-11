package com.ebremer.beakgraph.core.fuseki;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.syntax.syntaxtransform.QueryTransformOps;

/**
 * Runs a SPARQL query against a BeakGraph dataset and writes the response,
 * resolving document-relative IRIs against a base URI.
 * <p>
 * BeakGraph stores IRIs that were relative in the source document ({@code <>},
 * {@code <sibling>}) in relative form. This helper applies {@link RelativeIRIResolver}
 * in both directions - relativizing IRIs the query names so they match the
 * dictionary, and resolving result IRIs to absolute form - so every endpoint
 * that serves HDF5 data ({@code LWSStorageServlet} and the single-file
 * {@code /rdf} endpoint) behaves consistently.
 */
public final class BGSparqlService {

    /** Maximum accepted SPARQL request body. Real queries are tiny; an unbounded
     *  readAllBytes lets a single request allocate arbitrary heap. */
    static final int MAX_QUERY_BODY_BYTES = 1 << 20; // 1 MiB

    /** Hard wall-clock limit per query so one pathological query cannot pin the server. */
    private static final long QUERY_TIMEOUT_SECONDS = 30;

    /** Thrown when a POST body exceeds {@link #MAX_QUERY_BODY_BYTES}; callers map it to HTTP 413. */
    public static final class QueryBodyTooLargeException extends IOException {
        QueryBodyTooLargeException(String message) {
            super(message);
        }
    }

    private BGSparqlService() {
    }

    /** Extract the SPARQL query string from a GET or POST request. */
    public static String extractQuery(HttpServletRequest req) throws IOException {
        if ("GET".equals(req.getMethod())) {
            return req.getParameter("query");
        }
        if ("POST".equals(req.getMethod())) {
            String ct = req.getContentType();
            if (ct != null && ct.toLowerCase().startsWith("application/x-www-form-urlencoded")) {
                return req.getParameter("query");
            }
            try (InputStream in = req.getInputStream()) {
                return readBody(in, MAX_QUERY_BODY_BYTES);
            }
        }
        return null;
    }

    /** Read at most {@code max} bytes as UTF-8; reject anything larger. */
    static String readBody(InputStream in, int max) throws IOException {
        byte[] body = in.readNBytes(max + 1);
        if (body.length > max) {
            throw new QueryBodyTooLargeException("SPARQL query body exceeds " + max + " bytes");
        }
        return new String(body, StandardCharsets.UTF_8);
    }

    /**
     * Execute {@code queryStr} against {@code ds} and serialize the result,
     * resolving relative IRIs against {@code baseURI} (the URL/URI the data is
     * served from). On any failure an HTTP 400 is written.
     */
    public static void execute(Dataset ds, String queryStr, String baseURI,
                               String acceptHeader, HttpServletResponse resp) throws IOException {
        String accept = (acceptHeader == null) ? "" : acceptHeader.toLowerCase();
        try {
            Query query = QueryFactory.create(queryStr);
            RelativeIRIResolver resolver = new RelativeIRIResolver(baseURI);
            // Relativize document IRIs the query names so they match the
            // dictionary. Skip the whole-query walk when there is no base.
            Query execQuery = resolver.isActive()
                    ? QueryTransformOps.transform(query, resolver.absoluteToStorage())
                    : query;
            try (QueryExecution qexec = QueryExecution.dataset(ds).query(execQuery)
                    .timeout(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS).build()) {
                if (execQuery.isSelectType()) {
                    ResultSet rs = resolver.resolve(qexec.execSelect());
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
                } else if (execQuery.isAskType()) {
                    boolean b = qexec.execAsk();
                    resp.setContentType("application/sparql-results+json");
                    resp.getWriter().write("{\"boolean\":" + b + "}");
                } else if (execQuery.isConstructType() || execQuery.isDescribeType()) {
                    Model m = execQuery.isConstructType() ? qexec.execConstruct() : qexec.execDescribe();
                    m = resolver.resolve(m);
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
                } else {
                    resp.sendError(400, "Unsupported SPARQL query type");
                }
            }
        } catch (Exception ex) {
            resp.sendError(400, "Query error: " + ex.getMessage());
        }
    }
}
