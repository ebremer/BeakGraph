package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.jena.graph.Node;
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

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BGSparqlService.class);

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

    /**
     * Thrown when execution failed AFTER the response was committed: a 200 and
     * part of the body are already on the wire, so the only honest signal left
     * is an aborted transfer. Callers must NOT touch the response again (a
     * sendError would throw IllegalStateException) and should let this
     * propagate so the container closes the connection without completing the
     * response - a client then sees a truncated transfer instead of a
     * complete-looking 200 with silently missing rows.
     */
    public static final class QueryExecutionFailedException extends IOException {
        QueryExecutionFailedException(String message, Throwable cause) {
            super(message, cause);
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
     * Dictionary-membership probe backing the relativization rewrite decision.
     * BG datasets answer from the node table (a binary search - deliberately
     * NOT a find() over the dataset, which fans out across every named graph).
     * Any other dataset has no relative-stored IRIs, so nothing is rewritten.
     */
    private static Predicate<Node> storedTermProbe(Dataset ds) {
        if (ds.asDatasetGraph() instanceof BGDatasetGraph bgd) {
            NodeTable nodeTable = bgd.getBeakGraph().getReader().getNodeTable();
            return n -> {
                NodeId id = nodeTable.getNodeIdForNode(n);
                return id != null && !NodeId.isDoesNotExist(id);
            };
        }
        return n -> false;
    }

    /**
     * Execute {@code queryStr} against {@code ds} and serialize the result,
     * resolving relative IRIs against {@code baseURI} (the URL/URI the data is
     * served from). Client-side problems (parse errors, unsupported query
     * types) are answered with 400 and still return true - the reader is fine.
     *
     * @return true when the underlying reader behaved; false when execution
     *         failed (a 500 was written) - pooled callers should invalidate
     *         their instance rather than return it.
     * @throws QueryExecutionFailedException when execution failed after the
     *         response was committed (see that exception's contract)
     */
    public static boolean execute(Dataset ds, String queryStr, String baseURI,
                               String acceptHeader, HttpServletResponse resp) throws IOException {
        String accept = (acceptHeader == null) ? "" : acceptHeader.toLowerCase();
        try {
            Query query = QueryFactory.create(queryStr);
            RelativeIRIResolver resolver = new RelativeIRIResolver(baseURI);
            // Relativize document IRIs the query names so they match the
            // dictionary. Skip the whole-query walk when there is no base.
            Query execQuery = resolver.isActive()
                    ? QueryTransformOps.transform(query, resolver.absoluteToStorage(storedTermProbe(ds)))
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
            return true;
        } catch (org.apache.jena.query.QueryParseException ex) {
            // The client's own query text is at fault; the parse message is theirs.
            resp.sendError(400, "Query parse error: " + ex.getMessage());
            return true;
        } catch (Exception ex) {
            // Internal failure: log the details server-side, but do not echo
            // exception internals (paths, class names, state) back to the client.
            logger.error("SPARQL query execution failed", ex);
            if (resp.isCommitted()) {
                // Partial 200 body already flushed: sendError would throw
                // IllegalStateException. Rethrow so the container aborts the
                // connection - the honest signal for a truncated result.
                throw new QueryExecutionFailedException("Query failed after the response was committed", ex);
            }
            resp.sendError(500, "Query execution failed");
            return false;
        }
    }
}
