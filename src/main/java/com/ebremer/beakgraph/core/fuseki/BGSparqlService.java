package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    /**
     * Wall-clock limit per query so one pathological query cannot pin the server.
     * Configurable via the {@code beakgraph.query.timeout.seconds} system property
     * (the CLI's {@code -timeout} flag sets it); 0 or negative disables the limit.
     * Read per query, not cached, so embedding applications can adjust it at runtime.
     */
    static long queryTimeoutSeconds() {
        return Long.getLong("beakgraph.query.timeout.seconds", 30L);
    }

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
     * <p>
     * {@link #isReaderHealthy()} tells a pooled caller whether the underlying
     * reader is at fault. A query that timed out mid-stream, or failed on a
     * query-level error (unregistered function, failed SERVICE call), leaves
     * the reader perfectly usable; only an unexplained failure marks it
     * unhealthy, so that it is invalidated rather than re-issued.
     */
    public static final class QueryExecutionFailedException extends IOException {
        private final boolean readerHealthy;

        QueryExecutionFailedException(String message, Throwable cause, boolean readerHealthy) {
            super(message, cause);
            this.readerHealthy = readerHealthy;
        }

        /** False only when the failure is attributable to the reader itself. */
        public boolean isReaderHealthy() {
            return readerHealthy;
        }
    }

    /**
     * Response body stream with two jobs.
     * <p>
     * It records whether a write to the CLIENT failed: a client that closes
     * the connection while a large result is streaming surfaces as an
     * IOException (Jetty's EofException, "connection reset") from exactly this
     * stream - and from nowhere else - so tagging the failure here tells a
     * transport problem apart from a failure inside the reader, which is what
     * decides whether a pooled reader gets invalidated.
     * <p>
     * It also defers explicit flushes until {@link #COMMIT_THRESHOLD} bytes of
     * body exist. An explicit flush commits the response however little has
     * been written, and Jena's writers flush their partial output in a
     * {@code finally} when execution fails - which turned every error that
     * surfaces lazily (a failed SERVICE call, a timeout on a slow first row)
     * into a truncated 200 instead of a 400/503. Below the threshold nothing
     * is on the wire yet, so {@code sendError} can still answer honestly;
     * beyond it the result is genuinely streaming and flushes pass through
     * (the container commits on its own once its buffer fills in any case).
     */
    private static final class ResponseOutput extends OutputStream {
        static final long COMMIT_THRESHOLD = 16 * 1024;

        private final OutputStream out;
        private long written;
        boolean responseFailed;

        ResponseOutput(OutputStream out) {
            this.out = out;
        }

        @Override public void write(int b) throws IOException {
            try { out.write(b); written++; } catch (IOException e) { responseFailed = true; throw e; }
        }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            try { out.write(b, off, len); written += len; } catch (IOException e) { responseFailed = true; throw e; }
        }

        @Override public void flush() throws IOException {
            if (written < COMMIT_THRESHOLD) {
                return; // keep the response uncommitted while an error can still be reported
            }
            try { out.flush(); } catch (IOException e) { responseFailed = true; throw e; }
        }

        @Override public void close() throws IOException {
            try { out.close(); } catch (IOException e) { responseFailed = true; throw e; }
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

    /** Collapses a query onto one log line (bounded, whitespace-normalized). */
    private static String oneLine(String query) {
        if (query == null) return "";
        String s = query.strip().replaceAll("\\s+", " ");
        return (s.length() > 300) ? s.substring(0, 300) + "..." : s;
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
    /**
     * True when any statement carries an RDF 1.2 triple term (object position
     * only - the data model permits them nowhere else). Linear over the
     * materialized CONSTRUCT/DESCRIBE result, which is already fully in RAM.
     */
    private static boolean containsTripleTerms(Model m) {
        var it = m.listStatements();
        while (it.hasNext()) {
            if (it.next().getObject().asNode().isTripleTerm()) {
                return true;
            }
        }
        return false;
    }

    private static Predicate<Node> storedTermProbe(Dataset ds) {
        if (ds.asDatasetGraph() instanceof BGDatasetGraph bgd) {
            NodeTable nodeTable = bgd.getBeakGraph().getReader().getNodeTable();
            return n -> !NodeId.isDoesNotExist(nodeTable.getNodeIdForNode(n));
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
        ResponseOutput out = null;
        try {
            // DELIBERATE: no Syntax argument, so Jena's default (syntaxARQ)
            // applies. Do NOT "upgrade" this to Syntax.syntaxSPARQL_12 - it
            // looks obviously correct and is a regression: syntaxARQ already
            // parses SPARQL 1.2 triple terms, reifiers-as-annotations, TRIPLE/
            // SUBJECT/OBJECT etc., AND the CDT extensions (UNFOLD/FOLD), while
            // syntaxSPARQL_12 drops UNFOLD from the grammar and silently breaks
            // the supported CDT surface (PLAN Part I §4.0 Trap 1).
            Query query = QueryFactory.create(queryStr);
            RelativeIRIResolver resolver = new RelativeIRIResolver(baseURI);
            // Relativize document IRIs the query names so they match the
            // dictionary. Skip the whole-query walk when there is no base.
            Query execQuery = resolver.isActive()
                    ? QueryTransformOps.transform(query, resolver.absoluteToStorage(storedTermProbe(ds)))
                    : query;
            long timeoutSeconds = queryTimeoutSeconds();
            var qexecBuilder = QueryExecution.dataset(ds).query(execQuery);
            if (timeoutSeconds > 0) {
                qexecBuilder = qexecBuilder.timeout(timeoutSeconds, TimeUnit.SECONDS);
            }
            try (QueryExecution qexec = qexecBuilder.build()) {
                if (execQuery.isSelectType()) {
                    ResultSet rs = resolver.resolve(qexec.execSelect());
                    if (accept.contains("json")) {
                        resp.setContentType("application/sparql-results+json");
                        out = new ResponseOutput(resp.getOutputStream());
                        ResultSetFormatter.outputAsJSON(out, rs);
                    } else if (accept.contains("csv")) {
                        resp.setContentType("text/csv");
                        out = new ResponseOutput(resp.getOutputStream());
                        ResultSetFormatter.outputAsCSV(out, rs);
                    } else {
                        resp.setContentType("application/sparql-results+xml");
                        out = new ResponseOutput(resp.getOutputStream());
                        ResultSetFormatter.outputAsXML(out, rs);
                    }
                } else if (execQuery.isAskType()) {
                    boolean b = qexec.execAsk();
                    resp.setContentType("application/sparql-results+json");
                    out = new ResponseOutput(resp.getOutputStream());
                    out.write(("{\"boolean\":" + b + "}").getBytes(StandardCharsets.UTF_8));
                } else if (execQuery.isConstructType() || execQuery.isDescribeType()) {
                    Model m = execQuery.isConstructType() ? qexec.execConstruct() : qexec.execDescribe();
                    m = resolver.resolve(m);
                    if (containsTripleTerms(m) && !accept.contains("turtle")) {
                        // JSON-LD and RDF/XML have no RDF 1.2 triple-term
                        // syntax; refuse plainly rather than emit a corrupt
                        // body or an opaque serializer failure.
                        resp.sendError(400, "Result contains RDF 1.2 triple terms, which this response format "
                                + "cannot represent; request text/turtle");
                    } else if (accept.contains("json")) {
                        resp.setContentType("application/ld+json");
                        out = new ResponseOutput(resp.getOutputStream());
                        RDFDataMgr.write(out, m, RDFFormat.JSONLD);
                    } else if (accept.contains("turtle")) {
                        resp.setContentType("text/turtle");
                        out = new ResponseOutput(resp.getOutputStream());
                        RDFDataMgr.write(out, m, RDFFormat.TURTLE);
                    } else {
                        resp.setContentType("application/rdf+xml");
                        out = new ResponseOutput(resp.getOutputStream());
                        RDFDataMgr.write(out, m, RDFFormat.RDFXML);
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
        } catch (org.apache.jena.query.QueryCancelledException ex) {
            // The wall-clock limit fired - the reader is healthy, the query was just
            // slow. Tell the client plainly instead of a generic 500.
            long limit = queryTimeoutSeconds();
            logger.warn("SPARQL query cancelled by the {}s timeout (raise with -timeout / "
                    + "beakgraph.query.timeout.seconds): {}", limit, oneLine(queryStr));
            if (resp.isCommitted()) {
                // Slow query, healthy reader: abort the transfer, keep the reader.
                throw new QueryExecutionFailedException("Query timed out after the response was committed", ex, true);
            }
            resp.sendError(503, "Query timed out after " + limit
                    + "s (server limit; adjustable with -timeout)");
            return true;
        } catch (Exception ex) {
            if (out != null && out.responseFailed) {
                // The connection to the client failed while the body was
                // streaming (the client went away, in practice). Routine, and
                // no reflection on the reader: nothing more can be written, so
                // finish quietly and let the container discard the connection.
                logger.debug("SPARQL client connection failed while the response was streaming: {}",
                        oneLine(queryStr));
                return true;
            }
            if (ex instanceof org.apache.jena.query.QueryException) {
                // The QUERY is at fault, not the reader: an unregistered
                // function (QueryBuildException), an evaluation failure, a
                // SERVICE call that could not be completed. Such errors surface
                // lazily, so the response may already be committed.
                logger.info("SPARQL query rejected: {} [{}]", ex.getMessage(), oneLine(queryStr));
                if (resp.isCommitted()) {
                    throw new QueryExecutionFailedException("Query failed after the response was committed", ex, true);
                }
                resp.sendError(400, "Query error: " + ex.getMessage());
                return true;
            }
            // Internal failure: log the details server-side, but do not echo
            // exception internals (paths, class names, state) back to the client.
            logger.error("SPARQL query execution failed", ex);
            if (resp.isCommitted()) {
                // Partial 200 body already flushed: sendError would throw
                // IllegalStateException. Rethrow so the container aborts the
                // connection - the honest signal for a truncated result.
                throw new QueryExecutionFailedException("Query failed after the response was committed", ex, false);
            }
            resp.sendError(500, "Query execution failed");
            return false;
        }
    }
}
