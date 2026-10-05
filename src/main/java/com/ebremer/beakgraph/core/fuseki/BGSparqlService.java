package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.core.RelativeIRIResolver;
import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.DatasetDescription;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.syntax.ElementData;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
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

    /**
     * Largest CONSTRUCT / DESCRIBE result served in a format that has to be
     * materialized (JSON-LD, RDF/XML). Turtle and N-Triples are streamed and
     * unbounded. Configurable via {@code beakgraph.query.construct.max.triples};
     * 0 or negative disables the cap. The wall-clock limit alone did not bound
     * memory: a fast whole-store CONSTRUCT filled the heap well inside it (BG-222).
     */
    static long constructMaxTriples() {
        return Long.getLong("beakgraph.query.construct.max.triples", 1_000_000L);
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
            if (ct != null && ct.toLowerCase(Locale.ROOT).startsWith("application/x-www-form-urlencoded")) {
                return req.getParameter("query");
            }
            try (InputStream in = req.getInputStream()) {
                return readBody(in, MAX_QUERY_BODY_BYTES);
            }
        }
        return null;
    }

    /**
     * The SPARQL 1.1 Protocol dataset of a request ({@code default-graph-uri}
     * and {@code named-graph-uri}, repeatable, read from the URL for GET and
     * for a direct POST, from the form for a form-encoded POST), or null when
     * the request names none. Used to be ignored silently (BG-344).
     */
    public static DatasetDescription extractDatasetDescription(HttpServletRequest req) {
        return datasetDescription(req.getParameterValues("default-graph-uri"), req.getParameterValues("named-graph-uri"));
    }

    static DatasetDescription datasetDescription(String[] defaultGraphUris, String[] namedGraphUris) {
        List<String> dg = (defaultGraphUris == null) ? List.of() : Arrays.asList(defaultGraphUris);
        List<String> ng = (namedGraphUris == null) ? List.of() : Arrays.asList(namedGraphUris);
        if (dg.isEmpty() && ng.isEmpty()) {
            return null;
        }
        return DatasetDescription.create(dg, ng);
    }

    /**
     * Protocol section 2.1.4: a dataset given by the protocol parameters
     * REPLACES the query's own FROM / FROM NAMED clauses (both of them - the
     * protocol's dataset is the whole description). A null description leaves
     * the query untouched. Graph names are matched as given; they are absolute
     * IRIs in stored data, so no relativization applies.
     */
    static void applyProtocolDataset(Query query, DatasetDescription protocolDataset) {
        if (protocolDataset == null || protocolDataset.isEmpty()) {
            return;
        }
        query.getGraphURIs().clear();
        query.getNamedGraphURIs().clear();
        protocolDataset.getDefaultGraphURIs().forEach(query::addGraphURI);
        protocolDataset.getNamedGraphURIs().forEach(query::addNamedGraphURI);
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

    /**
     * Jena's syntax transform rewrites the WHERE pattern, but not the nodes a
     * DESCRIBE names nor the rows of a VALUES clause (inline or top-level);
     * a client naming the document there by its absolute URL missed the
     * stored relative term (BG-397). Applied in place to the transformed copy.
     */
    static void rewriteOutsideThePattern(Query q, NodeTransform t) {
        if (q.isDescribeType()) {
            List<Node> nodes = q.getResultURIs();
            List<Node> rewritten = nodes.stream().map(t::apply).toList();
            nodes.clear();
            nodes.addAll(rewritten);
        }
        if (q.hasValues()) {
            List<Binding> rows = q.getValuesData().stream().map(b -> transformValues(b, t)).toList();
            q.setValuesDataBlock(q.getValuesVariables(), rows);
        }
        if (q.getQueryPattern() != null) {
            ElementWalker.walk(q.getQueryPattern(), new ElementVisitorBase() {
                @Override
                public void visit(ElementData el) {
                    List<Binding> rows = el.getRows();
                    for (int i = 0; i < rows.size(); i++) {
                        rows.set(i, transformValues(rows.get(i), t));
                    }
                }
            });
        }
    }

    /** The row with every VALUE mapped through {@code t} (Jena's NodeTransformLib maps the variables, not the values). */
    private static Binding transformValues(Binding row, NodeTransform t) {
        BindingBuilder bb = Binding.builder();
        row.forEach((v, node) -> bb.add(v, t.apply(node)));
        return bb.build();
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
        return execute(ds, queryStr, baseURI, acceptHeader, resp, null);
    }

    /**
     * As {@link #execute(Dataset, String, String, String, HttpServletResponse)},
     * with the request's protocol dataset ({@link #extractDatasetDescription})
     * applied over the query's FROM / FROM NAMED clauses when non-null.
     */
    public static boolean execute(Dataset ds, String queryStr, String baseURI,
                               String acceptHeader, HttpServletResponse resp,
                               DatasetDescription protocolDataset) throws IOException {
        String accept = (acceptHeader == null) ? "" : acceptHeader.toLowerCase(Locale.ROOT);
        ResponseOutput out = null;
        try {
            // DELIBERATE: no Syntax argument, so Jena's default (syntaxARQ)
            // applies. Do NOT "upgrade" this to Syntax.syntaxSPARQL_12 - it
            // looks obviously correct and is a regression: syntaxARQ already
            // parses SPARQL 1.2 triple terms, reifiers-as-annotations, TRIPLE/
            // SUBJECT/OBJECT etc., AND the CDT extensions (UNFOLD/FOLD), while
            // syntaxSPARQL_12 drops UNFOLD from the grammar and silently breaks
            // the supported CDT surface (the syntaxARQ decision, CHANGELOG.md "Format v5 design notes").
            // The served URL is the query's base as well: a client naming the
            // document by relative reference (<>, <image.png>) must reach the
            // stored relative term through the same relativization, not the
            // JVM's working directory as a file: IRI (BG-41). The two-argument
            // overload keeps the default syntax; a client's own BASE still wins.
            RelativeIRIResolver resolver = new RelativeIRIResolver(baseURI);
            Query query = resolver.isActive() ? QueryFactory.create(queryStr, baseURI) : QueryFactory.create(queryStr);
            applyProtocolDataset(query, protocolDataset);
            // Relativize document IRIs the query names so they match the
            // dictionary. Skip the whole-query walk when there is no base.
            Query execQuery = query;
            if (resolver.isActive()) {
                NodeTransform toStorage = resolver.absoluteToStorage(storedTermProbe(ds));
                execQuery = QueryTransformOps.transform(query, toStorage);
                rewriteOutsideThePattern(execQuery, toStorage);
            }
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
                    // Negotiated like SELECT, through Jena's formatters: the
                    // hand-written {"boolean":b} lacked the mandatory "head"
                    // member and ignored the Accept header (BG-42).
                    boolean b = qexec.execAsk();
                    if (accept.contains("json")) {
                        resp.setContentType("application/sparql-results+json");
                        out = new ResponseOutput(resp.getOutputStream());
                        ResultSetFormatter.outputAsJSON(out, b);
                    } else if (accept.contains("csv")) {
                        resp.setContentType("text/csv");
                        out = new ResponseOutput(resp.getOutputStream());
                        ResultSetFormatter.outputAsCSV(out, b);
                    } else {
                        resp.setContentType("application/sparql-results+xml");
                        out = new ResponseOutput(resp.getOutputStream());
                        ResultSetFormatter.outputAsXML(out, b);
                    }
                } else if (execQuery.isConstructType() || execQuery.isDescribeType()) {
                    Iterator<Triple> triples = resolver.resolve(execQuery.isConstructType()
                            ? qexec.execConstructTriples() : qexec.execDescribeTriples());
                    boolean ntriples = accept.contains("n-triples") || accept.contains("ntriples");
                    if (accept.contains("turtle") || ntriples) {
                        // Streamed triple by triple: no Model, no size limit.
                        // (Turtle carries RDF 1.2 triple terms natively.)
                        resp.setContentType(ntriples ? "application/n-triples" : "text/turtle");
                        out = new ResponseOutput(resp.getOutputStream());
                        StreamRDF stream = StreamRDFWriter.getWriterStream(out,
                                ntriples ? RDFFormat.NTRIPLES : RDFFormat.TURTLE_BLOCKS);
                        stream.start();
                        if (!ntriples) {
                            execQuery.getPrefixMapping().getNsPrefixMap().forEach(stream::prefix);
                        }
                        while (triples.hasNext()) {
                            stream.triple(triples.next());
                        }
                        stream.finish();
                    } else {
                        // JSON-LD and RDF/XML are whole-document formats: the
                        // result must be materialized, so it is capped.
                        long cap = constructMaxTriples();
                        Model m = ModelFactory.createDefaultModel();
                        m.setNsPrefixes(execQuery.getPrefixMapping());
                        long n = 0;
                        boolean tooLarge = false;
                        while (triples.hasNext()) {
                            Triple t = triples.next();
                            if (cap > 0 && ++n > cap) {
                                tooLarge = true;
                                break;
                            }
                            m.getGraph().add(t);
                        }
                        if (tooLarge) {
                            resp.sendError(413, "Result exceeds " + cap + " triples, the limit for a response format "
                                    + "that must be held in memory; request text/turtle or application/n-triples, "
                                    + "which are streamed (server limit; adjustable with beakgraph.query.construct.max.triples)");
                        } else if (containsTripleTerms(m)) {
                            // JSON-LD and RDF/XML have no RDF 1.2 triple-term
                            // syntax; refuse plainly rather than emit a corrupt
                            // body or an opaque serializer failure.
                            resp.sendError(400, "Result contains RDF 1.2 triple terms, which this response format "
                                    + "cannot represent; request text/turtle");
                        } else if (accept.contains("json")) {
                            resp.setContentType("application/ld+json");
                            out = new ResponseOutput(resp.getOutputStream());
                            RDFDataMgr.write(out, m, RDFFormat.JSONLD);
                        } else {
                            resp.setContentType("application/rdf+xml");
                            out = new ResponseOutput(resp.getOutputStream());
                            RDFDataMgr.write(out, m, RDFFormat.RDFXML);
                        }
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
