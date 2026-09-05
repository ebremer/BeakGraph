package com.ebremer.beakgraph.core.fuseki;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.beakgraph.lws.LWSMetadataRefresher;
import java.util.function.Supplier;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import com.ebremer.ns.LWS;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only LWS (W3C Linked Web Storage) storage endpoint over a generated
 * metadata model + on-disk files. The unauthenticated read surface follows the
 * LWS Protocol 1.0 draft (w3c.github.io/lws-protocol/lws10-core/):
 *
 * <ul>
 *   <li>Container GETs return the LWS container representation (JSON-LD with
 *       the {@code https://www.w3.org/ns/lws/v1} context: id / type /
 *       totalItems / items) negotiated among {@code application/lws+json},
 *       {@code application/ld+json} and {@code application/json} with an
 *       identical body; Turtle and HTML remain as additional representations.</li>
 *   <li>Pagination is link-based per the spec: when membership exceeds
 *       {@link #PAGE_SIZE}, responses carry {@code Link} headers with
 *       {@code rel="first"/"last"/"next"/"prev"}; the body's {@code items}
 *       holds only the current page while {@code totalItems} stays the full
 *       count. A bare GET of a large container IS the first page.</li>
 *   <li>Every GET/HEAD response carries {@code rel="linkset"},
 *       {@code rel="https://www.w3.org/ns/lws#storageDescription"},
 *       {@code rel="type"}, and (for non-root resources) {@code rel="up"}
 *       Link headers, plus ETag and Last-Modified with conditional-request
 *       (304) support.</li>
 *   <li>Data resources support single-range requests (206/416,
 *       {@code Accept-Ranges: bytes}).</li>
 * </ul>
 *
 * Write operations are not served: this deployment is read-only, so linkset
 * responses honestly advertise {@code Allow: GET, HEAD} rather than PATCH.
 */
public class LWSStorageServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    /**
     * Optional configured public base (the CLI's {@code -base}), ending with '/'.
     * Null - the default - means every response derives its base from the request
     * it answers; see {@link #liveBase}.
     */
    private static volatile String BASE;
    private static Path STORAGE_ROOT;
    /** The metadata snapshot in use: fetched per lookup, swapped wholesale by the refresher, never mutated. */
    private final transient Supplier<Model> models;
    /** Re-scans the storage tree when a request names a path the snapshot does not know (null: static model). */
    private final transient LWSMetadataRefresher refresher;
    private static final String HTTP_ROOT = LWSMetadataGenerator.CANONICAL_BASE;
    private static final Resource LWS_CONTAINER = LWS.Container;
    private static final Property LWS_ITEMS = LWS.items;
    private static final Property AS_MEDIA_TYPE = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#mediaType");
    private static final Property SCHEMA_SIZE = ResourceFactory.createProperty("https://schema.org/size");
    private static final Property AS_UPDATED = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#updated");
    // Body-embedded pagination navigation (mirrors of the normative Link headers;
    // the as: prefix is defined by the lws/v1 context, so "as:next" etc. expand
    // to real IRIs under JSON-LD processing).
    private static final Property AS_FIRST = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#first");
    private static final Property AS_LAST = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#last");
    private static final Property AS_NEXT = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#next");
    private static final Property AS_PREV = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#prev");
    private static final Logger logger = LoggerFactory.getLogger(LWSStorageServlet.class);

    /** LWS media type for container representations and the storage description. */
    static final String LWS_JSON = "application/lws+json";
    /** rel value for storage-description discovery (the full URI, per LWS Discovery). */
    static final String REL_STORAGE_DESCRIPTION = "https://www.w3.org/ns/lws#storageDescription";
    /** Server-determined page size; containers larger than this paginate. */
    static final int PAGE_SIZE = 5;

    /** Container listing ETags, valid for one metadata snapshot: cleared when the refresher swaps it. */
    private final transient Map<String, String> etagCache = new ConcurrentHashMap<>();
    private transient volatile Model etagSnapshot;
    /** Stable Last-Modified fallback for metadata without a timestamp. */
    private final long initTime = System.currentTimeMillis();

    /** Serves one fixed metadata model (no refresh). */
    public LWSStorageServlet(Model model) {
        this(() -> model, null);
    }

    /** Serves whatever metadata snapshot the refresher currently holds. */
    public LWSStorageServlet(LWSMetadataRefresher refresher) {
        this(refresher::current, refresher);
    }

    private LWSStorageServlet(Supplier<Model> models, LWSMetadataRefresher refresher) {
        this.models = models;
        this.refresher = refresher;
    }

    private Model model() {
        return models.get();
    }

    /**
     * Resolves a canonical resource URI against the current metadata snapshot.
     * A URI the snapshot does not know, but whose file exists on disk, was added
     * since the last scan: the refresher is asked to re-scan (it does so only if
     * the file is newer than its last scan) and the lookup is retried against
     * the then-current snapshot, so a freshly copied file is servable at once
     * rather than after the next periodic scan. Returns null when the resource
     * is unknown.
     */
    private Resource lookup(String resourceURI) {
        Model m = model();
        Resource r = m.getResource(resourceURI);
        if (m.containsResource(r)) return r;
        if (refresher != null && STORAGE_ROOT != null && resourceURI.startsWith(HTTP_ROOT)) {
            String rel = resourceURI.substring(HTTP_ROOT.length());
            if (rel.startsWith("/")) rel = rel.substring(1);
            Path onDisk = resolveWithin(STORAGE_ROOT, rel);
            if (onDisk != null && Files.exists(onDisk)) {
                refresher.refreshOnDemand(onDisk);
                // Re-check whatever is current now: a concurrent request may have
                // published the snapshot that lists this file already.
                m = model();
                r = m.getResource(resourceURI);
                if (m.containsResource(r)) return r;
            }
        }
        return null;
    }
    /**
     * Pins the public base every response advertises (normalized to end with
     * '/'), or clears it with null so the base is derived per request again.
     */
    public static void setBase(String b) {
        BASE = (b == null || b.isBlank()) ? null : (b.endsWith("/") ? b : b + "/");
    }

    /**
     * The base URL clients reach this server on, ending with '/'. It is the
     * configured public base when {@link #setBase} was given one (the CLI's
     * {@code -base}, for a reverse proxy that does not forward the original
     * host); otherwise it is derived from the request's own scheme, host and
     * port, so a client on another machine gets links it can follow - a fixed
     * {@code http://localhost:<port>/} used to send every remote client's next,
     * up, linkset and storage-description links to its own loopback. Behind a
     * proxy that sends {@code Forwarded} / {@code X-Forwarded-*},
     * {@link #honourForwardedHeaders} makes the request report the public
     * origin here.
     */
    static String liveBase(HttpServletRequest req) {
        String configured = BASE;
        if (configured != null) {
            return configured;
        }
        // scheme://authority/path with default ports omitted - the container
        // builds it from the (forwarded) scheme, host and port of this request.
        String url = req.getRequestURL().toString();
        int authorityStart = url.indexOf("//") + 2;
        int pathStart = url.indexOf('/', authorityStart);
        String origin = pathStart < 0 ? url : url.substring(0, pathStart);
        String ctx = req.getContextPath();
        return origin + (ctx == null ? "" : ctx) + "/";
    }

    /**
     * Makes every HTTP connector of {@code server} apply the standard
     * {@code Forwarded} / {@code X-Forwarded-*} headers to the request's scheme,
     * host and port, so links minted by {@link #liveBase} behind a reverse proxy
     * carry the public origin. Call before the server starts.
     */
    static void honourForwardedHeaders(Server server) {
        for (Connector c : server.getConnectors()) {
            HttpConnectionFactory http = c.getConnectionFactory(HttpConnectionFactory.class);
            if (http != null) {
                http.getHttpConfiguration().addCustomizer(new ForwardedRequestCustomizer());
            }
        }
    }

    public static void setStorageRoot(Path root) {
        STORAGE_ROOT = root;
    }
    private boolean isHDF5(Path file) {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".h5");
    }

    private boolean isSparqlRequest(HttpServletRequest req) {
        String method = req.getMethod();
        String ct = req.getContentType();
        if ("POST".equals(method) && ct != null) {
            String ctl = ct.toLowerCase(Locale.ROOT);
            if (ctl.startsWith("application/sparql-query")) return true;
            if (ctl.startsWith("application/x-www-form-urlencoded") && req.getParameter("query") != null) return true;
      }
      return "GET".equals(method) && req.getParameter("query") != null;
  }
    private void handleSparqlQuery(HttpServletRequest req, HttpServletResponse resp, String reqPath, Path h5File) throws IOException {
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
            // Resolve document-relative IRIs against the URL this .h5 is served
            // from, on the same live base the advertised links use - so result
            // IRIs and navigation links agree (and both follow -base when set).
            healthy = BGSparqlService.execute(bg.getDataset(), queryStr,
                    liveBase(req) + encodeHref(reqPath), req.getHeader("Accept"), resp,
                    BGSparqlService.extractDatasetDescription(req));
        } catch (BGSparqlService.QueryExecutionFailedException ex) {
            // Failure after the response committed: nothing may touch the response
            // now. Rethrow (after the finally settles the reader) so the container
            // aborts the connection instead of finishing a truncated 200 body as
            // if it were complete. Whether the READER is at fault is the service's
            // call: a post-commit timeout or a query-level error leaves it healthy
            // and it goes back to the pool; only an unexplained failure invalidates.
            healthy = ex.isReaderHealthy();
            throw ex;
        } catch (java.util.NoSuchElementException exhausted) {
            // The pool has no free reader for this store within its wait: the
            // store is busy, not broken. Say so and invite a retry instead of
            // reporting an internal error (bg is null here - nothing to return).
            logger.warn("No pooled reader available for {} within the pool's wait: {}", h5File, exhausted.getMessage());
            if (!resp.isCommitted()) {
                resp.setHeader("Retry-After", "1");
                resp.sendError(503, "Store busy: all readers in use, retry shortly");
            }
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
    /**
     * The linkset URI of a resource: {@code <resource>.meta}. The live root is
     * the bare base ending in '/', whose linkset is {@code <base>/.meta} - the
     * path the .meta handler resolves to the root container. Stripping the
     * slash first, as this used to, minted {@code http://host:port.meta}, a
     * different host entirely.
     */
    private String getLinksetURI(String resourceURI) {
        return resourceURI + ".meta";
    }
    private void serveLinkset(HttpServletResponse resp, String base, String resourceURI) throws IOException {
        Resource r = lookup(resourceURI);
        if (r == null) {
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
        // Read-only deployment: advertise exactly the methods this linkset supports.
        resp.setHeader("Allow", "GET, HEAD");
        addStorageDescriptionLink(resp, base);
        // anchor/up are advertised on the LIVE base, never the canonical one.
        resp.getWriter().write(linksetJson(toLiveUri(resourceURI, base), typeHref,
                up == null ? null : toLiveUri(up, base), media, size, updated));
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

    /**
     * The LWS storage description document. Data model per LWS Discovery: id +
     * type "Storage" + services, including the mandatory StorageDescription
     * service pointing at this document.
     */
    static String descriptionJson(String base) {
        JsonObject doc = Json.createObjectBuilder()
            .add("@context", "https://www.w3.org/ns/lws/v1")
            .add("id", base)
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

    /**
     * Container page URI on the live base; reqPath carries no leading slash and
     * base ends with '/'. The path is percent-encoded: reqPath is the DECODED
     * path, and a subcontainer named "my slides" must yield
     * {@code .../my%20slides?page=2}, not a Link target with a raw space that
     * clients reject or mangle.
     */
    private static String pageUri(String base, String reqPath, int page) {
        return base + encodeHref(reqPath) + "?page=" + page;
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

    /**
     * Parse a single-range {@code Range: bytes=...} header against a resource of
     * {@code size} bytes. Returns {@code null} when the header is absent,
     * malformed, multi-range, or not a bytes range (the request is then served
     * whole, as RFC 9110 permits); a {start,end} pair when satisfiable; and
     * {@code {-1,-1}} when the range is syntactically valid but unsatisfiable
     * (416 with {@code Content-Range: bytes *}{@code /size}).
     */
    static long[] parseRange(String header, long size) {
        if (header == null || !header.startsWith("bytes=") || header.contains(",")) {
            return null;
        }
        String spec = header.substring("bytes=".length()).trim();
        int dash = spec.indexOf('-');
        if (dash < 0) return null;
        String startStr = spec.substring(0, dash).trim();
        String endStr = spec.substring(dash + 1).trim();
        try {
            if (startStr.isEmpty()) {
                // suffix form: last N bytes
                if (endStr.isEmpty()) return null;
                long suffix = Long.parseLong(endStr);
                if (suffix <= 0 || size == 0) return new long[]{-1, -1};
                long start = Math.max(0, size - suffix);
                return new long[]{start, size - 1};
            }
            long start = Long.parseLong(startStr);
            if (start < 0) return null;
            if (start >= size) return new long[]{-1, -1};
            long end = endStr.isEmpty() ? size - 1 : Long.parseLong(endStr);
            if (end < start) return null;
            return new long[]{start, Math.min(end, size - 1)};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** rel="https://www.w3.org/ns/lws#storageDescription" on every storage response (LWS Discovery). */
    private void addStorageDescriptionLink(HttpServletResponse resp, String base) {
        resp.addHeader("Link", "<" + base + "description>; rel=\"" + REL_STORAGE_DESCRIPTION + "\"");
    }

    /** The Link headers every resource/container response must carry. */
    private void addCommonLinks(HttpServletResponse resp, String base, String resourceURI, boolean isContainer) {
        resp.addHeader("Link", "<" + getLinksetURI(toLiveUri(resourceURI, base)) + ">; rel=\"linkset\"; type=\"application/linkset+json\"");
        addStorageDescriptionLink(resp, base);
        resp.addHeader("Link", "<" + (isContainer ? LWS.Container.getURI() : LWS.DataResource.getURI()) + ">; rel=\"type\"");
        String up = getParentURI(resourceURI);
        if (up != null) {
            resp.addHeader("Link", "<" + toLiveUri(up, base) + ">; rel=\"up\"");
        }
    }

    /** Items sorted by URI: pagination needs a stable order across requests. */
    private List<Resource> sortedItems(Resource container) {
        List<Resource> items = container.listProperties(LWS_ITEMS).mapWith(Statement::getResource).toList();
        items.sort(Comparator.comparing(Resource::getURI));
        return items;
    }

    /**
     * Listing-version ETag (changes when the generated membership metadata
     * changes). The model is immutable per servlet instance, so it is cached.
     */
    private String containerEtag(Resource r, String resourceURI, List<Resource> items) {
        // Validators are cached per snapshot: a refresh publishes a new model, and
        // listings computed from it must not revalidate against the old ETags.
        Model snapshot = r.getModel();
        if (snapshot != etagSnapshot) {
            synchronized (etagCache) {
                if (snapshot != etagSnapshot) {
                    etagCache.clear();
                    etagSnapshot = snapshot;
                }
            }
        }
        return etagCache.computeIfAbsent(resourceURI, k -> {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(resourceURI.getBytes(StandardCharsets.UTF_8));
                // The page size shapes the representation (which items land on
                // which page): without it in the validator, changing PAGE_SIZE
                // between server runs would 304-revalidate clients' stale slicings.
                md.update(Integer.toString(PAGE_SIZE).getBytes(StandardCharsets.UTF_8));
                md.update(Long.toString(items.size()).getBytes(StandardCharsets.UTF_8));
                Statement own = r.getProperty(AS_UPDATED);
                if (own != null) md.update(own.getString().getBytes(StandardCharsets.UTF_8));
                for (Resource it : items) {
                    md.update(it.getURI().getBytes(StandardCharsets.UTF_8));
                    Statement mod = it.getProperty(AS_UPDATED);
                    if (mod != null) md.update(mod.getString().getBytes(StandardCharsets.UTF_8));
                }
                byte[] d = md.digest();
                StringBuilder sb = new StringBuilder("\"");
                for (int i = 0; i < 16; i++) sb.append(String.format("%02x", d[i]));
                return sb.append('"').toString();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e); // SHA-256 is mandatory in every JRE
            }
        });
    }

    /** Container Last-Modified: its own timestamp, else the newest member's, else server start. */
    private long containerLastModified(Resource r, List<Resource> items) {
        long best = parseInstantMillis(r.getProperty(AS_UPDATED));
        for (Resource it : items) {
            best = Math.max(best, parseInstantMillis(it.getProperty(AS_UPDATED)));
        }
        return best > 0 ? best : initTime;
    }

    private static long parseInstantMillis(Statement s) {
        if (s == null) return -1;
        try {
            return Instant.parse(s.getString()).toEpochMilli();
        } catch (RuntimeException e) {
            return -1;
        }
    }

    /** True (and 304 sent) when the request's validators match. Headers must be set first. */
    private static boolean notModified(HttpServletRequest req, HttpServletResponse resp, String etag, long lastModified) {
        String ifNoneMatch = req.getHeader("If-None-Match");
        long ifModifiedSince = req.getDateHeader("If-Modified-Since");
        if (etag.equals(ifNoneMatch)
                || (ifNoneMatch == null && ifModifiedSince >= 0 && lastModified / 1000 <= ifModifiedSince / 1000)) {
            resp.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return true;
        }
        return false;
    }

    /** A {@code {"id": <uri>}} node reference ("id" maps to @id in the lws/v1 context). */
    private static JsonObjectBuilder nodeRef(String uri) {
        return Json.createObjectBuilder().add("id", uri);
    }

    /**
     * The LWS container representation (JSON-LD, {@code https://www.w3.org/ns/lws/v1}
     * context): id / type / totalItems reflect the full membership; {@code items}
     * holds the (possibly paginated) current page. When paginated, the
     * navigation URIs are ALSO embedded as {@code as:first/as:last/as:next/as:prev}
     * node references - a convenience mirror of the normative Link headers
     * (extra representation properties the spec does not forbid), so the body
     * alone is navigable.
     */
    private String containerJson(String base, String liveId, long totalItems, List<Resource> pageItems,
                                 String first, String last, String next, String prev) {
        JsonObjectBuilder root = Json.createObjectBuilder()
            .add("@context", "https://www.w3.org/ns/lws/v1")
            .add("id", liveId)
            .add("type", "Container")
            .add("totalItems", totalItems);
        if (first != null) root.add("as:first", nodeRef(first));
        if (last != null)  root.add("as:last", nodeRef(last));
        if (next != null)  root.add("as:next", nodeRef(next));
        if (prev != null)  root.add("as:prev", nodeRef(prev));
        JsonArrayBuilder arr = Json.createArrayBuilder();
        for (Resource it : pageItems) {
            JsonObjectBuilder o = Json.createObjectBuilder();
            boolean isC = it.hasProperty(RDF.type, LWS_CONTAINER);
            o.add("id", toLiveUri(it.getURI(), base));
            o.add("type", isC ? "Container" : "DataResource");
            if (!isC) {
                // mediaType is REQUIRED for DataResources in the representation.
                Statement m = it.getProperty(AS_MEDIA_TYPE);
                o.add("mediaType", m != null ? m.getString() : "application/octet-stream");
            }
            Statement sz = it.getProperty(SCHEMA_SIZE);
            if (sz != null) {
                try {
                    o.add("size", sz.getLong());
                } catch (RuntimeException ignore) {
                    // a malformed size literal is dropped, not fatal (size is a SHOULD)
                }
            }
            Statement mod = it.getProperty(AS_UPDATED);
            if (mod != null) o.add("modified", mod.getString());
            arr.add(o);
        }
        root.add("items", arr);
        return writeJson(root.build());
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        boolean isHead = "HEAD".equals(req.getMethod());
        // Every absolute URI this response advertises is minted on the base the
        // client reached us on (or the configured -base).
        final String base = liveBase(req);
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
                resp.setHeader("Allow", "GET, HEAD");
                addStorageDescriptionLink(resp, base);
                resp.getWriter().write(linksetJson(base + "description",
                        LWS.DataResource.getURI(), base, null, null, null));
                return;
            }
            String baseResourceURI = basePath.isEmpty() ? HTTP_ROOT : HTTP_ROOT + "/" + basePath;
            serveLinkset(resp, base, baseResourceURI);
            return;
        }
        if (reqPath.equals("description")) {
            // The storage description MUST be available as application/lws+json;
            // ld+json / json are equivalent bodies with a different Content-Type.
            String acceptHdr = req.getHeader("Accept") != null ? req.getHeader("Accept").toLowerCase(Locale.ROOT) : "";
            String ct = acceptHdr.contains("ld+json") && !acceptHdr.contains("lws+json")
                    ? "application/ld+json"
                    : (acceptHdr.contains("lws+json") || acceptHdr.isEmpty() || !acceptHdr.contains("json"))
                        ? LWS_JSON : "application/json";
            resp.setContentType(ct);
            addStorageDescriptionLink(resp, base);
            // addHeader, not setHeader: a second setHeader replaces the first Link
            // header, which silently dropped the storageDescription link.
            resp.addHeader("Link", "<" + getLinksetURI(base + "description") + ">; rel=\"linkset\"; type=\"application/linkset+json\"");
            resp.setHeader("Vary", "Accept");
            if (!isHead) resp.getWriter().write(descriptionJson(base));
            return;
        }
        if (reqPath.startsWith("HalcyonStorage")) {
            reqPath = reqPath.substring("HalcyonStorage".length());
            if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        }
        String resourceURI = reqPath.isEmpty() ? HTTP_ROOT : HTTP_ROOT + "/" + reqPath;
        Resource r = lookup(resourceURI);
        if (r == null) {
            resp.sendError(404, "Resource not found: " + resourceURI);
            return;
        }
        // SPARQL on .h5 files checked FIRST (fixes Jena Accept header triggering metadata instead of query)
        Path h5Candidate = resolveWithin(STORAGE_ROOT, reqPath);
        if (h5Candidate != null && Files.exists(h5Candidate) && !Files.isDirectory(h5Candidate)
                && isHDF5(h5Candidate) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, reqPath, h5Candidate);
            return;
        }
        boolean isContainer = r.hasProperty(RDF.type, LWS_CONTAINER);
        addCommonLinks(resp, base, resourceURI, isContainer);
        resp.setHeader("Vary", "Accept");
        String accept = req.getHeader("Accept") != null ? req.getHeader("Accept").toLowerCase(Locale.ROOT) : "";
        String formatParam = req.getParameter("format");
        boolean forceTurtle = "turtle".equalsIgnoreCase(formatParam);
        boolean forceJsonLd = "jsonld".equalsIgnoreCase(formatParam);
        boolean wantHtml = acceptsHtmlRepresentation(accept) && !forceTurtle && !forceJsonLd;
        boolean wantTurtle = accept.contains("turtle") || forceTurtle;
        boolean wantJson = (accept.contains("ld+json") || accept.contains("json")) || forceJsonLd;

        if (isContainer) {
            List<Resource> items = sortedItems(r);
            int total = items.size();

            // ---- Pagination (link-based, per LWS): the bare container URI IS the
            // first page once membership exceeds the threshold; ?page=N addresses
            // pages directly. Navigation travels in Link headers only.
            int page = 1;
            boolean paginated = total > PAGE_SIZE;
            String pageStr = req.getParameter("page");
            if (pageStr != null) {
                try {
                    page = Integer.parseInt(pageStr.trim());
                } catch (NumberFormatException e) {
                    resp.sendError(400, "Invalid 'page' parameter: " + pageStr);
                    return;
                }
                if (page < 1) { resp.sendError(400, "'page' must be >= 1"); return; }
                paginated = true;
                long start = (long)(page-1) * PAGE_SIZE;   // long: a huge page must not overflow to a negative index
                if (start >= total && total > 0) { resp.sendError(404); return; }
                if (total == 0 && page > 1) { resp.sendError(404); return; }
            }
            List<Resource> pageItems = items;
            String firstUri = null, lastUri = null, nextUri = null, prevUri = null;
            if (paginated) {
                int pages = Math.max(1, (total + PAGE_SIZE - 1) / PAGE_SIZE);
                int startIdx = (page - 1) * PAGE_SIZE;
                pageItems = items.subList(Math.min(startIdx, total), Math.min(startIdx + PAGE_SIZE, total));
                firstUri = pageUri(base, reqPath, 1);
                lastUri = pageUri(base, reqPath, pages);
                if (page < pages) nextUri = pageUri(base, reqPath, page + 1);
                if (page > 1)     prevUri = pageUri(base, reqPath, page - 1);
                // Normative navigation (LWS): Link headers. The same URIs are
                // mirrored into the JSON-LD / Turtle bodies below.
                resp.addHeader("Link", "<" + firstUri + ">; rel=\"first\"");
                resp.addHeader("Link", "<" + lastUri + ">; rel=\"last\"");
                if (nextUri != null) resp.addHeader("Link", "<" + nextUri + ">; rel=\"next\"");
                if (prevUri != null) resp.addHeader("Link", "<" + prevUri + ">; rel=\"prev\"");
            }

            // ---- Listing-version validators + conditional GET (ETag is
            // membership-scoped: the listing version, not the page).
            String etag = containerEtag(r, resourceURI, items);
            long lastModified = containerLastModified(r, items);
            resp.setHeader("ETag", etag);
            resp.setDateHeader("Last-Modified", lastModified);
            // no-cache = "revalidate before reuse", not "don't cache": without it,
            // heuristic freshness (from Last-Modified) lets browsers replay stale
            // listings for days without ever asking the server again.
            resp.setHeader("Cache-Control", "no-cache");
            if (notModified(req, resp, etag, lastModified)) return;

            String liveSelf = toLiveUri(resourceURI, base);

            if (wantHtml) {
                resp.setContentType("text/html; charset=utf-8");
                // The request path and item names come from the URL / the filesystem;
                // escape them for HTML and percent-encode hrefs (stored-XSS sink).
                String safePath = escapeHtml(reqPath);
                if (isHead) return;
                try (PrintWriter out = resp.getWriter()) {
                    out.println("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>LWS Storage – /" + safePath + "</title>");
                    out.println("<style>body{font-family:sans-serif;margin:40px} ul{list-style:none;padding:0} a{color:#0066cc}</style></head><body>");
                    out.println("<h1><img src=\"/sparql/beakgraph.png\" width=\"100\"> Linked Web Storage: /" + safePath + "</h1>");
                    out.println("<p>");
                    String up = getParentURI(resourceURI);
                    if (up != null) {
                        // Same target as the rel="up" Link header, percent-encoded
                        // like every other href (parent names may need it).
                        String upRest = up.substring(HTTP_ROOT.length());
                        while (upRest.startsWith("/")) upRest = upRest.substring(1);
                        out.println("<a href=\"" + escapeHtml(base + encodeHref(upRest)) + "\">&#8679; Parent</a> | ");
                    }
                    out.println("<a href=\"" + escapeHtml(base) + "description\">Storage Description</a> | ");
                    out.println("<a href=\"?format=turtle\">Turtle</a> | <a href=\"?format=jsonld\">JSON-LD</a> | ");
                    out.println("<a href=\"/sparql/index.html\" target=\"_blank\">SPARQL Endpoint</a></p><hr>");
                    if (pageItems.isEmpty()) {
                        out.println("<p><em>Empty container.</em></p>");
                    } else {
                        out.println("<ul>");
                        for (Resource item : pageItems) {
                            String name = item.getURI().substring(HTTP_ROOT.length());
                            if (name.isEmpty()) name = "(root)";
                            String link = name.startsWith("/") ? name : "/" + name;
                            out.printf("<li><a href=\"%s\">%s</a></li>%n",
                                    escapeHtml(encodeHref(link)), escapeHtml(name));
                        }
                        out.println("</ul>");
                    }
                    if (paginated) {
                        int pages = Math.max(1, (total + PAGE_SIZE - 1) / PAGE_SIZE);
                        out.println("<hr><p>");
                        if (page > 1) out.println("<a href=\"?page=" + (page - 1) + "\">&laquo; Prev</a> ");
                        out.println("Page " + page + " of " + pages + " (" + total + " items)");
                        if (page < pages) out.println(" <a href=\"?page=" + (page + 1) + "\">Next &raquo;</a>");
                        out.println("</p>");
                    }
                    out.println("</body></html>");
                }
                return;
            }
            if (wantJson && !wantTurtle) {
                // LWS container representation: identical body for all three JSON
                // flavors, only the Content-Type varies (spec MUST).
                String ct = forceJsonLd ? "application/ld+json"
                        : accept.contains("lws+json") ? LWS_JSON
                        : accept.contains("ld+json") ? "application/ld+json"
                        : "application/json";
                resp.setContentType(ct);
                resp.setCharacterEncoding("UTF-8");
                if (!isHead) resp.getWriter().write(
                        containerJson(base, liveSelf, total, pageItems, firstUri, lastUri, nextUri, prevUri));
                return;
            }
            if (wantTurtle) {
                // Additional RDF representation: the same information as the LWS
                // shape, expressed as model triples (page-scoped items).
                Model out = ModelFactory.createDefaultModel();
                Resource httpR = out.createResource(liveSelf);
                r.listProperties().forEachRemaining(s -> {
                    RDFNode obj = s.getObject();
                    if (!exposableToClient(obj)) return; // never leak file:/// server paths
                    if (s.getPredicate().equals(LWS_ITEMS)) return; // page-scoped below
                    if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                        httpR.addProperty(s.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI(), base)));
                    } else {
                        httpR.addProperty(s.getPredicate(), obj);
                    }
                });
                // Body-embedded navigation, mirroring the Link headers.
                if (firstUri != null) httpR.addProperty(AS_FIRST, out.createResource(firstUri));
                if (lastUri != null)  httpR.addProperty(AS_LAST, out.createResource(lastUri));
                if (nextUri != null)  httpR.addProperty(AS_NEXT, out.createResource(nextUri));
                if (prevUri != null)  httpR.addProperty(AS_PREV, out.createResource(prevUri));
                for (Resource it : pageItems) {
                    String itHttp = toLiveUri(it.getURI(), base);
                    httpR.addProperty(LWS_ITEMS, out.createResource(itHttp));
                    it.listProperties().forEachRemaining(st -> {
                        RDFNode obj = st.getObject();
                        if (!exposableToClient(obj)) return; // never leak file:/// server paths
                        if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                            out.getResource(itHttp).addProperty(st.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI(), base)));
                        } else {
                            out.getResource(itHttp).addProperty(st.getPredicate(), obj);
                        }
                    });
                }
                resp.setContentType("text/turtle");
                if (!isHead) RDFDataMgr.write(resp.getOutputStream(), out, RDFFormat.TURTLE);
                return;
            }
            resp.sendError(406);
            return;
        }

        // ---- Non-container (data resource) ----
        // GET on a DataResource returns its STORED representation (LWS). The RDF
        // description of the resource - its metadata triples - is a different
        // document and is served only on explicit request (?format=turtle or
        // ?format=jsonld); it is also always reachable through the linkset at
        // <resource>.meta. It must never be selected by the Accept header: doing
        // so handed a client that asked for text/turtle or application/json (Jena's
        // default RDF Accept header, for one) the description INSTEAD of a stored
        // .ttl/.json file - a well-formed 200 carrying the wrong data. Content
        // negotiation does not apply to a resource with a single representation;
        // per RFC 9110 the stored bytes are served whatever the Accept header says.
        if (forceTurtle || forceJsonLd) {
            // RDF description of the data resource (its metadata triples).
            Model out = ModelFactory.createDefaultModel();
            Resource httpR = out.createResource(base + (reqPath.isEmpty() ? "" : reqPath));
            r.listProperties().forEachRemaining(s -> {
                RDFNode obj = s.getObject();
                if (!exposableToClient(obj)) return; // never leak file:/// server paths
                if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                    httpR.addProperty(s.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI(), base)));
                } else {
                    httpR.addProperty(s.getPredicate(), obj);
                }
            });
            resp.setContentType(forceTurtle ? "text/turtle" : "application/ld+json");
            if (!isHead) RDFDataMgr.write(resp.getOutputStream(), out, forceTurtle ? RDFFormat.TURTLE : RDFFormat.JSONLD);
            return;
        }
        if (STORAGE_ROOT == null) { resp.sendError(500, "Storage root not set"); return; }
        Path localFile = resolveWithin(STORAGE_ROOT, reqPath);
        if (localFile == null) { resp.sendError(403, "Forbidden"); return; }
        if (!Files.exists(localFile) || Files.isDirectory(localFile)) { resp.sendError(404); return; }
        if (isHDF5(localFile) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, reqPath, localFile);
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
        resp.setHeader("Accept-Ranges", "bytes");
        if (notModified(req, resp, etag, lastModified)) return;
        resp.setContentType(media);
        // Stored bytes are served as a download: with nosniff above, this keeps an
        // HTML/SVG file someone placed under the root from rendering in this origin.
        String filename = localFile.getFileName().toString().replace("\"", "");
        resp.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");

        // ---- Range requests (RFC 9110 / LWS MUST). Single ranges only; anything
        // else is served whole, which RFC 9110 permits. Range is defined for GET.
        long[] range = isHead ? null : parseRange(req.getHeader("Range"), size);
        String ifRange = req.getHeader("If-Range");
        if (range != null && ifRange != null && !ifRange.equals(etag)) {
            range = null; // validator changed: send the full representation
        }
        if (range != null && range[0] == -1) {
            // setStatus, not sendError: sendError may reset the buffer/headers and
            // the 416 MUST carry Content-Range: bytes */size.
            resp.setStatus(416);
            resp.setHeader("Content-Range", "bytes */" + size);
            resp.setContentLength(0);
            return;
        }
        if (range != null) {
            long start = range[0];
            long end = range[1];
            long len = end - start + 1;
            resp.setStatus(206);
            resp.setHeader("Content-Range", "bytes " + start + "-" + end + "/" + size);
            resp.setContentLengthLong(len);
            try (InputStream in = Files.newInputStream(localFile)) {
                in.skipNBytes(start);
                copyBounded(in, resp.getOutputStream(), len);
            }
            return;
        }
        resp.setContentLengthLong(size);
        // HEAD gets the same headers without the body (and without the file copy).
        if (!isHead) {
            Files.copy(localFile, resp.getOutputStream());
        }
    }

    private static void copyBounded(InputStream in, OutputStream out, long len) throws IOException {
        byte[] buf = new byte[64 * 1024];
        long remaining = len;
        while (remaining > 0) {
            int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
            if (n < 0) throw new EOFException("File shrank while serving range");
            out.write(buf, 0, n);
            remaining -= n;
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setHeader("X-Content-Type-Options", "nosniff");
        // One line per request is access-log volume: DEBUG, so a long-running
        // -endpoint server does not grow its log file with every POST (BG-193).
        logger.debug("LWS {} {} ct={} query-param={} bodyLen={}",
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
        Resource r = lookup(resourceURI);
        if (r == null) {
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
            handleSparqlQuery(req, resp, reqPath, localFile);
            return;
        }
        resp.sendError(405, "Method not allowed");
    }
    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Same routing and headers as GET; each body-writing branch checks the
        // method and skips the body for HEAD.
        doGet(req, resp);
    }
}
