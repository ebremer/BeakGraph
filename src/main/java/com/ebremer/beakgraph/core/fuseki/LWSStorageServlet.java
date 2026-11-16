package com.ebremer.beakgraph.core.fuseki;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import com.ebremer.beakgraph.lws.LWSMetadataRefresher;
import java.util.function.Supplier;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import com.ebremer.ns.LWS;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.*;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonWriter;
import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
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
 * LWS Protocol 1.0 editor's draft of 2026-11-09
 * (w3c.github.io/lws-protocol/lws10-core/):
 *
 * <ul>
 *   <li>The storage URI ({@code <base>description}) serves the storage
 *       description - a Controlled Identifier document, {@code application/lws+cid}
 *       by default - whose StorageRoot service is the root container. It
 *       describes no MoveResource capability: no {@code up} link can change.</li>
 *   <li>Container GETs return the LWS container representation (JSON-LD with
 *       the {@code https://www.w3.org/ns/lws/v1} context: id / type /
 *       totalItems / items, each item with id / type / format / size /
 *       modified) as {@code application/lws+json}, and the same body as
 *       {@code application/ld+json} and {@code application/json}. A request
 *       for an RDF media type with the LWS profile ({@code application/ld+json}
 *       or {@code text/turtle} with {@code profile="https://www.w3.org/ns/lws/v1"})
 *       gets a representation RDF isomorphic to the {@code application/lws+json}
 *       one, labelled with that profile. Plain Turtle and HTML remain as
 *       additional representations.</li>
 *   <li>Pagination is link-based per the spec: when membership exceeds
 *       {@link #PAGE_SIZE}, responses carry {@code Link} headers with
 *       {@code rel="first"/"last"/"next"/"prev"}; the body's {@code items}
 *       holds only the current page while {@code totalItems} stays the full
 *       count. A bare GET of a large container IS the first page.</li>
 *   <li>Every GET/HEAD response carries a {@code rel="https://www.w3.org/ns/lws#storage"}
 *       Link to the storage URI; containers and data resources add
 *       {@code rel="linkset"}, {@code rel="type"} and (below the root)
 *       {@code rel="up"}. Every one carries an ETag and Last-Modified, and
 *       conditional requests are evaluated per RFC 9110 (304 / 412).</li>
 *   <li>Each resource's metadata is also an RFC 9264 linkset at
 *       {@code <resource>.meta}.</li>
 *   <li>Data resources support single-range requests (206/416,
 *       {@code Accept-Ranges: bytes}).</li>
 *   <li>Errors the servlet raises are RFC 9457 problem details.</li>
 * </ul>
 *
 * Write operations are not served: this deployment is read-only by design, so
 * PUT, PATCH, DELETE and POST (except a SPARQL query to a store) answer 405
 * with the {@code Allow} header naming what each resource does support, and
 * linkset responses advertise {@code Allow: GET, HEAD, OPTIONS} rather than
 * the PATCH a writable LWS server must offer.
 */
public class LWSStorageServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    /**
     * Optional configured public base (the CLI's {@code -base}), ending with '/'.
     * Null - the default - means every response derives its base from the request
     * it answers; see {@link #liveBase}. Per instance, like the storage root:
     * they used to be static fields behind static setters, so one servlet's
     * configuration was every servlet's (BG-323).
     */
    private final String configuredBase;
    /** The directory served (null: no data resources, e.g. single-file mode). */
    private final transient Path storageRoot;
    /** The metadata snapshot in use: fetched per lookup, swapped wholesale by the refresher, never mutated. */
    private final transient Supplier<Model> models;
    /** Re-scans the storage tree when a request names a path the snapshot does not know (null: static model). */
    private final transient LWSMetadataRefresher refresher;
    private static final String HTTP_ROOT = LWSMetadataGenerator.CANONICAL_BASE;
    private static final Resource LWS_CONTAINER = LWS.Container;
    private static final Property LWS_ITEMS = LWS.items;
    private static final Property FORMAT = LWS.format;
    private static final Property SCHEMA_SIZE = LWS.size;
    private static final Property MODIFIED = LWS.modified;
    // Plain-Turtle pagination navigation, mirroring the normative Link headers.
    // The JSON body and the LWS-profile RDF carry none: the LWS context
    // defines no term for them.
    private static final Property AS_FIRST = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#first");
    private static final Property AS_LAST = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#last");
    private static final Property AS_NEXT = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#next");
    private static final Property AS_PREV = ResourceFactory.createProperty("https://www.w3.org/ns/activitystreams#prev");
    private static final Logger logger = LoggerFactory.getLogger(LWSStorageServlet.class);

    /** LWS media type for container representations. */
    static final String LWS_JSON = "application/lws+json";
    /** LWS media type for the storage description (a Controlled Identifier document). */
    static final String LWS_CID = "application/lws+cid";
    /**
     * The LWS JSON-LD context. The same URI identifies the LWS profile (RFC
     * 6906): a container representation in an RDF media type conforms to it
     * when it is RDF isomorphic to the {@link #LWS_JSON} one.
     */
    static final String LWS_CONTEXT = "https://www.w3.org/ns/lws/v1";
    /**
     * The offered container media types a client can ask for in the LWS
     * profile: RDF media types that define a {@code profile} parameter
     * (LWS Content Negotiation lists JSON-LD, Turtle, TriG, N-Triples, N-Quads).
     */
    static final Set<String> LWS_PROFILE_TYPES = Set.of("application/ld+json", "text/turtle");
    /** The Controlled Identifier context, first in the storage description's {@code @context}. */
    static final String CID_CONTEXT = "https://www.w3.org/ns/cid/v1";
    /** rel of the Link to the storage URI that every GET/HEAD response carries (LWS Discovery and Binding). */
    static final String REL_STORAGE = LWS.storage.getURI();
    /** The storage URI, relative to the base: it serves the storage description. */
    static final String STORAGE_PATH = "description";
    /** The storage description's type for the SPARQL endpoint service: the LWS vocabulary has no term for one. */
    static final String SPARQL_SERVICE_TYPE = "http://www.w3.org/ns/sparql-service-description#Service";
    static final String LINKSET_JSON = "application/linkset+json";
    /** The methods every LWS resource here supports: this storage is read-only by design. */
    static final String ALLOW_READ = "GET, HEAD, OPTIONS";
    /** A served store also takes a SPARQL query by POST. */
    static final String ALLOW_QUERYABLE = "GET, HEAD, OPTIONS, POST";
    /** Server-determined page size; containers larger than this paginate. */
    static final int PAGE_SIZE = 5;

    /**
     * A container's listing as derived from one metadata snapshot: its items
     * sorted once, and the validators computed once. The model is immutable
     * per snapshot, so the whole view is memoised - every request used to
     * re-collect and re-sort the full membership (BG-257). The map is cleared
     * when the refresher swaps the snapshot.
     */
    private record ContainerView(List<Resource> items, String etag, long lastModified) {}
    private final transient Map<String, ContainerView> views = new ConcurrentHashMap<>();
    private transient volatile Model viewSnapshot;
    /** Stable Last-Modified fallback for metadata without a timestamp. */
    private final long initTime = System.currentTimeMillis();

    /**
     * Serves one fixed metadata model (no refresh).
     *
     * @param configuredBase the public base every response advertises, or null to derive it per request
     * @param storageRoot    the served directory, or null when there are no data resources
     */
    public LWSStorageServlet(Model model, String configuredBase, Path storageRoot) {
        this(() -> model, null, configuredBase, storageRoot);
    }

    /** Serves whatever metadata snapshot the refresher currently holds. */
    public LWSStorageServlet(LWSMetadataRefresher refresher, String configuredBase, Path storageRoot) {
        this(refresher::current, refresher, configuredBase, storageRoot);
    }

    private LWSStorageServlet(Supplier<Model> models, LWSMetadataRefresher refresher, String configuredBase, Path storageRoot) {
        this.models = Objects.requireNonNull(models, "models");
        this.refresher = refresher;
        this.configuredBase = normalizeBase(configuredBase);
        this.storageRoot = storageRoot;
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
        if (refresher != null && storageRoot != null && resourceURI.startsWith(HTTP_ROOT)) {
            String rel = resourceURI.substring(HTTP_ROOT.length());
            if (rel.startsWith("/")) rel = rel.substring(1);
            Path onDisk = resolveWithin(storageRoot, rel);
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
    /** A configured public base normalized to end with '/', or null for none (blank counts as none). */
    static String normalizeBase(String b) {
        return (b == null || b.isBlank()) ? null : (b.endsWith("/") ? b : b + "/");
    }

    /** {@link #liveBase(HttpServletRequest, String)} with this servlet's configured base. */
    String liveBase(HttpServletRequest req) {
        return liveBase(req, configuredBase);
    }

    /**
     * The base URL clients reach this server on, ending with '/'. It is the
     * configured public base when one was given (the CLI's {@code -base}, for
     * a reverse proxy that does not forward the original host); otherwise it
     * is derived from the request's own scheme, host and port, so a client on
     * another machine gets links it can follow - a fixed
     * {@code http://localhost:<port>/} used to send every remote client's next,
     * up, linkset and storage-description links to its own loopback. Behind a
     * proxy that sends {@code Forwarded} / {@code X-Forwarded-*},
     * {@link #honourForwardedHeaders} makes the request report the public
     * origin here.
     *
     * @param configured a base ending with '/', or null to derive one from the request
     */
    static String liveBase(HttpServletRequest req, String configured) {
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

    /** The legacy path alias: {@code HalcyonStorage/x} names {@code x}. Exactly that segment - not any name sharing its prefix. */
    static final String ALIAS = "HalcyonStorage";

    /**
     * Strips the alias segment. A prefix test used to strip it from
     * {@code HalcyonStorageArchive/...} too, making any entry whose name starts
     * with the alias unreachable (BG-47).
     */
    static String stripAlias(String reqPath) {
        if (reqPath.equals(ALIAS)) {
            return "";
        }
        if (reqPath.startsWith(ALIAS + "/")) {
            return reqPath.substring(ALIAS.length() + 1);
        }
        return reqPath;
    }

    private static String resourceUriOf(String reqPath) {
        return reqPath.isEmpty() ? HTTP_ROOT : HTTP_ROOT + "/" + reqPath;
    }

    /**
     * Whether {@code path} is a store this server answers SPARQL for: an HDF5
     * file ({@code .h5} / {@code .hdf5}), a Zarr store in a ZIP archive
     * ({@code .zarr.zip}), or the directory of a Zarr store ({@code *.zarr},
     * holding a {@code zarr.json}).
     */
    private static boolean isStore(Path path) {
        if (path == null || path.getFileName() == null) {
            return false;
        }
        if (Files.isDirectory(path)) {
            return com.ebremer.beakgraph.core.BeakGraphFiles.isZarrStoreDirectory(path);
        }
        String name = path.getFileName().toString();
        return Files.isRegularFile(path) && (com.ebremer.beakgraph.core.BeakGraphFiles.isBeakGraphFileName(name)
                || com.ebremer.beakgraph.core.BeakGraphFiles.isZarrArchiveName(name));
    }

    /**
     * The keys of a Zarr store that are served: a node's metadata document,
     * and a chunk of an array. Nothing else that may lie in the directory.
     */
    private static final java.util.regex.Pattern ZARR_KEY =
            java.util.regex.Pattern.compile("(?:[^/]+/)*zarr\\.json|(?:[^/]+/)+c(?:/\\d+)+");

    /**
     * The file of a key of a Zarr store, when {@code reqPath} names one:
     * {@code <store>.zarr/<key>}, in a store this storage lists. The store is
     * ONE resource here and what it holds is not listed, but its keys are
     * served - whole or by range - so that it can be read where it lies, as
     * an .h5 file is by its ranges. Null for anything else: the request is
     * then answered as any other.
     */
    private Path zarrKey(String reqPath) {
        if (storageRoot == null) {
            return null;
        }
        String[] segments = reqPath.split("/");
        for (int i = 0; i < segments.length - 1; i++) {
            if (!com.ebremer.beakgraph.core.BeakGraphFiles.isZarrStoreName(segments[i])) {
                continue;
            }
            String storePath = String.join("/", java.util.Arrays.copyOfRange(segments, 0, i + 1));
            String key = String.join("/", java.util.Arrays.copyOfRange(segments, i + 1, segments.length));
            if (!ZARR_KEY.matcher(key).matches()) {
                return null;
            }
            Path store = resolveWithin(storageRoot, storePath);
            // A store the listing has: not one under a hidden directory, or shadowed by a fixed route.
            if (store == null || !isStore(store) || lookup(resourceUriOf(storePath)) == null) {
                return null;
            }
            Path file = resolveWithin(storageRoot, reqPath);
            return (file != null && Files.isRegularFile(file)) ? file : null;
        }
        return null;
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
    /**
     * The RFC 9264 linkset of a resource: its metadata links as a standalone
     * resource (LWS Metadata), with the validators every GET/HEAD response
     * carries. Read-only deployment: {@code Allow} names exactly the methods
     * this linkset supports - no PATCH.
     */
    private void serveLinkset(HttpServletRequest req, HttpServletResponse resp, String base, String resourceURI) throws IOException {
        Resource r = lookup(resourceURI);
        if (r == null) {
            sendProblem(req, resp, 404, "No resource has this linkset");
            return;
        }
        boolean isContainer = r.hasProperty(RDF.type, LWS_CONTAINER);
        String anchor = toLiveUri(resourceURI, base);
        String up = getParentURI(resourceURI);
        // anchor/up are advertised on the LIVE base, never the canonical one.
        byte[] body = linksetJson(anchor, isContainer ? LWS.Container.getURI() : LWS.DataResource.getURI(),
                up == null ? null : toLiveUri(up, base), getLinksetURI(anchor), storageUri(base))
                .getBytes(StandardCharsets.UTF_8);
        resp.setHeader("Allow", ALLOW_READ);
        addStorageLink(resp, base);
        String modified = isContainer ? modifiedOf(r) : liveItem(r).updated();
        if (answerPreconditions(req, resp, contentEtag(LINKSET_JSON, body), lastModifiedOf(modified))) {
            return;
        }
        writeBody(req, resp, LINKSET_JSON, body);
    }

    /**
     * Build an RFC 9264 linkset+json document holding a resource's links - its
     * type, parent ({@code up}, omitted for the root and the storage), own
     * linkset and storage - and nothing that is not a link: format, size and
     * modification time travel in the container listing and the HTTP headers.
     * Every value is emitted through jakarta.json, so a quote/backslash/newline
     * in a URI is escaped rather than breaking (or injecting into) the JSON.
     */
    static String linksetJson(String anchor, String typeHref, String up, String linkset, String storage) {
        JsonObjectBuilder link = Json.createObjectBuilder()
            .add("anchor", anchor)
            .add("type", hrefArray(typeHref));
        if (up != null) {
            link.add("up", hrefArray(up));
        }
        link.add("linkset", Json.createArrayBuilder()
                .add(Json.createObjectBuilder().add("href", linkset).add("type", LINKSET_JSON)));
        link.add(REL_STORAGE, hrefArray(storage));
        JsonObject root = Json.createObjectBuilder()
            .add("linkset", Json.createArrayBuilder().add(link))
            .build();
        return writeJson(root);
    }

    /** The storage URI on a base: where the storage description is served. */
    static String storageUri(String base) {
        return base + STORAGE_PATH;
    }

    /**
     * The storage description (LWS Discovery): a W3C Controlled Identifier
     * document whose {@code @context} array starts with the CID and LWS
     * contexts, whose id is the storage URI - per CID also this document's own
     * URL - and whose services are the mandatory StorageRoot (the root
     * container) and the SPARQL endpoint.
     */
    static String descriptionJson(String base) {
        JsonObject doc = Json.createObjectBuilder()
            .add("@context", Json.createArrayBuilder().add(CID_CONTEXT).add(LWS_CONTEXT))
            .add("id", storageUri(base))
            .add("type", "Storage")
            .add("service", Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                    .add("type", "StorageRoot")
                    .add("serviceEndpoint", base))
                .add(Json.createObjectBuilder()
                    .add("type", SPARQL_SERVICE_TYPE)
                    // /rdf is the SPARQL protocol endpoint in both modes; /sparql
                    // is the YASGUI page, which answers 405 to a query POST (BG-37).
                    .add("serviceEndpoint", base + "rdf")))
            .build();
        return writeJson(doc);
    }

    /**
     * Serves the storage URI: the storage description, {@code application/lws+cid}
     * unless the client negotiates JSON-LD or JSON (a client accepting none of
     * them still gets the LWS type). Only a directory is a storage: in
     * single-file mode there is no root container for the StorageRoot service
     * to name, so there is no storage description either.
     */
    private void serveStorageDescription(HttpServletRequest req, HttpServletResponse resp, String base) throws IOException {
        if (lookup(HTTP_ROOT) == null) {
            sendProblem(req, resp, 404, "No storage is served here");
            return;
        }
        String negotiated = negotiate(req.getHeader("Accept"), DESCRIPTION_TYPES);
        String contentType = negotiated == null ? LWS_CID : negotiated;
        byte[] body = descriptionJson(base).getBytes(StandardCharsets.UTF_8);
        addStorageLink(resp, base);
        resp.addHeader("Link", "<" + getLinksetURI(storageUri(base)) + ">; rel=\"linkset\"; type=\"" + LINKSET_JSON + "\"");
        resp.addHeader("Link", "<" + LWS.Storage.getURI() + ">; rel=\"type\"");
        resp.setHeader("Vary", "Accept");
        if (answerPreconditions(req, resp, contentEtag(contentType, body), initTime)) {
            return;
        }
        writeBody(req, resp, contentType, body);
    }

    /** The linkset of the storage description itself: it is not in the metadata model. */
    private void serveStorageLinkset(HttpServletRequest req, HttpServletResponse resp, String base) throws IOException {
        if (lookup(HTTP_ROOT) == null) {
            sendProblem(req, resp, 404, "No storage is served here");
            return;
        }
        String storage = storageUri(base);
        byte[] body = linksetJson(storage, LWS.Storage.getURI(), null, getLinksetURI(storage), storage)
                .getBytes(StandardCharsets.UTF_8);
        resp.setHeader("Allow", ALLOW_READ);
        addStorageLink(resp, base);
        if (answerPreconditions(req, resp, contentEtag(LINKSET_JSON, body), initTime)) {
            return;
        }
        writeBody(req, resp, LINKSET_JSON, body);
    }

    /**
     * Writes a body as exactly {@code contentType} - no charset parameter, so a
     * requested {@code application/lws+json} is echoed verbatim (JSON is UTF-8
     * by definition) - and skips it for HEAD, which gets the same headers.
     */
    private static void writeBody(HttpServletRequest req, HttpServletResponse resp, String contentType, byte[] body) throws IOException {
        resp.setContentType(contentType);
        resp.setContentLength(body.length);
        if (!"HEAD".equals(req.getMethod())) {
            resp.getOutputStream().write(body);
        }
    }

    /**
     * Answers an error the servlet raises with RFC 9457 problem details, which
     * LWS asks for on 400 ("Servers SHOULD use the standard format defined in
     * RFC 9457"); every error here uses the same shape. The detail never
     * carries a server path.
     */
    static void sendProblem(HttpServletRequest req, HttpServletResponse resp, int status, String detail) throws IOException {
        JsonObjectBuilder problem = Json.createObjectBuilder()
            .add("type", "about:blank")
            .add("title", HttpStatus.getMessage(status))
            .add("status", status);
        if (detail != null) {
            problem.add("detail", detail);
        }
        resp.setStatus(status);
        writeBody(req, resp, "application/problem+json", writeJson(problem.build()).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A strong entity tag over a representation's exact bytes and media type:
     * equal bytes served under two content types are two representations,
     * which RFC 9110 gives distinct strong validators.
     */
    static String contentEtag(String contentType, byte[] body) {
        MessageDigest md = sha256();
        md.update(contentType.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8));
        md.update((byte) 0);
        md.update(body);
        return quotedHex(md.digest());
    }

    /**
     * A container listing's validator specialised to one representation: the
     * listing ETag is computed once per snapshot, and HTML, Turtle and each
     * JSON media type of it are distinct representations of the same URI.
     */
    static String representationEtag(String listingEtag, String contentType) {
        MessageDigest md = sha256();
        md.update(listingEtag.getBytes(StandardCharsets.UTF_8));
        md.update((byte) 0);
        md.update(contentType.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8));
        return quotedHex(md.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e); // SHA-256 is mandatory in every JRE
        }
    }

    private static String quotedHex(byte[] digest) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < 16; i++) {
            sb.append(String.format(Locale.ROOT, "%02x", digest[i]));
        }
        return sb.append('"').toString();
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
     * linksets whenever the server ran on a non-default port. The canonical model
     * holds RAW file names, so the remainder is percent-encoded here - the single
     * encoding point for item ids, Link targets and RDF subjects (a raw space in a
     * JSON-LD id, a Link {@code <...>} or a Turtle IRI is invalid; BG-40).
     */
    static String toLiveUri(String canonicalUri, String base) {
        String rest = canonicalUri.substring(HTTP_ROOT.length());
        while (rest.startsWith("/")) {
            rest = rest.substring(1);
        }
        return base + encodeHref(rest);
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
     * Whether the Accept header admits an HTML representation.
     * RFC 9110: {@code *}{@code /*} and {@code text/*} match every/any text
     * representation - curl's default {@code Accept: *}{@code /*} must get a
     * page, not a 406.
     */
    static boolean acceptsHtmlRepresentation(String accept) {
        return negotiate(accept, List.of("text/html")) != null;
    }

    /** The container representations, in server preference order (ties in q are broken this way). */
    static final List<String> CONTAINER_TYPES = List.of("text/html", LWS_JSON, "application/ld+json", "application/json", "text/turtle");
    /** The storage description's media types, {@code application/lws+cid} first (the spec's MUST). */
    static final List<String> DESCRIPTION_TYPES = List.of(LWS_CID, "application/ld+json", "application/json");

    /**
     * Whether the negotiated container media type is to be served in the LWS
     * profile: it is one of {@link #LWS_PROFILE_TYPES} and the request asked
     * for it with that profile.
     */
    static boolean inLwsProfile(String chosen, String accept) {
        return chosen != null && LWS_PROFILE_TYPES.contains(chosen) && requestsLwsProfile(accept, chosen);
    }

    /**
     * The Content-Type a negotiated container representation is served as.
     * LWS requires a response in the LWS profile to carry the same
     * {@code profile} parameter the request named, so a profiled choice gets
     * it back; every other choice is served as negotiated.
     */
    static String containerContentType(String chosen, String accept) {
        return inLwsProfile(chosen, accept)
                ? chosen + "; profile=\"" + LWS_CONTEXT + "\""
                : chosen;
    }

    /**
     * Whether an Accept header asks for {@code mediaType} with the LWS
     * profile: a media range of that type, not excluded by {@code q=0}, whose
     * {@code profile} parameter (a space-separated list of URIs, quoted or not)
     * names {@link #LWS_CONTEXT}.
     */
    static boolean requestsLwsProfile(String accept, String mediaType) {
        if (accept == null) {
            return false;
        }
        for (String range : accept.split(",")) {
            String[] parts = range.trim().split(";");
            if (!parts[0].trim().equalsIgnoreCase(mediaType)) {
                continue;
            }
            boolean profiled = false;
            boolean excluded = false;
            for (int i = 1; i < parts.length; i++) {
                String param = parts[i].trim();
                int eq = param.indexOf('=');
                if (eq < 0) {
                    continue;
                }
                String name = param.substring(0, eq).trim();
                String value = param.substring(eq + 1).trim();
                if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }
                if (name.equalsIgnoreCase("profile")) {
                    profiled = Arrays.asList(value.trim().split("\\s+")).contains(LWS_CONTEXT);
                } else if (name.equalsIgnoreCase("q")) {
                    try {
                        excluded = Double.parseDouble(value) <= 0.0;
                    } catch (NumberFormatException e) {
                        excluded = true;
                    }
                }
            }
            if (profiled && !excluded) {
                return true;
            }
        }
        return false;
    }

    /**
     * RFC 9110 proactive negotiation: the offered type with the highest
     * quality among the Accept header's media ranges (a range's parameters
     * other than {@code q} are ignored; {@code *}{@code /*} and {@code type/*}
     * match with lower precedence than an exact type; {@code q=0} excludes),
     * ties broken by the offered order. An absent or blank header accepts the
     * first offered type; no acceptable type returns null (406). Substring
     * matching used to ignore q-values entirely and labelled a plain
     * {@code application/json} request as {@code application/ld+json} whenever
     * both appeared (BG-46).
     */
    static String negotiate(String accept, List<String> offered) {
        if (accept == null || accept.isBlank()) {
            return offered.get(0);
        }
        List<Object[]> ranges = new ArrayList<>(); // {type, q, specificity}
        for (String range : accept.split(",")) {
            String[] parts = range.trim().split(";");
            String type = parts[0].trim().toLowerCase(Locale.ROOT);
            if (type.isEmpty()) {
                continue;
            }
            double q = 1.0;
            for (int i = 1; i < parts.length; i++) {
                String param = parts[i].trim();
                if (param.length() > 2 && (param.charAt(0) == 'q' || param.charAt(0) == 'Q') && param.charAt(1) == '=') {
                    try {
                        q = Double.parseDouble(param.substring(2).trim());
                    } catch (NumberFormatException e) {
                        q = 0.0;
                    }
                }
            }
            int specificity = type.equals("*/*") ? 0 : type.endsWith("/*") ? 1 : 2;
            ranges.add(new Object[]{type, q, specificity});
        }
        String best = null;
        double bestQ = 0.0;
        for (String candidate : offered) {
            String lower = candidate.toLowerCase(Locale.ROOT);
            double q = 0.0;
            int matched = -1;
            for (Object[] r : ranges) {
                String type = (String) r[0];
                int specificity = (Integer) r[2];
                boolean matches = specificity == 0
                        || (specificity == 1 && lower.startsWith(type.substring(0, type.length() - 1)))
                        || type.equals(lower);
                if (matches && specificity > matched) {
                    matched = specificity;
                    q = (Double) r[1];
                }
            }
            if (q > bestQ) {
                bestQ = q;
                best = candidate;
            }
        }
        return best;
    }

    /**
     * RFC 6266 / RFC 8187 attachment disposition: an ASCII-only quoted
     * fallback (control and non-ASCII characters replaced, {@code \} and
     * {@code "} escaped) plus the {@code filename*} UTF-8 form, so a name
     * with a quote, a trailing backslash or a non-ASCII letter neither breaks
     * the header nor arrives mangled (BG-48).
     */
    static String contentDisposition(String name) {
        StringBuilder ascii = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c < 0x20 || c > 0x7E) {
                ascii.append('_');
                continue;
            }
            if (c == '\\' || c == '"') {
                ascii.append('\\');
            }
            ascii.append(c);
        }
        String utf8 = URLEncoder.encode(name, StandardCharsets.UTF_8)
                .replace("+", "%20").replace("*", "%2A").replace("~", "%7E");
        return "attachment; filename=\"" + ascii + "\"; filename*=UTF-8''" + utf8;
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

    /**
     * The Link to the storage URI with {@code rel="https://www.w3.org/ns/lws#storage"},
     * which every GET/HEAD response for a storage resource MUST carry (LWS
     * Discovery and Binding).
     */
    private static void addStorageLink(HttpServletResponse resp, String base) {
        resp.addHeader("Link", "<" + storageUri(base) + ">; rel=\"" + REL_STORAGE + "\"");
    }

    /** The Link headers every resource/container response must carry. */
    private void addCommonLinks(HttpServletResponse resp, String base, String resourceURI, boolean isContainer) {
        resp.addHeader("Link", "<" + getLinksetURI(toLiveUri(resourceURI, base)) + ">; rel=\"linkset\"; type=\"" + LINKSET_JSON + "\"");
        addStorageLink(resp, base);
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
     * The memoised listing of a container for the current snapshot: sorted
     * items, listing-version ETag and Last-Modified. Views are cached per
     * snapshot: a refresh publishes a new model, and listings computed from it
     * must not revalidate against the old ETags.
     */
    private ContainerView containerView(Resource r, String resourceURI) {
        Model snapshot = r.getModel();
        if (snapshot != viewSnapshot) {
            synchronized (views) {
                if (snapshot != viewSnapshot) {
                    views.clear();
                    viewSnapshot = snapshot;
                }
            }
        }
        return views.computeIfAbsent(resourceURI, k -> {
            List<Resource> items = Collections.unmodifiableList(sortedItems(r));
            return new ContainerView(items, containerEtag(r, resourceURI, items), containerLastModified(r, items));
        });
    }

    /**
     * Listing-version ETag (changes when the generated membership metadata
     * changes); {@link #representationEtag} specialises it per media type.
     */
    private static String containerEtag(Resource r, String resourceURI, List<Resource> items) {
        MessageDigest md = sha256();
        md.update(resourceURI.getBytes(StandardCharsets.UTF_8));
        // The page size shapes the representation (which items land on
        // which page): without it in the validator, changing PAGE_SIZE
        // between server runs would 304-revalidate clients' stale slicings.
        md.update(Integer.toString(PAGE_SIZE).getBytes(StandardCharsets.UTF_8));
        md.update(Long.toString(items.size()).getBytes(StandardCharsets.UTF_8));
        Statement own = r.getProperty(MODIFIED);
        if (own != null) md.update(own.getString().getBytes(StandardCharsets.UTF_8));
        for (Resource it : items) {
            md.update(it.getURI().getBytes(StandardCharsets.UTF_8));
            Statement mod = it.getProperty(MODIFIED);
            if (mod != null) md.update(mod.getString().getBytes(StandardCharsets.UTF_8));
        }
        return quotedHex(md.digest());
    }

    /**
     * A data resource's size and modification instant as the listing reports
     * them: from the FILE when it exists (the model's literals are a snapshot
     * of the last scan; a file replaced since then showed its old size and
     * time until the next periodic scan, BG-388), else from the model. When
     * the file disagrees with the snapshot the refresher is asked to re-scan
     * (it does so only if the file is newer than its last scan), so the next
     * request's snapshot - and its validators - catch up.
     */
    private record LiveItem(Long size, String updated) {}

    private LiveItem liveItem(Resource it) {
        Long size = null;
        String updated = null;
        Statement sz = it.getProperty(SCHEMA_SIZE);
        if (sz != null) {
            try {
                size = sz.getLong();
            } catch (RuntimeException ignore) {
                // a malformed size literal is dropped, not fatal (size is a SHOULD)
            }
        }
        updated = modifiedOf(it);
        if (storageRoot != null && !it.hasProperty(RDF.type, LWS_CONTAINER) && it.getURI().startsWith(HTTP_ROOT)) {
            String rel = it.getURI().substring(HTTP_ROOT.length());
            if (rel.startsWith("/")) rel = rel.substring(1);
            Path file = resolveWithin(storageRoot, rel);
            if (file != null && Files.isDirectory(file) && isStore(file)) {
                file = file.resolve("zarr.json"); // a Zarr store: the document that stands for it
            }
            if (file != null && Files.isRegularFile(file)) {
                try {
                    BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
                    String liveUpdated = LWSMetadataGenerator.modifiedLiteral(attrs);
                    if ((size == null || size != attrs.size()) || !liveUpdated.equals(updated)) {
                        if (refresher != null) {
                            refresher.refreshOnDemand(file);
                        }
                    }
                    size = attrs.size();
                    updated = liveUpdated;
                } catch (IOException ignore) {
                    // the snapshot's values stand
                }
            }
        }
        return new LiveItem(size, updated);
    }

    /** Container Last-Modified: its own timestamp, else the newest member's, else server start. */
    private long containerLastModified(Resource r, List<Resource> items) {
        long best = parseInstantMillis(modifiedOf(r));
        for (Resource it : items) {
            best = Math.max(best, parseInstantMillis(modifiedOf(it)));
        }
        return best > 0 ? best : initTime;
    }

    /** The resource's {@code dcterms:modified} lexical form in the metadata snapshot, or null. */
    private static String modifiedOf(Resource r) {
        Statement s = r.getProperty(MODIFIED);
        return s == null ? null : s.getString();
    }

    /** A Last-Modified from a {@code dcterms:modified} lexical form, server start when there is none. */
    private long lastModifiedOf(String modified) {
        long millis = parseInstantMillis(modified);
        return millis > 0 ? millis : initTime;
    }

    private static long parseInstantMillis(String instant) {
        if (instant == null) return -1;
        try {
            return Instant.parse(instant).toEpochMilli();
        } catch (RuntimeException e) {
            return -1;
        }
    }

    /**
     * Sets a GET/HEAD response's validators - ETag, Last-Modified and
     * {@code Cache-Control: no-cache} - and evaluates the request's
     * preconditions in RFC 9110 s13.2.2 order: If-Match, else
     * If-Unmodified-Since (412 when it fails); then If-None-Match, else
     * If-Modified-Since (304 when the client's copy is current). Returns true
     * when that answered the request.
     * <p>
     * {@code no-cache} is "revalidate before reuse", not "don't cache":
     * without it, heuristic freshness (from Last-Modified) lets browsers
     * replay stale listings for days, and an intermediary could replay a
     * stale 206 after a file was replaced; the validators make the
     * revalidation a cheap 304 (BG-389).
     */
    private static boolean answerPreconditions(HttpServletRequest req, HttpServletResponse resp, String etag, long lastModified) throws IOException {
        resp.setHeader("ETag", etag);
        resp.setDateHeader("Last-Modified", lastModified);
        resp.setHeader("Cache-Control", "no-cache");
        List<String> ifMatch = Collections.list(req.getHeaders("If-Match"));
        if (!ifMatch.isEmpty()) {
            if (!ifMatchMatches(ifMatch, etag)) {
                sendProblem(req, resp, HttpServletResponse.SC_PRECONDITION_FAILED, "If-Match names no current representation");
                return true;
            }
        } else {
            long ifUnmodifiedSince = dateHeader(req, "If-Unmodified-Since");
            if (ifUnmodifiedSince >= 0 && lastModified / 1000 > ifUnmodifiedSince / 1000) {
                sendProblem(req, resp, HttpServletResponse.SC_PRECONDITION_FAILED, "Modified since If-Unmodified-Since");
                return true;
            }
        }
        List<String> ifNoneMatch = Collections.list(req.getHeaders("If-None-Match"));
        long ifModifiedSince = dateHeader(req, "If-Modified-Since");
        boolean current = !ifNoneMatch.isEmpty()
                ? ifNoneMatchMatches(ifNoneMatch, etag)
                : (ifModifiedSince >= 0 && lastModified / 1000 <= ifModifiedSince / 1000);
        if (current) {
            resp.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return true;
        }
        return false;
    }

    /** A date header's value, -1 when absent or not a valid HTTP date (RFC 9110: an invalid date is ignored). */
    private static long dateHeader(HttpServletRequest req, String name) {
        try {
            return req.getDateHeader(name);
        } catch (IllegalArgumentException e) {
            return -1;
        }
    }

    /**
     * RFC 9110 s13.1.1: {@code If-Match} is {@code *} or a list of entity tags
     * compared strongly - a weak tag never matches.
     */
    static boolean ifMatchMatches(List<String> headerValues, String etag) {
        for (String value : headerValues) {
            for (String token : value.split(",")) {
                String t = token.trim();
                if (t.equals("*") || (!t.startsWith("W/") && t.equals(etag))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * RFC 9110 s13.1.2: {@code If-None-Match} is a list of entity tags (the
     * field may repeat) or {@code *}, compared weakly - a {@code W/} prefix on
     * either side is ignored. Exact string equality used to make a list, a
     * weak validator or {@code *} never produce a 304 (BG-49).
     */
    static boolean ifNoneMatchMatches(List<String> headerValues, String etag) {
        String strong = etag.startsWith("W/") ? etag.substring(2) : etag;
        for (String value : headerValues) {
            for (String token : value.split(",")) {
                String t = token.trim();
                if (t.isEmpty()) {
                    continue;
                }
                if (t.equals("*")) {
                    return true;
                }
                if (t.startsWith("W/")) {
                    t = t.substring(2);
                }
                if (t.equals(strong)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * One entry of a container representation's {@code items}: the live id,
     * whether it is a container, its format (DataResources only, where it is
     * required), size and modification time. The JSON body and the LWS-profile
     * RDF are both written from these, so they cannot disagree.
     */
    private record ListedItem(String id, boolean container, String format, Long size, String modified) {}

    private List<ListedItem> listedItems(String base, List<Resource> pageItems) {
        List<ListedItem> listed = new ArrayList<>(pageItems.size());
        for (Resource it : pageItems) {
            boolean isC = it.hasProperty(RDF.type, LWS_CONTAINER);
            String format = null;
            if (!isC) {
                Statement m = it.getProperty(FORMAT);
                format = m != null ? m.getString() : "application/octet-stream";
            }
            LiveItem live = liveItem(it);
            listed.add(new ListedItem(toLiveUri(it.getURI(), base), isC, format, live.size(), live.updated()));
        }
        return listed;
    }

    /**
     * The LWS container representation (JSON-LD, {@code https://www.w3.org/ns/lws/v1}
     * context): id / type / totalItems reflect the full membership; {@code items}
     * holds the (possibly paginated) current page, each entry with id, type,
     * format, size and modified. Pagination travels in the Link headers alone:
     * the LWS context defines no term for navigation, so the body carries none.
     */
    private static String containerJson(String liveId, long totalItems, List<ListedItem> pageItems) {
        JsonObjectBuilder root = Json.createObjectBuilder()
            .add("@context", LWS_CONTEXT)
            .add("id", liveId)
            .add("type", "Container")
            .add("totalItems", totalItems);
        JsonArrayBuilder arr = Json.createArrayBuilder();
        for (ListedItem it : pageItems) {
            JsonObjectBuilder o = Json.createObjectBuilder();
            o.add("id", it.id());
            o.add("type", it.container() ? "Container" : "DataResource");
            if (it.format() != null) o.add("format", it.format());
            if (it.size() != null) o.add("size", it.size());
            if (it.modified() != null) o.add("modified", it.modified());
            arr.add(o);
        }
        root.add("items", arr);
        return writeJson(root.build());
    }

    /**
     * The RDF that {@link #containerJson} encodes under the LWS context - the
     * types, {@code lws:totalItems} (a JSON integer: {@code xsd:integer}),
     * {@code lws:items}, {@code dcterms:format}, {@code schema:size}
     * ({@code xsd:long}) and {@code dcterms:modified} ({@code xsd:dateTime}) -
     * and nothing else, so any serialization of it is in the LWS profile. The
     * model's own triples (owl:sameAs, a member container's own items) and the
     * Turtle listing's navigation are not part of that RDF.
     */
    private static Model lwsProfileModel(String liveId, long totalItems, List<ListedItem> pageItems) {
        Model out = ModelFactory.createDefaultModel();
        out.setNsPrefix("lws", LWS.NS);
        out.setNsPrefix("dcterms", org.apache.jena.vocabulary.DCTerms.NS);
        out.setNsPrefix("schema", "https://schema.org/");
        out.setNsPrefix("xsd", org.apache.jena.vocabulary.XSD.NS);
        Resource self = out.createResource(liveId)
            .addProperty(RDF.type, LWS.Container)
            .addProperty(LWS.totalItems, out.createTypedLiteral(Long.toString(totalItems), XSDDatatype.XSDinteger));
        for (ListedItem it : pageItems) {
            Resource item = out.createResource(it.id())
                .addProperty(RDF.type, it.container() ? LWS.Container : LWS.DataResource);
            self.addProperty(LWS_ITEMS, item);
            if (it.format() != null) item.addProperty(FORMAT, it.format());
            if (it.size() != null) item.addProperty(SCHEMA_SIZE, out.createTypedLiteral(Long.toString(it.size()), XSDDatatype.XSDlong));
            if (it.modified() != null) item.addProperty(MODIFIED, out.createTypedLiteral(it.modified(), XSDDatatype.XSDdateTime));
        }
        return out;
    }

    /**
     * The request's path below the base, decoded, without leading or trailing
     * '/' - or null when the URI is malformed.
     */
    static String requestPath(HttpServletRequest req) {
        String reqPath = decodePath(req.getRequestURI());
        if (reqPath == null) return null;
        if (reqPath.startsWith("/")) reqPath = reqPath.substring(1);
        if (reqPath.endsWith("/")) reqPath = reqPath.substring(0, reqPath.length() - 1);
        return reqPath;
    }

    /**
     * Write methods answer 405 with the {@code Allow} header RFC 9110 requires,
     * whatever the target: LWS clients are told to expect 405 for an operation
     * a server does not support, and this storage is read-only by design.
     * PATCH would otherwise reach HttpServlet's bare 405, and TRACE its echo of
     * the request.
     */
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        switch (req.getMethod()) {
            case "PUT", "DELETE", "PATCH", "TRACE" -> methodNotAllowed(req, resp);
            default -> super.service(req, resp);
        }
    }

    private void methodNotAllowed(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setHeader("X-Content-Type-Options", "nosniff");
        resp.setHeader("Allow", allowFor(req));
        sendProblem(req, resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                "This storage is read-only; " + req.getMethod() + " is not supported");
    }

    /** OPTIONS names what the target supports: the read operations, and POST on a store. */
    @Override
    protected void doOptions(HttpServletRequest req, HttpServletResponse resp) {
        resp.setHeader("Allow", allowFor(req));
        resp.setContentLength(0);
    }

    /**
     * The methods the target resource supports: every resource here is
     * readable, and a served store also takes a SPARQL query by POST.
     */
    private String allowFor(HttpServletRequest req) {
        String reqPath = requestPath(req);
        if (reqPath != null && storageRoot != null) {
            String rel = stripAlias(reqPath);
            Path file = resolveWithin(storageRoot, rel);
            if (isStore(file) && lookup(resourceUriOf(rel)) != null) {
                return ALLOW_QUERYABLE;
            }
        }
        return ALLOW_READ;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        boolean isHead = "HEAD".equals(req.getMethod());
        // Every absolute URI this response advertises is minted on the base the
        // client reached us on (or the configured -base).
        final String base = liveBase(req);
        // Stop browsers from MIME-sniffing served content into something executable.
        resp.setHeader("X-Content-Type-Options", "nosniff");
        String reqPath = requestPath(req);
        if (reqPath == null) { sendProblem(req, resp, 400, "Malformed request URI"); return; }
        // A stored entry literally named "<x>.meta" is that entry, not the
        // linkset of <x>: the literal path is looked up first (BG-47).
        if (reqPath.endsWith(".meta") && lookup(resourceUriOf(stripAlias(reqPath))) == null) {
            String basePath = stripAlias(reqPath.substring(0, reqPath.length() - 5));
            if (basePath.equals(STORAGE_PATH)) {
                serveStorageLinkset(req, resp, base);
                return;
            }
            serveLinkset(req, resp, base, resourceUriOf(basePath));
            return;
        }
        if (reqPath.equals(STORAGE_PATH)) {
            serveStorageDescription(req, resp, base);
            return;
        }
        reqPath = stripAlias(reqPath);
        // A key of a Zarr store: its bytes, for a reader of the store. Not a resource of the listing.
        Path zarrKey = zarrKey(reqPath);
        if (zarrKey != null) {
            serveBytes(req, resp, zarrKey, zarrKey.getFileName().toString().equals("zarr.json")
                    ? LWSMetadataGenerator.ZARR_STORE_MEDIA_TYPE : "application/octet-stream", isHead);
            return;
        }
        String resourceURI = resourceUriOf(reqPath);
        Resource r = lookup(resourceURI);
        if (r == null) {
            sendProblem(req, resp, 404, "No resource at this URI");
            return;
        }
        // SPARQL on a store checked FIRST (fixes Jena Accept header triggering metadata instead of query)
        Path h5Candidate = resolveWithin(storageRoot, reqPath);
        if (isStore(h5Candidate) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, reqPath, h5Candidate);
            return;
        }
        boolean isContainer = r.hasProperty(RDF.type, LWS_CONTAINER);
        addCommonLinks(resp, base, resourceURI, isContainer);
        resp.setHeader("Vary", "Accept");
        String formatParam = req.getParameter("format");
        boolean forceTurtle = "turtle".equalsIgnoreCase(formatParam);
        boolean forceJsonLd = "jsonld".equalsIgnoreCase(formatParam);
        // ?format= overrides negotiation; otherwise the Accept header's ranges
        // and q-values pick one of the container representations.
        String chosen = forceTurtle ? "text/turtle"
                : forceJsonLd ? "application/ld+json"
                : negotiate(req.getHeader("Accept"), CONTAINER_TYPES);
        boolean wantHtml = "text/html".equals(chosen);
        boolean wantTurtle = "text/turtle".equals(chosen);
        boolean wantJson = chosen != null && chosen.endsWith("json");

        if (isContainer) {
            if (chosen == null) {
                sendProblem(req, resp, 406, "A container is available as application/lws+json, application/ld+json, "
                        + "application/json, text/turtle or text/html");
                return;
            }
            // The media type the representation is served as: a request for an
            // RDF type in the LWS profile gets the profile back (LWS Content
            // Negotiation), and the representation is then in that profile.
            boolean lwsProfile = !forceTurtle && !forceJsonLd && inLwsProfile(chosen, req.getHeader("Accept"));
            String contentType = lwsProfile ? containerContentType(chosen, req.getHeader("Accept")) : chosen;
            ContainerView view = containerView(r, resourceURI);
            List<Resource> items = view.items();
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
                    sendProblem(req, resp, 400, "Invalid 'page' parameter: " + pageStr);
                    return;
                }
                if (page < 1) { sendProblem(req, resp, 400, "'page' must be >= 1"); return; }
                paginated = true;
                long start = (long)(page-1) * PAGE_SIZE;   // long: a huge page must not overflow to a negative index
                if ((start >= total && total > 0) || (total == 0 && page > 1)) {
                    sendProblem(req, resp, 404, "The container has no page " + page);
                    return;
                }
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
                // Normative navigation (LWS): Link headers. The plain Turtle
                // body mirrors them; the JSON body and the LWS profile cannot
                // (see containerJson).
                resp.addHeader("Link", "<" + firstUri + ">; rel=\"first\"");
                resp.addHeader("Link", "<" + lastUri + ">; rel=\"last\"");
                if (nextUri != null) resp.addHeader("Link", "<" + nextUri + ">; rel=\"next\"");
                if (prevUri != null) resp.addHeader("Link", "<" + prevUri + ">; rel=\"prev\"");
            }

            // ---- Validators + conditional GET. The listing version is
            // membership-scoped (not per page: each page is its own URI), and
            // specialised per media type - HTML, Turtle and each JSON flavour
            // are distinct representations of the same URI.
            if (answerPreconditions(req, resp, representationEtag(view.etag(), contentType), view.lastModified())) {
                return;
            }

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
                        // Same (percent-encoded) target as the rel="up" Link header.
                        out.println("<a href=\"" + escapeHtml(toLiveUri(up, base)) + "\">&#8679; Parent</a> | ");
                    }
                    out.println("<a href=\"" + escapeHtml(storageUri(base)) + "\">Storage Description</a> | ");
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
            if (wantJson) {
                // LWS container representation: one body for every JSON flavour
                // (so the profiled JSON-LD is the lws+json document itself), only
                // the Content-Type varies.
                writeBody(req, resp, contentType,
                        containerJson(liveSelf, total, listedItems(base, pageItems)).getBytes(StandardCharsets.UTF_8));
                return;
            }
            if (wantTurtle && lwsProfile) {
                // Turtle in the LWS profile: exactly the RDF the lws+json body
                // encodes, so the two are isomorphic as the profile requires.
                ByteArrayOutputStream serialized = new ByteArrayOutputStream();
                RDFDataMgr.write(serialized, lwsProfileModel(liveSelf, total, listedItems(base, pageItems)), RDFFormat.TURTLE);
                writeBody(req, resp, contentType, serialized.toByteArray());
                return;
            }
            if (wantTurtle) {
                // Additional RDF representation, not in the LWS profile: the
                // model's triples (page-scoped items) plus navigation.
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
                    LiveItem live = liveItem(it);
                    it.listProperties().forEachRemaining(st -> {
                        RDFNode obj = st.getObject();
                        if (!exposableToClient(obj)) return; // never leak file:/// server paths
                        if (st.getPredicate().equals(SCHEMA_SIZE) || st.getPredicate().equals(MODIFIED)) return; // live values below
                        if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                            out.getResource(itHttp).addProperty(st.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI(), base)));
                        } else {
                            out.getResource(itHttp).addProperty(st.getPredicate(), obj);
                        }
                    });
                    if (live.size() != null) {
                        out.getResource(itHttp).addProperty(SCHEMA_SIZE, out.createTypedLiteral(live.size(), org.apache.jena.vocabulary.XSD.integer.getURI()));
                    }
                    if (live.updated() != null) {
                        out.getResource(itHttp).addProperty(MODIFIED, out.createTypedLiteral(live.updated(), org.apache.jena.vocabulary.XSD.dateTime.getURI()));
                    }
                }
                resp.setContentType("text/turtle");
                if (!isHead) RDFDataMgr.write(resp.getOutputStream(), out, RDFFormat.TURTLE);
                return;
            }
            sendProblem(req, resp, 406, null);
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
            // The live, percent-encoded name (reqPath is the DECODED path).
            Resource httpR = out.createResource(toLiveUri(resourceURI, base));
            r.listProperties().forEachRemaining(s -> {
                RDFNode obj = s.getObject();
                if (!exposableToClient(obj)) return; // never leak file:/// server paths
                if (obj.isResource() && obj.asResource().getURI() != null && obj.asResource().getURI().startsWith(HTTP_ROOT)) {
                    httpR.addProperty(s.getPredicate(), out.createResource(toLiveUri(obj.asResource().getURI(), base)));
                } else {
                    httpR.addProperty(s.getPredicate(), obj);
                }
            });
            String contentType = forceTurtle ? "text/turtle" : "application/ld+json";
            ByteArrayOutputStream serialized = new ByteArrayOutputStream();
            RDFDataMgr.write(serialized, out, forceTurtle ? RDFFormat.TURTLE : RDFFormat.JSONLD);
            byte[] body = serialized.toByteArray();
            if (answerPreconditions(req, resp, contentEtag(contentType, body), lastModifiedOf(liveItem(r).updated()))) {
                return;
            }
            writeBody(req, resp, contentType, body);
            return;
        }
        if (storageRoot == null) { sendProblem(req, resp, 500, "No storage root is configured"); return; }
        Path localFile = resolveWithin(storageRoot, reqPath);
        if (localFile == null) { sendProblem(req, resp, 403, null); return; }
        if (isStore(localFile) && isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, reqPath, localFile);
            return;
        }
        if (Files.isDirectory(localFile) && isStore(localFile)) {
            // A Zarr store is a directory: what a GET of it returns is the document
            // it is opened from, which is also what the listing describes it by.
            localFile = localFile.resolve("zarr.json");
        }
        if (!Files.exists(localFile) || Files.isDirectory(localFile)) {
            sendProblem(req, resp, 404, "No resource at this URI");
            return;
        }
        Statement mediaStmt = r.getProperty(FORMAT);
        String media = (mediaStmt != null) ? mediaStmt.getString() : null;
        if (media == null || media.isBlank()) {
            // Resource metadata didn't declare a media type; probe the file, else fall back.
            media = Files.probeContentType(localFile);
            if (media == null) media = "application/octet-stream";
        }
        serveBytes(req, resp, localFile, media, isHead);
    }

    /**
     * Serves a file's bytes as {@code media}: whole, or the one range asked
     * for, with the validators a reader by ranges pins.
     */
    private static void serveBytes(HttpServletRequest req, HttpServletResponse resp, Path localFile, String media,
                                   boolean isHead) throws IOException {
        // The file is OPENED FIRST and the validator derived from that handle:
        // size from the channel, mtime read right after the open. Stat-then-open
        // let an atomic replace in between send the new file's bytes under the
        // old file's ETag / Content-Range (BG-385); the open handle stays on
        // the bytes the validator describes (POSIX keeps the unlinked inode,
        // Java opens with FILE_SHARE_DELETE on Windows).
        try (FileChannel channel = FileChannel.open(localFile, StandardOpenOption.READ)) {
            long size = channel.size();
            BasicFileAttributes attrs = Files.readAttributes(localFile, BasicFileAttributes.class);
            long lastModified = attrs.lastModifiedTime().toMillis();
            if (attrs.size() != size) {
                // Replaced between the open and the stat: the handle is authoritative.
                lastModified = System.currentTimeMillis();
            }
            String etag = "\"" + size + "-" + lastModified + "\"";
            resp.setHeader("Accept-Ranges", "bytes");
            // A remote reader fetches blocks by range: answerPreconditions'
            // no-cache keeps an intermediary from replaying a stale 206 after
            // the file was replaced (BG-389).
            if (answerPreconditions(req, resp, etag, lastModified)) return;
            resp.setContentType(media);
            // Stored bytes are served as a download: with nosniff above, this keeps an
            // HTML/SVG file someone placed under the root from rendering in this origin.
            resp.setHeader("Content-Disposition", contentDisposition(localFile.getFileName().toString()));

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
                channel.position(start);
                copyBounded(Channels.newInputStream(channel), resp.getOutputStream(), len);
                return;
            }
            resp.setContentLengthLong(size);
            // HEAD gets the same headers without the body (and without the file copy).
            if (!isHead) {
                copyBounded(Channels.newInputStream(channel), resp.getOutputStream(), size);
            }
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
        String reqPath = requestPath(req);
        if (reqPath == null) { sendProblem(req, resp, 400, "Malformed request URI"); return; }
        reqPath = stripAlias(reqPath);
        if (zarrKey(reqPath) != null) {
            // A key of a Zarr store is read, and nothing else: the query goes to the store.
            methodNotAllowed(req, resp);
            return;
        }
        String resourceURI = resourceUriOf(reqPath);
        Resource r = lookup(resourceURI);
        if (r == null) {
            sendProblem(req, resp, 404, "No resource at this URI");
            return;
        }
        Path localFile = r.hasProperty(RDF.type, LWS_CONTAINER) ? null : resolveWithin(storageRoot, reqPath);
        if (!isStore(localFile)) {
            // Creating resources (POST to a container) and every other write is
            // not supported: this storage is read-only. 405, not 406 - it is the
            // METHOD that is unsupported, not a content-negotiation failure.
            methodNotAllowed(req, resp);
            return;
        }
        if (isSparqlRequest(req)) {
            handleSparqlQuery(req, resp, reqPath, localFile);
            return;
        }
        // A store takes POST - for a SPARQL query, which this request is not.
        resp.setHeader("Allow", ALLOW_QUERYABLE);
        String ct = req.getContentType();
        if (ct != null && ct.toLowerCase(Locale.ROOT).startsWith("application/x-www-form-urlencoded")) {
            sendProblem(req, resp, 400, "No SPARQL query provided");
        } else {
            sendProblem(req, resp, 415, "A store takes a SPARQL query: application/sparql-query, "
                    + "or application/x-www-form-urlencoded with a query parameter");
        }
    }
    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Same routing and headers as GET; each body-writing branch checks the
        // method and skips the body for HEAD.
        doGet(req, resp);
    }
}
