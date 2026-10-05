package com.ebremer.beakgraph.utils;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.JsonLdErrorCode;
import com.apicatalog.jsonld.JsonLdOptions;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;
import com.apicatalog.jsonld.loader.DocumentLoader;
import com.apicatalog.jsonld.loader.DocumentLoaderOptions;
import com.apicatalog.jsonld.loader.HttpLoader;
import com.ebremer.beakgraph.core.lib.RelativeIris;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * How a JSON-LD source's {@code @context} references are loaded while it is
 * converted. Every engine parses JSON-LD through Titanium with the sentinel
 * base ({@link RelativeIris#parseBase}), and Titanium's default loader then
 * did two wrong things (BG-424, BG-426):
 * <ul>
 *   <li>a <b>relative</b> context ({@code "@context": "context.jsonld"} next
 *       to the document - the common publishing layout) resolved to
 *       {@code http://beakgraph.invalid/...} and was fetched over HTTP from a
 *       reserved host, so it could never load;</li>
 *   <li>an <b>absolute</b> {@code http(s)} context was dereferenced with no
 *       timeout and no opt-out, and a {@code file:} context read any local
 *       file - a conversion of a downloaded document became a network and
 *       file-system oracle for the converter host.</li>
 * </ul>
 * The loader installed here maps a sentinel-resolved reference back onto the
 * source tree (the directory the sentinel directory stands for: the
 * document's own for a single source, the merge root for a merge), accepts a
 * {@code file:} reference only inside that tree, and refuses remote contexts
 * unless {@value #REMOTE_PROPERTY} is set ({@code -jsonLdRemote} on the
 * command line) - then fetched with a {@value #TIMEOUT_SECONDS}-second
 * timeout and cached PROCESS-WIDE: a merge of many documents sharing one
 * remote context fetches it once, and the concurrent parses of the ultra
 * and plaid engines coalesce on the first fetch instead of each issuing
 * their own (the per-parse cache Titanium ships fetched once per document,
 * BG-428). Errors name the reference and the way out.
 */
public final class JsonLdContexts {

    /** System property: {@code true} lets {@code @context} references be fetched over http(s). */
    public static final String REMOTE_PROPERTY = "beakgraph.jsonld.remote";
    /** Request timeout for remote contexts, when they are enabled. */
    public static final int TIMEOUT_SECONDS = 30;

    private JsonLdContexts() {}

    public static boolean remoteAllowed() {
        return Boolean.getBoolean(REMOTE_PROPERTY);
    }

    /** Remote contexts, shared by every parse in the process; a loading miss is computed once per URI. */
    private static final Cache<URI, Document> REMOTE_CACHE = Caffeine.newBuilder().maximumSize(256).build();
    private static volatile DocumentLoader remoteFetcher = HttpLoader.defaultInstance();

    /** Test hook: what fetches a remote context on a cache miss. */
    static void setRemoteFetcher(DocumentLoader fetcher) {
        remoteFetcher = fetcher;
    }

    /** Forgets every cached remote context (tests; a long-running process that wants fresh copies). */
    public static void clearRemoteCache() {
        REMOTE_CACHE.invalidateAll();
    }

    private static final class CachedFailure extends RuntimeException {
        CachedFailure(JsonLdError cause) {
            super(cause);
        }
    }

    /** The process-wide caching loader: concurrent first requests for one URI wait for a single fetch. */
    private static final DocumentLoader CACHING_REMOTE = (uri, options) -> {
        try {
            return REMOTE_CACHE.get(uri, u -> {
                try {
                    return remoteFetcher.loadDocument(u, options);
                } catch (JsonLdError e) {
                    throw new CachedFailure(e);
                }
            });
        } catch (CachedFailure e) {
            throw (JsonLdError) e.getCause();
        }
    };

    /**
     * Fresh options for parsing {@code input} against {@code base} - fresh per
     * parse: Jena calls {@code setBase} on the object it is handed, so one
     * instance must never be shared across the concurrent parses of the
     * ultra and plaid engines.
     */
    public static JsonLdOptions options(String base, File input) {
        Path root = RelativeIris.sourceRoot(input, base);
        JsonLdOptions options = new JsonLdOptions(new SourceTreeLoader(root, remoteAllowed()));
        options.setBase(URI.create(base));
        options.setTimeout(Duration.ofSeconds(TIMEOUT_SECONDS));
        return options;
    }

    /** Loads contexts from the source tree; remote ones only when allowed. */
    static final class SourceTreeLoader implements DocumentLoader {
        private final Path root;
        private final DocumentLoader remote; // null when remote contexts are disabled

        SourceTreeLoader(Path root, boolean remoteAllowed) {
            this.root = root.toAbsolutePath().normalize();
            this.remote = remoteAllowed ? CACHING_REMOTE : null;
        }

        @Override
        public Document loadDocument(URI uri, DocumentLoaderOptions options) throws JsonLdError {
            String s = uri.toString();
            if (s.startsWith(RelativeIris.SENTINEL_PREFIX)) {
                return local(uri, sentinelToPath(uri));
            }
            String scheme = uri.getScheme();
            if ("file".equalsIgnoreCase(scheme)) {
                Path p;
                try {
                    p = Path.of(uri);
                } catch (RuntimeException e) {
                    throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Cannot read JSON-LD context " + uri + ": " + e.getMessage(), e);
                }
                return local(uri, p);
            }
            if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
                if (remote == null) {
                    throw new JsonLdError(JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED,
                            "Remote JSON-LD context " + uri + " is not fetched: remote contexts are disabled. Place a copy "
                            + "of the context next to the document and reference it relatively, or allow remote contexts "
                            + "with -jsonLdRemote (-D" + REMOTE_PROPERTY + "=true)");
                }
                try {
                    return remote.loadDocument(uri, options);
                } catch (JsonLdError | RuntimeException e) {
                    // Titanium's own message does not name the reference.
                    throw new JsonLdError(JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED,
                            "Cannot load remote JSON-LD context " + uri + ": " + e.getMessage(), e);
                }
            }
            throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED,
                    "Unsupported JSON-LD context reference " + uri + " (scheme " + scheme + ")");
        }

        /**
         * A reference the parser resolved against the sentinel base, mapped
         * onto the source tree: {@code http://beakgraph.invalid/d/.../d/a/ctx.jsonld}
         * is {@code <root>/a/ctx.jsonld}; fewer {@code d} levels (a {@code ../}
         * that climbed above the root) land outside the tree and are refused.
         */
        private Path sentinelToPath(URI uri) throws JsonLdError {
            if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED,
                        "JSON-LD context reference " + uri + " must name a file in the source tree (no query or fragment)");
            }
            String path = uri.getPath(); // percent-decoded
            if (path == null) {
                throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "JSON-LD context reference " + uri + " has no path");
            }
            String[] segments = path.split("/", -1);
            int i = 1; // segments[0] is the empty root segment
            int levels = 0;
            while (i < segments.length - 1 && levels < RelativeIris.SENTINEL_DEPTH && "d".equals(segments[i])) {
                i++;
                levels++;
            }
            Path target = root;
            for (int up = levels; up < RelativeIris.SENTINEL_DEPTH; up++) {
                target = target.resolve("..");
            }
            for (; i < segments.length; i++) {
                if (!segments[i].isEmpty()) {
                    target = target.resolve(segments[i]);
                }
            }
            return target;
        }

        private Document local(URI reference, Path p) throws JsonLdError {
            Path resolved = p.toAbsolutePath().normalize();
            if (!resolved.startsWith(root)) {
                throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED,
                        "JSON-LD context " + reference + " is outside the source tree " + root + " (" + resolved + ")");
            }
            if (!Files.isRegularFile(resolved)) {
                throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED,
                        "JSON-LD context " + reference + " not found in the source tree: " + resolved);
            }
            try (InputStream in = Files.newInputStream(resolved)) {
                return JsonDocument.of(in);
            } catch (IOException e) {
                throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED,
                        "Cannot read JSON-LD context " + reference + " (" + resolved + "): " + e.getMessage(), e);
            }
        }
    }
}
