package com.ebremer.beakgraph.core.lib;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Document-relative IRI handling shared by every writer engine and the
 * query-time resolver.
 * <p>
 * Relative references in a source document are parsed against the sentinel
 * base {@link #SENTINEL_BASE} and stripped back to relative form for storage;
 * at query time they are resolved against the URL the store is served from.
 * The sentinel lives on the reserved {@code .invalid} host, {@link
 * #SENTINEL_DEPTH} directory levels deep, under a leaf no document can name
 * as a sibling ({@code %00}). The depth is what lets parent references
 * survive: RFC 3986 resolution discards {@code ..} segments above the root,
 * so against a one-segment base {@code <../t.png>}, {@code </t.png>} and
 * {@code <t.png>} all collapsed onto the child form {@code t.png} and, served
 * from any URL below the server root, resolved to the wrong resource.
 * <p>
 * {@link #relativize} is a textual relativizer that emits the canonical
 * {@code ../}-prefixed form for any number of levels; Jena's {@code
 * IRIx.relativize} only steps up one level and otherwise emits a
 * path-absolute form, which is wrong against a base of different depth.
 * A resolved IRI directly under the sentinel root is stored path-absolute
 * ({@code "/LICENSE"}): that is where a source's {@code </LICENSE>} lands, and
 * also where a reference more than {@value #SENTINEL_DEPTH} levels up
 * collapses, which resolves identically against any real serving URL.
 */
public final class RelativeIris {

    public static final String SENTINEL_PREFIX = "http://beakgraph.invalid/";
    public static final int SENTINEL_DEPTH = 32;
    public static final String SENTINEL_DIR = SENTINEL_PREFIX + "d/".repeat(SENTINEL_DEPTH);
    public static final String SENTINEL_BASE = SENTINEL_DIR + "%00";

    private RelativeIris() {}

    /**
     * The base a source document is parsed against. A single-source build
     * parses against {@link #SENTINEL_BASE}, so {@code <>} is stored as
     * {@code ""}. A merge (several inputs into one store) gives each document
     * its own base: the sentinel directory plus the document's path relative
     * to {@code root} (the CLI's {@code -src}; the inputs' common ancestor
     * directory when null). Relativized against {@link #SENTINEL_BASE} that
     * stores {@code <>} of {@code a/x.ttl} as {@code "a/x.ttl"} and its
     * {@code <img.png>} as {@code "a/img.png"}, so two documents' identical
     * relative references stay the distinct resources RFC 3986 says they
     * are, and the merged store serves them below its own URL exactly as the
     * source tree was laid out. One document merged alone parses like a
     * single-source build (the same rule that scopes its blank nodes).
     */
    public static String parseBase(File input, List<File> inputs, File root) {
        if (inputs == null || inputs.size() <= 1) {
            return SENTINEL_BASE;
        }
        Path rootPath = (root != null ? root.toPath() : commonAncestor(inputs)).toAbsolutePath().normalize();
        Path rel = rootPath.relativize(input.toPath().toAbsolutePath().normalize());
        StringBuilder sb = new StringBuilder(SENTINEL_DIR);
        for (int i = 0; i < rel.getNameCount(); i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(encodePathSegment(rel.getName(i).toString()));
        }
        return sb.toString();
    }

    /** Deepest directory containing every input (the input's own directory for one input). */
    public static Path commonAncestor(List<File> inputs) {
        Path common = null;
        for (File f : inputs) {
            Path dir = f.toPath().toAbsolutePath().normalize().getParent();
            if (common == null) {
                common = dir;
                continue;
            }
            while (common != null && !dir.startsWith(common)) {
                common = common.getParent();
            }
        }
        if (common == null) {
            throw new IllegalArgumentException("Sources share no common root: " + inputs);
        }
        return common;
    }

    /** Percent-encodes what an IRI path segment cannot carry verbatim; non-ASCII stays (IRIs allow it). */
    static String encodePathSegment(String segment) {
        StringBuilder sb = new StringBuilder(segment.length());
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            boolean keep = c >= 0x80
                    || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || "-._~!$&'()*+,;=:@".indexOf(c) >= 0;
            if (keep) {
                sb.append(c);
            } else {
                sb.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16)))
                  .append(Character.toUpperCase(Character.forDigit(c & 0xf, 16)));
            }
        }
        return sb.toString();
    }

    /** Whether {@code iri} was resolved against the sentinel and must be stored relative. */
    public static boolean isSentinelBased(String iri) {
        return iri != null && iri.startsWith(SENTINEL_PREFIX);
    }

    /**
     * Storage form of an IRI resolved against {@link #SENTINEL_BASE}:
     * {@code ""} for the document, {@code "#f"} / {@code "?q"} for its
     * fragments and queries, {@code "x/y"} for children, {@code "../x"} for
     * parents (any number of levels) and the path-absolute {@code "/x"} for
     * an IRI directly under the sentinel root. Null when {@code iri} is not
     * sentinel-based.
     */
    public static String toStorageForm(String iri) {
        if (!isSentinelBased(iri)) {
            return null;
        }
        String fromRoot = iri.substring(SENTINEL_PREFIX.length() - 1);   // "/..."
        if (!fromRoot.startsWith("/d/")) {
            return fromRoot;   // </LICENSE>, </a/b?x=1>, or collapsed above the sentinel's depth
        }
        return relativize(SENTINEL_BASE, iri);
    }

    /**
     * Relativizes {@code iri} against {@code base} textually (RFC 3986 §5.2
     * inverse). Returns null when the two differ in scheme or authority, or
     * either is not hierarchical; otherwise a reference that resolves back to
     * {@code iri} against {@code base}. Both inputs are expected in the
     * normalized form {@code IRIx} produces, so segments compare verbatim.
     */
    public static String relativize(String base, String iri) {
        int bAuth = authorityEnd(base);
        int tAuth = authorityEnd(iri);
        if (bAuth < 0 || tAuth < 0 || bAuth != tAuth || !base.regionMatches(0, iri, 0, bAuth)) {
            return null;
        }
        String[] b = splitPathQueryFragment(base.substring(bAuth));
        String[] t = splitPathQueryFragment(iri.substring(tAuth));
        if (t[0].equals(b[0])) {
            if (!t[1].isEmpty()) {
                return t[1] + t[2];                      // "?q", "?q#f"
            }
            if (b[1].isEmpty()) {
                return t[2];                             // "", "#f"
            }
            // Same path, no query, but the base has one: "" would inherit it.
            // Fall through to the segment form, which names the leaf explicitly.
        }
        List<String> baseDir = segments(b[0]);
        baseDir.remove(baseDir.size() - 1);              // drop the base's leaf
        List<String> target = segments(t[0]);
        int shared = 0;
        int max = Math.min(baseDir.size(), target.size() - 1);   // the target's leaf never matches a directory
        while (shared < max && baseDir.get(shared).equals(target.get(shared))) {
            shared++;
        }
        int ups = baseDir.size() - shared;
        String rest = String.join("/", target.subList(shared, target.size()));
        StringBuilder out = new StringBuilder();
        if (ups == 0) {
            // A child reference must not be mistaken for a scheme ("a:b") or an
            // authority ("//x"), and the directory itself needs "./", not "".
            String first = target.size() > shared ? target.get(shared) : "";
            if (rest.isEmpty() || first.isEmpty() || first.indexOf(':') >= 0) {
                out.append("./");
            }
        } else {
            out.append("../".repeat(ups));
        }
        return out.append(rest).append(t[1]).append(t[2]).toString();
    }

    /**
     * The path-absolute reference ({@code "/a/b?q#f"}) for {@code iri} when it
     * shares {@code base}'s scheme and authority, else null. The form the
     * writer stores for a source's {@code </a/b>} reference.
     */
    public static String pathAbsoluteForm(String base, String iri) {
        int bAuth = authorityEnd(base);
        int tAuth = authorityEnd(iri);
        if (bAuth < 0 || tAuth < 0 || bAuth != tAuth || !base.regionMatches(0, iri, 0, bAuth)) {
            return null;
        }
        String rest = iri.substring(tAuth);
        return rest.startsWith("/") ? rest : "/" + rest;
    }

    /** Index just past the authority of a hierarchical {@code scheme://authority...} IRI, or -1. */
    private static int authorityEnd(String iri) {
        if (iri == null) {
            return -1;
        }
        int scheme = iri.indexOf("://");
        if (scheme <= 0) {
            return -1;
        }
        for (int i = scheme + 3; i < iri.length(); i++) {
            char c = iri.charAt(i);
            if (c == '/' || c == '?' || c == '#') {
                return i;
            }
        }
        return iri.length();
    }

    /** {@code {path, "?query" or "", "#fragment" or ""}}; an empty path reads as "/". */
    private static String[] splitPathQueryFragment(String rest) {
        String fragment = "";
        int hash = rest.indexOf('#');
        if (hash >= 0) {
            fragment = rest.substring(hash);
            rest = rest.substring(0, hash);
        }
        String query = "";
        int q = rest.indexOf('?');
        if (q >= 0) {
            query = rest.substring(q);
            rest = rest.substring(0, q);
        }
        return new String[]{rest.isEmpty() ? "/" : rest, query, fragment};
    }

    /** Path segments after the leading slash; a trailing slash yields an empty leaf. */
    private static List<String> segments(String path) {
        String p = path.startsWith("/") ? path.substring(1) : path;
        return new ArrayList<>(Arrays.asList(p.split("/", -1)));
    }
}
