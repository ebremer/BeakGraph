package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.lws.LWSMetadataGenerator;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-179 / BG-397 / BG-41 / BG-42 / BG-222: the network-reachable SPARQL
 * surface of the LWS servlet, end to end through a real Jetty container -
 * routing of GET {@code ?query=}, form and direct POSTs, the 1 MiB body cap,
 * parse errors, the wall-clock timeout, content negotiation for SELECT and
 * ASK (with the mandatory {@code head} member), streamed CONSTRUCT / DESCRIBE
 * with the materialized-format cap, and the document-relative IRI contract:
 * a client may name the served document as {@code <>}, {@code <image.png>}
 * or by its absolute served URL, in triple patterns, FILTER, VALUES, GRAPH,
 * DESCRIBE and CONSTRUCT, and results come back absolute.
 */
class LWSSparqlEndpointTest {

    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        <> a geo:FeatureCollection ; ex:name "doc" ; ex:has <image.png> .
        <image.png> a ex:Image ; ex:width 10 .
        <#frag> ex:name "fragment" .
        ex:abs ex:p ex:o ; ex:says <<( ex:a ex:b ex:c )>> .
        ex:s1 ex:p ex:o1 . ex:s2 ex:p ex:o2 . ex:s3 ex:p ex:o3 . ex:s4 ex:p ex:o4 .
        ex:g1 { ex:x ex:p <image.png> . ex:x ex:name "in g1" . }
        """;

    @TempDir
    static Path dir;
    private static Server server;
    private static String base;
    private static Path h5;
    private static final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @BeforeAll
    static void startServer() throws Exception {
        Path root = Files.createDirectories(dir.resolve("storage"));
        Path trig = dir.resolve("data.trig");
        Files.writeString(trig, TRIG, StandardCharsets.UTF_8);
        h5 = root.resolve("data.h5");
        HDF5Writer.Builder().setSource(trig.toFile()).setDestination(h5.toFile())
                .setSpatial(false).setFeatures(false).build().write();
        Model model = LWSMetadataGenerator.generateLWSModel(root);
        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder(new LWSStorageServlet(model, null, root)), "/*");
        server.setHandler(ctx);
        server.start();
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        base = "http://localhost:" + port + "/";
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) server.stop();
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String doc() {
        return base + "data.h5";
    }

    private static HttpResponse<String> get(String sparql, String accept) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(doc() + "?query=" + enc(sparql))).timeout(Duration.ofSeconds(120));
        if (accept != null) b.header("Accept", accept);
        return http.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(String body, String contentType, String accept) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(doc())).timeout(Duration.ofSeconds(120))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (accept != null) b.header("Accept", accept);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String contentType(HttpResponse<?> r) {
        return r.headers().firstValue("Content-Type").orElse("");
    }

    private static ResultSet json(HttpResponse<String> r) {
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(contentType(r).startsWith("application/sparql-results+json"), contentType(r));
        return ResultSetFactory.fromJSON(new ByteArrayInputStream(r.body().getBytes(StandardCharsets.UTF_8)));
    }

    private static List<String> column(ResultSet rs, String var) {
        List<String> out = new ArrayList<>();
        while (rs.hasNext()) {
            out.add(String.valueOf(rs.next().get(var)));
        }
        return out;
    }

    private static boolean askJson(String sparql) throws Exception {
        HttpResponse<String> r = get(sparql, "application/sparql-results+json");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"head\""), "SPARQL Results JSON requires the head member: " + r.body());
        String compact = r.body().replace(" ", "").replace("\n", "");
        assertTrue(compact.contains("\"boolean\":true") || compact.contains("\"boolean\":false"), r.body());
        return compact.contains("\"boolean\":true");
    }

    // ---- (a)-(f) routing and negotiation --------------------------------------

    @Test
    void getQueryNegotiatesJsonAndDefaultsToXml() throws Exception {
        String q = "PREFIX ex: <http://ex.org/> SELECT ?s WHERE { ?s ex:p ?o }";
        ResultSet rs = json(get(q, "application/sparql-results+json"));
        assertEquals(5, column(rs, "s").size(), "ex:abs and ex:s1..s4");

        HttpResponse<String> xml = get(q, null);
        assertEquals(200, xml.statusCode(), xml.body());
        assertTrue(contentType(xml).startsWith("application/sparql-results+xml"), contentType(xml));
        ResultSet parsed = ResultSetFactory.fromXML(new ByteArrayInputStream(xml.body().getBytes(StandardCharsets.UTF_8)));
        assertEquals(5, column(parsed, "s").size());

        HttpResponse<String> csv = get(q, "text/csv");
        assertEquals(200, csv.statusCode());
        assertTrue(contentType(csv).startsWith("text/csv"));
    }

    @Test
    void postFormAndDirectBodiesAreRouted() throws Exception {
        String q = "PREFIX ex: <http://ex.org/> SELECT ?s WHERE { ?s ex:p ?o }";
        HttpResponse<String> form = post("query=" + enc(q), "application/x-www-form-urlencoded", "application/sparql-results+json");
        assertEquals(5, column(json(form), "s").size(), "form-encoded POST");
        HttpResponse<String> direct = post(q, "application/sparql-query; charset=utf-8", "application/sparql-results+json");
        assertEquals(5, column(json(direct), "s").size(), "direct POST with a charset parameter on the media type");
    }

    @Test
    void oversizedBodyIs413AndParseErrorIs400() throws Exception {
        byte[] big = new byte[BGSparqlService.MAX_QUERY_BODY_BYTES + 1];
        java.util.Arrays.fill(big, (byte) ' ');
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(doc())).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/sparql-query")
                .POST(HttpRequest.BodyPublishers.ofByteArray(big)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(413, r.statusCode());
        assertEquals(400, get("SELECT ?s WHERE { ?s ?p }", "application/sparql-results+json").statusCode());
        assertEquals(400, get("SELECT * WHERE { ?s ?p ?o } ORDER BY", null).statusCode());
    }

    @Test
    void slowQueryIsCancelledWith503() throws Exception {
        String old = System.getProperty("beakgraph.query.timeout.seconds");
        System.setProperty("beakgraph.query.timeout.seconds", "1");
        try {
            String cross = "SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i . ?j ?k ?l . ?m ?n ?o . ?p ?q ?r . ?s ?t ?u . ?v ?w ?x }";
            long start = System.nanoTime();
            HttpResponse<String> r = get(cross, "application/sparql-results+json");
            assertEquals(503, r.statusCode(), r.body());
            assertTrue(Duration.ofNanos(System.nanoTime() - start).getSeconds() < 60, "the limit must cut the query short");
            // The reader survives a timeout: the next query answers normally.
            assertEquals(5, column(json(get("PREFIX ex: <http://ex.org/> SELECT ?s WHERE { ?s ex:p ?o }",
                    "application/sparql-results+json")), "s").size());
        } finally {
            if (old == null) System.clearProperty("beakgraph.query.timeout.seconds");
            else System.setProperty("beakgraph.query.timeout.seconds", old);
        }
    }

    // ---- ASK (BG-42) ------------------------------------------------------------

    @Test
    void askNegotiatesAndCarriesTheHeadMember() throws Exception {
        String q = "PREFIX ex: <http://ex.org/> ASK { ex:abs ex:p ex:o }";
        HttpResponse<String> j = get(q, "application/sparql-results+json");
        assertEquals(200, j.statusCode());
        assertTrue(contentType(j).startsWith("application/sparql-results+json"));
        assertTrue(j.body().contains("\"head\""), j.body());
        assertTrue(j.body().replace(" ", "").contains("\"boolean\":true"), j.body());

        HttpResponse<String> x = get(q, "application/sparql-results+xml");
        assertEquals(200, x.statusCode());
        assertTrue(contentType(x).startsWith("application/sparql-results+xml"), contentType(x));
        assertTrue(x.body().contains("<boolean>true</boolean>"), x.body());

        HttpResponse<String> none = get("PREFIX ex: <http://ex.org/> ASK { ex:abs ex:p ex:nowhere }", null);
        assertTrue(contentType(none).startsWith("application/sparql-results+xml"), "no Accept: XML, as for SELECT");
        assertTrue(none.body().contains("<boolean>false</boolean>"), none.body());
    }

    // ---- document-relative IRIs (BG-41, BG-397) ---------------------------------

    @Test
    void relativeReferencesInTheQueryNameTheDocument() throws Exception {
        String prefixes = "PREFIX ex: <http://ex.org/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
        assertTrue(askJson(prefixes + "ASK { <> a geo:FeatureCollection }"), "<> is the served document");
        assertTrue(askJson(prefixes + "ASK { <image.png> a ex:Image }"), "<image.png> is the sibling");
        assertTrue(askJson(prefixes + "ASK { <#frag> ex:name \"fragment\" }"));
        assertTrue(askJson(prefixes + "ASK { <" + doc() + "> a geo:FeatureCollection }"), "the absolute served URL");
        assertTrue(askJson(prefixes + "ASK { <" + base + "image.png> a ex:Image }"), "the absolute sibling URL");
        assertFalse(askJson(prefixes + "ASK { <" + base + "nowhere.png> ?p ?o }"));
        assertTrue(askJson(prefixes + "BASE <http://elsewhere.example/> ASK { <> a geo:FeatureCollection }") == false,
                "an explicit BASE in the query wins over the served URL");
    }

    @Test
    void absoluteIrisAreRewrittenInEveryPosition() throws Exception {
        String prefixes = "PREFIX ex: <http://ex.org/> ";
        String abs = "<" + doc() + ">";
        String img = "<" + base + "image.png>";
        assertEquals(List.of("doc"), column(json(get(prefixes + "SELECT ?n WHERE { ?s ex:name ?n FILTER(?s = " + abs + ") }",
                "application/sparql-results+json")), "n").stream().map(s -> s.replace("\"", "")).toList(), "FILTER");
        System.out.println("VALUES-BODY>>>" + get(prefixes + "SELECT ?n WHERE { VALUES ?s { " + abs + " } ?s ex:name ?n }", "application/sparql-results+json").body() + "<<<");
        assertEquals(List.of("doc"), column(json(get(prefixes + "SELECT ?n WHERE { VALUES ?s { " + abs + " } ?s ex:name ?n }",
                "application/sparql-results+json")), "n").stream().map(s -> s.replace("\"", "")).toList(), "VALUES");
        assertEquals(List.of("in g1"), column(json(get(prefixes + "SELECT ?n WHERE { GRAPH <http://ex.org/g1> { ?x ex:p " + img + " . ?x ex:name ?n } }",
                "application/sparql-results+json")), "n").stream().map(s -> s.replace("\"", "")).toList(), "GRAPH");
        // Results come back absolute: the stored relative <image.png> is served as its URL.
        List<String> has = column(json(get(prefixes + "SELECT ?o WHERE { " + abs + " ex:has ?o }", "application/sparql-results+json")), "o");
        assertEquals(List.of(base + "image.png"), has);
    }

    @Test
    void constructAndDescribeResolveAndStream() throws Exception {
        String prefixes = "PREFIX ex: <http://ex.org/> ";
        String abs = "<" + doc() + ">";
        HttpResponse<String> d = get("DESCRIBE " + abs, "text/turtle");
        assertEquals(200, d.statusCode(), d.body());
        assertTrue(contentType(d).startsWith("text/turtle"));
        Model described = ModelFactory.createDefaultModel();
        RDFDataMgr.read(described, new ByteArrayInputStream(d.body().getBytes(StandardCharsets.UTF_8)), Lang.TURTLE);
        System.out.println("DESCRIBE-BODY>>>" + d.body() + "<<<");
        assertTrue(described.containsResource(described.createResource(doc())), d.body());
        assertFalse(d.body().contains("<>"), "no relative IRI leaves the server: " + d.body());
        assertTrue(d.body().contains(base + "image.png"), d.body());

        HttpResponse<String> nt = get(prefixes + "CONSTRUCT { ?s ex:p ?o } WHERE { ?s ex:p ?o }", "application/n-triples");
        assertEquals(200, nt.statusCode(), nt.body());
        assertTrue(contentType(nt).startsWith("application/n-triples"), contentType(nt));
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new ByteArrayInputStream(nt.body().getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
        assertEquals(5, m.size(), "ex:abs and ex:s1..s4 carry ex:p in the default graph: " + nt.body());

        // A triple term streams in Turtle and is refused for JSON-LD.
        HttpResponse<String> tt = get(prefixes + "CONSTRUCT { ?s ex:says ?o } WHERE { ?s ex:says ?o }", "text/turtle");
        assertEquals(200, tt.statusCode(), tt.body());
        assertTrue(tt.body().contains("<<("), tt.body());
        assertEquals(400, get(prefixes + "CONSTRUCT { ?s ex:says ?o } WHERE { ?s ex:says ?o }", "application/ld+json").statusCode());
    }

    @Test
    void materializedFormatsAreCappedWhileTurtleStreams() throws Exception {
        String old = System.getProperty("beakgraph.query.construct.max.triples");
        System.setProperty("beakgraph.query.construct.max.triples", "3");
        try {
            String q = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
            HttpResponse<String> jsonld = get(q, "application/ld+json");
            assertEquals(413, jsonld.statusCode(), jsonld.body());
            HttpResponse<String> xml = get(q, "application/rdf+xml");
            assertEquals(413, xml.statusCode(), xml.body());
            HttpResponse<String> ttl = get(q, "text/turtle");
            assertEquals(200, ttl.statusCode(), ttl.body());
            Model m = ModelFactory.createDefaultModel();
            RDFDataMgr.read(m, new ByteArrayInputStream(ttl.body().getBytes(StandardCharsets.UTF_8)), Lang.TURTLE);
            assertTrue(m.size() > 3, "streamed Turtle is not capped: " + m.size());
            HttpResponse<String> small = get("PREFIX ex: <http://ex.org/> CONSTRUCT { ?s ex:name ?n } WHERE { ?s ex:name ?n }", "application/ld+json");
            assertEquals(200, small.statusCode(), "a result within the cap still serializes: " + small.body());
        } finally {
            if (old == null) System.clearProperty("beakgraph.query.construct.max.triples");
            else System.setProperty("beakgraph.query.construct.max.triples", old);
        }
    }

    // ---- the rewrite is what finds the rows (direct call, no base) ----------------

    /** A recording HttpServletResponse: content type, status of sendError, and the body. */
    private static final class Recorded {
        String contentType;
        int error;
        final ByteArrayOutputStream body = new ByteArrayOutputStream();

        HttpServletResponse proxy() {
            ServletOutputStream sos = new ServletOutputStream() {
                @Override public void write(int b) { body.write(b); }
                @Override public boolean isReady() { return true; }
                @Override public void setWriteListener(WriteListener l) { }
            };
            return (HttpServletResponse) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{HttpServletResponse.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "setContentType" -> { contentType = (String) args[0]; return null; }
                            case "getOutputStream" -> { return sos; }
                            case "getWriter" -> { return new PrintWriter(body, true, StandardCharsets.UTF_8); }
                            case "sendError" -> { error = (Integer) args[0]; return null; }
                            case "isCommitted" -> { return false; }
                            case "toString" -> { return "Recorded"; }
                            default -> {
                                Class<?> rt = method.getReturnType();
                                if (rt == boolean.class) return false;
                                if (rt == int.class) return 0;
                                if (rt == long.class) return 0L;
                                return null;
                            }
                        }
                    });
        }
    }

    @Test
    void withoutAServedBaseTheAbsoluteUrlMatchesNothing() throws Exception {
        BeakGraph bg = BG.getBeakGraph(h5.toFile());
        try {
            Dataset ds = bg.getDataset();
            String q = "PREFIX ex: <http://ex.org/> SELECT ?n WHERE { <" + doc() + "> ex:name ?n }";
            Recorded with = new Recorded();
            assertTrue(BGSparqlService.execute(ds, q, doc(), "application/sparql-results+json", with.proxy()));
            assertEquals(0, with.error);
            ResultSet rs = ResultSetFactory.fromJSON(new ByteArrayInputStream(with.body.toByteArray()));
            assertEquals(1, column(rs, "n").size(), "with the served base the absolute URL is relativized to the stored <>");

            Recorded without = new Recorded();
            assertTrue(BGSparqlService.execute(ds, q, null, "application/sparql-results+json", without.proxy()));
            assertEquals(0, without.error);
            rs = ResultSetFactory.fromJSON(new ByteArrayInputStream(without.body.toByteArray()));
            assertEquals(0, column(rs, "n").size(), "without a base nothing is rewritten, so the absolute URL misses");

            // A non-BG dataset holding the same data has no stored-relative terms: no rewrite, no rows.
            Dataset plain = org.apache.jena.query.DatasetFactory.create();
            RDFDataMgr.read(plain, new ByteArrayInputStream(TRIG.getBytes(StandardCharsets.UTF_8)), Lang.TRIG);
            Recorded other = new Recorded();
            assertTrue(BGSparqlService.execute(plain, q, doc(), "application/sparql-results+json", other.proxy()));
            rs = ResultSetFactory.fromJSON(new ByteArrayInputStream(other.body.toByteArray()));
            assertEquals(0, column(rs, "n").size());
            assertEquals(0, other.error, "a plain dataset is queried as-is, not refused");
        } finally {
            bg.close();
        }
    }

    @Test
    void resultSetLangsAreTheStandardOnes() {
        // Guards the media types this test asserts against Jena's registry.
        assertEquals("application/sparql-results+json", ResultSetLang.RS_JSON.getContentType().getContentTypeStr());
        assertEquals("application/sparql-results+xml", ResultSetLang.RS_XML.getContentType().getContentTypeStr());
    }
}
