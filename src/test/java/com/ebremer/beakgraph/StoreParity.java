package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;

/**
 * The ONE store-comparison harness the writer-parity suites share (BG-188):
 * semantic equivalence graph by graph plus probe queries, structural parity
 * of the HDF5 trees (names, dataset sizes, attributes), byte identity for the
 * engines that promise it, VoID-graph parity, and the two fixtures every
 * suite feeds its engine. Four suites used to carry ~120-line copies of this
 * - the same mirror-topology hazard {@link WriterEngines} exists to avoid.
 */
public final class StoreParity {

    private StoreParity() {}

    /** The mixed-types / named-graphs / bnodes / relative-IRI / triple-term fixture (TriG). */
    public static String mixedTrig() {
        String longStr = "y".repeat(150);
        return """
            @prefix ex: <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            ex:s0 ex:p0 ex:o0 .
            ex:s0 ex:p0 ex:o0 .
            ex:s0 ex:name "Alice" .
            ex:s0 ex:name "Alice"@en .
            ex:s0 ex:name "Alicia"@fr-CA .
            ex:s0 ex:count "42"^^xsd:int .
            ex:s0 ex:count "042"^^xsd:int .
            ex:s0 ex:count2 "-7"^^xsd:int .
            ex:s0 ex:big "9223372036854775806"^^xsd:long .
            ex:s0 ex:neg "-9007199254740993"^^xsd:long .
            ex:s0 ex:f "1.5"^^xsd:float .
            ex:s0 ex:d "-2.25E8"^^xsd:double .
            ex:s0 ex:unbounded "123456789012345678901234567890"^^xsd:integer .
            ex:s0 ex:flag true .
            ex:s0 ex:when "2024-05-06T07:08:09Z"^^xsd:dateTime .
            ex:s0 ex:longstr "%s" .
            ex:s0 ex:uni "h\\u00e9llo \\u00fcrld" .
            ex:s0 ex:ill "abc"^^xsd:int .
            ex:s0 ex:dl "hello"@en--ltr .
            ex:s0 ex:dl "hi"@en--rtl .
            ex:s0 ex:tt <<( ex:a ex:b ex:c )>> .
            ex:s0 ex:tt2 <<( ex:a ex:b <<( ex:x ex:y "nested" )>> )>> .
            ex:s0 ex:list "[1, 2]"^^<http://w3id.org/awslabs/neptune/SPARQL-CDTs/List> .
            ex:s0 ex:map "{\\"k\\": 1}"^^<http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map> .
            <> ex:self <sibling.png> ; ex:up <../up.png> ; ex:up2 <../../up2.png> ; ex:root </root.png> ; ex:frag <#frag> ; ex:query <?q=1> .
            _:b1 ex:p0 _:b2 .
            _:b2 ex:knows ex:s0 .
            ex:g1 {
                ex:s1 ex:p0 ex:o0 .
                ex:s1 ex:num "10"^^xsd:int .
                _:b1 ex:inGraph ex:g1 .
            }
            ex:g2 { ex:s0 ex:p0 ex:s1 . }
            """.formatted(longStr);
    }

    /** The geometry fixture: polygons, a multipolygon, a point, a CRS-prefixed WKT and a degenerate ring. */
    public static final String SPATIAL_TRIG = """
            @prefix ex: <http://ex.org/> .
            @prefix geo: <http://www.opengis.net/ont/geosparql#> .

            ex:geo1 geo:asWKT "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))"^^geo:wktLiteral .
            ex:geo2 geo:asWKT "MULTIPOLYGON(((200 200, 300 200, 300 300, 200 200)),((600 600, 700 600, 700 700, 600 600)))"^^geo:wktLiteral .
            ex:geo3 geo:asWKT "POINT(5000 6000)"^^geo:wktLiteral .
            ex:geo4 geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POLYGON((10 10, 60 10, 60 60, 10 60, 10 10))"^^geo:wktLiteral .
            ex:geo5 geo:asWKT "POLYGON((0 0, 1 0))"^^geo:wktLiteral .
            ex:geo1 ex:label "region one" .
            """;

    public static Set<String> graphNames(org.apache.jena.query.Dataset ds) {
        Set<String> names = new TreeSet<>();
        ds.asDatasetGraph().listGraphNodes().forEachRemaining(g -> names.add(g.toString()));
        return names;
    }

    public static Model graphModel(org.apache.jena.query.Dataset ds, String graphUri) {
        String q = (graphUri == null)
                ? "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
                : "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + graphUri + "> { ?s ?p ?o } }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            return qe.execConstruct();
        }
    }

    public static java.util.List<String> select(org.apache.jena.query.Dataset ds, String query) {
        java.util.List<String> rows = new java.util.ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                rs.getResultVars().forEach(v -> sb.append(v).append('=').append(qs.get(v)).append('|'));
                rows.add(sb.toString());
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    /** {@code SELECT (COUNT(..) AS ?n)}: the count. */
    public static long count(org.apache.jena.query.Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            return qe.execSelect().next().getLiteral("n").getLong();
        }
    }

    /** Semantic equality of the two stores, graph by graph. */
    public static void assertStoresEquivalent(Path seqH5, Path parH5, String... probes) throws Exception {
        try (BeakGraph a = new BeakGraph(new HDF5Reader(seqH5.toFile()));
             BeakGraph b = new BeakGraph(new HDF5Reader(parH5.toFile()))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();

            assertEquals(graphNames(da), graphNames(db), "graph lists must match");

            Model defA = graphModel(da, null);
            Model defB = graphModel(db, null);
            assertTrue(defA.isIsomorphicWith(defB),
                    "default graphs must be isomorphic (" + seqH5.getFileName() + "=" + defA.size() + ", " + parH5.getFileName() + "=" + defB.size() + ")");
            for (String g : graphNames(da)) {
                if (!g.startsWith("http") && !g.startsWith("urn")) continue; // skip any non-URI form
                Model ga = graphModel(da, g);
                Model gb = graphModel(db, g);
                assertTrue(ga.isIsomorphicWith(gb),
                        "graph <" + g + "> must be isomorphic (" + seqH5.getFileName() + "=" + ga.size() + ", " + parH5.getFileName() + "=" + gb.size() + ")");
            }
            for (String probe : probes) {
                assertEquals(select(da, probe), select(db, probe), "probe results must match: " + probe);
            }
        }
    }

    /**
     * Structural parity of the HDF5 trees: identical group/dataset names,
     * identical dataset sizes, and identical attribute values everywhere.
     * Counts and bit widths are bnode-order independent, so this must hold
     * exactly even though raw bytes may differ (see the class comment).
     */
    public static void assertSameStructure(Path seqH5, Path parH5) {
        try (HdfFile seq = new HdfFile(seqH5); HdfFile par = new HdfFile(parH5)) {
            compareGroups("/", (Group) seq.getChild(".BG"), (Group) par.getChild(".BG"));
        }
    }

    public static void compareGroups(String path, Group a, Group b) {
        assertEquals(a != null, b != null, "group presence at " + path);
        if (a == null) return;
        compareAttributes(path, a, b);
        Map<String, Node> ca = a.getChildren();
        Map<String, Node> cb = b.getChildren();
        assertEquals(new TreeSet<>(ca.keySet()), new TreeSet<>(cb.keySet()), "children of " + path);
        for (String name : ca.keySet()) {
            Node na = ca.get(name);
            Node nb = cb.get(name);
            assertEquals(na instanceof Group, nb instanceof Group, "node kind of " + path + name);
            if (na instanceof Group ga) {
                compareGroups(path + name + "/", ga, (Group) nb);
            } else {
                compareAttributes(path + name, na, nb);
                assertEquals(((Dataset) na).getSize(), ((Dataset) nb).getSize(),
                        "dataset size of " + path + name);
            }
        }
    }

    public static void compareAttributes(String path, Node a, Node b) {
        Map<String, Object> attrsA = new HashMap<>();
        Map<String, Object> attrsB = new HashMap<>();
        for (Map.Entry<String, Attribute> e : a.getAttributes().entrySet()) {
            attrsA.put(e.getKey(), e.getValue().getData());
        }
        for (Map.Entry<String, Attribute> e : b.getAttributes().entrySet()) {
            attrsB.put(e.getKey(), e.getValue().getData());
        }
        assertEquals(attrsA, attrsB, "attributes of " + path);
    }

    /**
     * Methods 0/2/3 are one format written through one jHDF sequence
     * (SPECIFICATIONS.md: "byte-identical stores"), so for a single source
     * the files must match byte for byte - not merely in structure. This is
     * what catches a different bit layout or FCD block content that the
     * readers still decode consistently.
     */
    public static void assertSameBytes(Path a, Path b) throws Exception {
        assertArrayEquals(Files.readAllBytes(a), Files.readAllBytes(b),
                "single-source stores must be byte-identical: " + a.getFileName() + " vs " + b.getFileName());
    }

    /**
     * BG-112: the statistics graph of two stores must be isomorphic and every
     * (predicate, value) pair in it must agree - void:triples,
     * distinctSubjects/Objects, properties, classes, entities and the
     * partitions are all literal or IRI values hanging off blank nodes, so
     * this is label-independent and complete.
     */
    public static void assertVoidParity(Path seqH5, Path otherH5, String label) throws Exception {
        try (BeakGraph a = new BeakGraph(new HDF5Reader(seqH5.toFile()));
             BeakGraph b = new BeakGraph(new HDF5Reader(otherH5.toFile()))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();
            Model va = graphModel(da, Params.VOIDSTRING);
            Model vb = graphModel(db, Params.VOIDSTRING);
            assertTrue(va.size() > 10, label + ": the statistics graph exists (" + va.size() + ")");
            assertTrue(va.isIsomorphicWith(vb), label + ": statistics graphs must be isomorphic (seq=" + va.size() + ", other=" + vb.size() + ")");
            String g = "GRAPH <" + Params.VOIDSTRING + ">";
            for (String probe : new String[]{
                    "SELECT ?p (COUNT(*) AS ?n) WHERE { " + g + " { ?s ?p ?o } } GROUP BY ?p ORDER BY ?p",
                    "SELECT ?p ?o WHERE { " + g + " { ?s ?p ?o } FILTER(isLiteral(?o)) } ORDER BY ?p ?o",
                    "SELECT ?p ?o WHERE { " + g + " { ?s ?p ?o } FILTER(isIRI(?o)) } ORDER BY ?p ?o",
                    "SELECT ?s ?p ?o WHERE { " + g + " { ?s ?p ?o } FILTER(isIRI(?s) && !isBlank(?o)) } ORDER BY ?s ?p ?o"}) {
                assertEquals(select(da, probe), select(db, probe), label + ": " + probe);
            }
        }
    }
}
