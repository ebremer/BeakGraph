package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import java.io.File;
import java.nio.charset.StandardCharsets;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end parity: the disk-based writer must produce a store that reads
 * back (through the UNMODIFIED jHDF-based readers) semantically identical to
 * the RAM writer's - same graphs, isomorphic per-graph content (blank node
 * labels legitimately differ), same query answers through both indexes, and
 * structurally identical HDF5 metadata (same dataset tree, same numEntries /
 * width / FCD attributes everywhere). The mixed fixture carries every term
 * kind the disk pipeline handles specially: base-direction literals, nested
 * triple terms and composite (cdt:) literals (BG-126).
 */
class HugeWriterParityTest {

    @TempDir
    static Path dir;

    @BeforeAll
    static void requireNative() {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static Path writeBoth(String name, String content, boolean spatial, boolean features) throws Exception {
        File src = dir.resolve(name).toFile();
        Files.write(src.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return writeBoth(name, src, spatial, features);
    }

    /** Writes src with both writers; returns the directory holding {name}.ram.h5 / {name}.huge.h5. */
    private static Path writeBoth(String name, File src, boolean spatial, boolean features) throws Exception {
        File ram = dir.resolve(name + ".ram.h5").toFile();
        File huge = dir.resolve(name + ".huge.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(ram)
                .setSpatial(spatial).setFeatures(features).build().write();
        HugeHDF5Writer.Builder().setSource(src).setDestination(huge)
                .setSpatial(spatial).setFeatures(features)
                // deliberately tiny batches: force spill runs and multi-level merges
                .setTermSpillBatch(512).setIdSpillBatch(1024).setMergeFanIn(4)
                .build().write();
        return dir;
    }

    private static Set<String> graphNames(org.apache.jena.query.Dataset ds) {
        Set<String> names = new TreeSet<>();
        ds.asDatasetGraph().listGraphNodes().forEachRemaining(g -> names.add(g.toString()));
        return names;
    }

    private static Model graphModel(org.apache.jena.query.Dataset ds, String graphUri) {
        String q = (graphUri == null)
                ? "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
                : "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + graphUri + "> { ?s ?p ?o } }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            return qe.execConstruct();
        }
    }

    private static java.util.List<String> select(org.apache.jena.query.Dataset ds, String query) {
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

    /** Semantic equality of the two stores, graph by graph. */
    private static void assertStoresEquivalent(Path ramH5, Path hugeH5, String... probes) throws Exception {
        try (BeakGraph a = new BeakGraph(new HDF5Reader(ramH5.toFile()));
             BeakGraph b = new BeakGraph(new HDF5Reader(hugeH5.toFile()))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();

            Set<String> graphsA = graphNames(da);
            Set<String> graphsB = graphNames(db);
            assertEquals(graphsA, graphsB, "graph lists must match");

            Model defA = graphModel(da, null);
            Model defB = graphModel(db, null);
            assertTrue(defA.isIsomorphicWith(defB),
                    "default graphs must be isomorphic (ram=" + defA.size() + ", huge=" + defB.size() + ")");
            for (String g : graphsA) {
                if (!g.startsWith("http") && !g.startsWith("urn")) continue; // skip any non-URI form
                Model ga = graphModel(da, g);
                Model gb = graphModel(db, g);
                assertTrue(ga.isIsomorphicWith(gb),
                        "graph <" + g + "> must be isomorphic (ram=" + ga.size() + ", huge=" + gb.size() + ")");
            }
            for (String probe : probes) {
                assertEquals(select(da, probe), select(db, probe), "probe results must match: " + probe);
            }
        }
    }

    /**
     * Structural parity of the HDF5 trees: identical group/dataset names and
     * identical attribute values everywhere. Counts and bit widths are
     * bnode-order independent, so this must hold exactly even though raw bytes
     * may differ (blank node ranks differ between the writers).
     */
    private static void assertSameStructure(Path ramH5, Path hugeH5) {
        try (HdfFile ram = new HdfFile(ramH5); HdfFile huge = new HdfFile(hugeH5)) {
            compareGroups("/", (Group) ram.getChild(".BG"), (Group) huge.getChild(".BG"));
        }
    }

    private static void compareGroups(String path, Group a, Group b) {
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

    private static void compareAttributes(String path, Node a, Node b) {
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

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    @Test
    void mixedTypesAndNamedGraphs() throws Exception {
        String longStr = "y".repeat(150);
        String trig = """
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
        writeBoth("mixed.trig", trig, false, false);
        Path ram = dir.resolve("mixed.trig.ram.h5");
        Path huge = dir.resolve("mixed.trig.huge.h5");
        assertStoresEquivalent(ram, huge,
                "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
                "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
                "SELECT ?s ?o WHERE { GRAPH <http://ex.org/g1> { ?s <http://ex.org/p0> ?o } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o }",
                "SELECT ?g ?s WHERE { GRAPH ?g { ?s <http://ex.org/p0> <http://ex.org/o0> } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/dl> ?o }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/tt> ?o }",
                "SELECT ?x WHERE { <http://ex.org/s0> <http://ex.org/tt2> <<( <http://ex.org/a> <http://ex.org/b> <<( <http://ex.org/x> <http://ex.org/y> ?x )>> )>> }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/list> ?o }");
        assertSameStructure(ram, huge);
    }

    @Test
    void spatialAndFeaturesParity() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            @prefix geo: <http://www.opengis.net/ont/geosparql#> .

            ex:geo1 geo:asWKT "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))"^^geo:wktLiteral .
            ex:geo2 geo:asWKT "MULTIPOLYGON(((200 200, 300 200, 300 300, 200 200)),((600 600, 700 600, 700 700, 600 600)))"^^geo:wktLiteral .
            ex:geo3 geo:asWKT "POINT(5000 6000)"^^geo:wktLiteral .
            ex:geo4 geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POLYGON((10 10, 60 10, 60 60, 10 60, 10 10))"^^geo:wktLiteral .
            ex:geo5 geo:asWKT "POLYGON((0 0, 1 0))"^^geo:wktLiteral .
            ex:geo1 ex:label "region one" .
            """;
        writeBoth("spatial.trig", trig, true, true);
        Path ram = dir.resolve("spatial.trig.ram.h5");
        Path huge = dir.resolve("spatial.trig.huge.h5");
        assertStoresEquivalent(ram, huge,
                "SELECT ?s ?p ?o WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }",
                "SELECT ?g WHERE { GRAPH ?g { <http://ex.org/geo1> ?p ?o } }");
        assertSameStructure(ram, huge);
    }

    @Test
    void stressManySpillRunsMatchesRamWriter() throws Exception {
        StringBuilder nq = new StringBuilder(1 << 22);
        for (int i = 0; i < 30_000; i++) {
            String s = "<http://ex.org/s" + (i % 500) + ">";
            String p = "<http://ex.org/p" + (i % 7) + ">";
            String o = switch (i % 4) {
                case 0 -> "<http://ex.org/o" + (i % 300) + ">";
                case 1 -> "\"str" + (i % 1000) + "\"";
                case 2 -> "\"" + (i % 1000 - 500) + "\"^^<http://www.w3.org/2001/XMLSchema#int>";
                default -> "\"" + ((i % 100) / 8.0) + "\"^^<http://www.w3.org/2001/XMLSchema#double>";
            };
            nq.append(s).append(' ').append(p).append(' ').append(o);
            if (i % 3 != 0) {
                nq.append(" <http://ex.org/g").append(i % 20).append('>');
            }
            nq.append(" .\n");
        }
        writeBoth("stress.nq", nq.toString(), false, false);
        Path ram = dir.resolve("stress.nq.ram.h5");
        Path huge = dir.resolve("stress.nq.huge.h5");
        assertStoresEquivalent(ram, huge,
                "SELECT ?s ?o WHERE { ?s <http://ex.org/p3> ?o }",
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g",
                "SELECT ?s WHERE { GRAPH <http://ex.org/g7> { ?s <http://ex.org/p1> \"str101\" } }");
        assertSameStructure(ram, huge);
    }

    /**
     * BG-126: per-document blank-node scoping must reach INSIDE triple terms
     * (HugeBuildPipeline.scopeNode recurses): two documents that both say
     * {@code _:b0} outside and inside a term stay two nodes, each co-referring
     * with itself, and the merge agrees with the sequential writer's.
     */
    @Test
    void mergeScopesBlankNodesPerDocumentInsideTripleTerms() throws Exception {
        Path src = Files.createDirectories(dir.resolve("ttmerge"));
        for (int i = 1; i <= 2; i++) {
            Files.write(src.resolve("d" + i + ".ttl"), ("@prefix : <http://ex.org/> .\n"
                    + "_:b0 :p <<( _:b0 :q :r )>> .\n_:b0 :label \"doc" + i + "\" .\n")
                    .getBytes(StandardCharsets.UTF_8));
        }
        java.util.List<File> inputs = java.util.List.of(src.resolve("d1.ttl").toFile(), src.resolve("d2.ttl").toFile());
        File ram = dir.resolve("ttmerge.ram.h5").toFile();
        File huge = dir.resolve("ttmerge.huge.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(ram).build().write();
        HugeHDF5Writer.Builder().setSources(inputs).setDestination(huge)
                .setTermSpillBatch(4).setIdSpillBatch(4).setMergeFanIn(2).build().write();
        String pre = "PREFIX : <http://ex.org/> ";
        assertStoresEquivalent(ram.toPath(), huge.toPath(),
                pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :label ?l }",
                pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :p <<( ?b :q :r )>> }",
                pre + "SELECT ?l WHERE { ?b :label ?l . ?b :p <<( ?b :q :r )>> }");
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(huge))) {
            assertEquals(java.util.List.of("n=\"2\"^^xsd:integer|"), select(bg.getDataset(),
                    pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :p <<( ?b :q :r )>> }"),
                    "each document's _:b0 co-refers with the one inside its own term");
            assertEquals(java.util.List.of("n=\"2\"^^xsd:integer|"), select(bg.getDataset(),
                    pre + "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b :label ?l }"));
        }
    }
}
