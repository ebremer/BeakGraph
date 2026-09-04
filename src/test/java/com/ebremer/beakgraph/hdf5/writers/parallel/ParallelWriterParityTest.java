package com.ebremer.beakgraph.hdf5.writers.parallel;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end parity: the parallel writer must produce a store that reads back
 * (through the UNMODIFIED jHDF-based readers) semantically identical to the
 * sequential writer's - same graphs, isomorphic per-graph content, same query
 * answers through both indexes, and structurally identical HDF5 metadata
 * (same dataset tree, same sizes, same numEntries / width / FCD attributes
 * everywhere). Raw bytes are NOT compared: the VoID generator mints fresh
 * blank-node labels on every write, so bnode dictionary ranks legitimately
 * differ between any two writes - including two sequential ones (same caveat
 * as HugeWriterParityTest).
 *
 * <p>{@link #idTupleOrderMatchesNodeComparatorOrder()} separately pins the
 * parallel index's core claim - sorting quads by dictionary-id tuple is
 * exactly the sequential writer's NodeComparator quad order - against real
 * parsed data including randomly-labeled VoID bnodes.
 */
class ParallelWriterParityTest {

    @TempDir
    static Path dir;

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    /** Writes src with both writers; returns the directory holding {name}.seq.h5 / {name}.par.h5. */
    private static Path writeBoth(String name, String content, boolean spatial, boolean features) throws Exception {
        File src = dir.resolve(name).toFile();
        Files.write(src.toPath(), content.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve(name + ".seq.h5").toFile();
        File par = dir.resolve(name + ".par.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq)
                .setSpatial(spatial).setFeatures(features).build().write();
        ParallelHDF5Writer.Builder().setSource(src).setDestination(par)
                .setSpatial(spatial).setFeatures(features)
                // deliberately odd core count: shakes out anything that silently
                // assumes the default or an even split
                .setCores(3)
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
    private static void assertStoresEquivalent(Path seqH5, Path parH5, String... probes) throws Exception {
        try (BeakGraph a = new BeakGraph(new HDF5Reader(seqH5.toFile()));
             BeakGraph b = new BeakGraph(new HDF5Reader(parH5.toFile()))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();

            assertEquals(graphNames(da), graphNames(db), "graph lists must match");

            Model defA = graphModel(da, null);
            Model defB = graphModel(db, null);
            assertTrue(defA.isIsomorphicWith(defB),
                    "default graphs must be isomorphic (seq=" + defA.size() + ", par=" + defB.size() + ")");
            for (String g : graphNames(da)) {
                if (!g.startsWith("http") && !g.startsWith("urn")) continue; // skip any non-URI form
                Model ga = graphModel(da, g);
                Model gb = graphModel(db, g);
                assertTrue(ga.isIsomorphicWith(gb),
                        "graph <" + g + "> must be isomorphic (seq=" + ga.size() + ", par=" + gb.size() + ")");
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
    private static void assertSameStructure(Path seqH5, Path parH5) {
        try (HdfFile seq = new HdfFile(seqH5); HdfFile par = new HdfFile(parH5)) {
            compareGroups("/", (Group) seq.getChild(".BG"), (Group) par.getChild(".BG"));
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
        Path seq = dir.resolve("mixed.trig.seq.h5");
        Path par = dir.resolve("mixed.trig.par.h5");
        assertStoresEquivalent(seq, par,
                "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
                "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
                "SELECT ?s ?o WHERE { GRAPH <http://ex.org/g1> { ?s <http://ex.org/p0> ?o } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o }",
                "SELECT ?g ?s WHERE { GRAPH ?g { ?s <http://ex.org/p0> <http://ex.org/o0> } }");
        assertSameStructure(seq, par);
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
        Path seq = dir.resolve("spatial.trig.seq.h5");
        Path par = dir.resolve("spatial.trig.par.h5");
        assertStoresEquivalent(seq, par,
                "SELECT ?s ?p ?o WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }",
                "SELECT ?g WHERE { GRAPH ?g { <http://ex.org/geo1> ?p ?o } }");
        assertSameStructure(seq, par);
    }

    @Test
    void stressManyQuadsMatchesSequentialWriter() throws Exception {
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
        Path seq = dir.resolve("stress.nq.seq.h5");
        Path par = dir.resolve("stress.nq.par.h5");
        assertStoresEquivalent(seq, par,
                "SELECT ?s ?o WHERE { ?s <http://ex.org/p3> ?o }",
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g",
                "SELECT ?s WHERE { GRAPH <http://ex.org/g7> { ?s <http://ex.org/p1> \"str101\" } }");
        assertSameStructure(seq, par);
    }

    @Test
    void singleCoreStillWorks() throws Exception {
        String ttl = "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
                   + "<http://ex.org/a> <http://ex.org/q> \"x\" .\n";
        File src = dir.resolve("one.ttl").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve("one.ttl.seq.h5").toFile();
        File par = dir.resolve("one.ttl.par.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq).build().write();
        ParallelHDF5Writer.Builder().setSource(src).setDestination(par).setCores(1).build().write();
        assertStoresEquivalent(seq.toPath(), par.toPath(),
                "SELECT ?o WHERE { <http://ex.org/a> ?p ?o }");
        assertSameStructure(seq.toPath(), par.toPath());
    }

    /**
     * The parallel index sorts quads by their dictionary-id tuple instead of
     * comparing nodes. This pins the equivalence directly: for both orderings,
     * mapping the NodeComparator-sorted quad sequence to id tuples yields
     * exactly the id-sorted tuple sequence. Exercised against real parsed
     * state - randomly-labeled VoID bnodes, aligned data bnodes, value-equal
     * literals of different terms, langStrings, a bnode-labeled named graph -
     * so every macro-ordering rule the claim rests on is present.
     */
    @Test
    void idTupleOrderMatchesNodeComparatorOrder() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            ex:s ex:v "1"^^xsd:int .
            ex:s ex:v "1"^^xsd:long .
            ex:s ex:v "1.0"^^xsd:double .
            ex:s ex:v "1"^^xsd:float .
            ex:s ex:v "01"^^xsd:integer .
            ex:s ex:w "a" .
            ex:s ex:w "a"@en .
            ex:s ex:w "a"@de .
            ex:s ex:when "2020-01-02T00:00:00"^^xsd:dateTime .
            ex:s ex:when "2020-01-02T08:00:00+14:00"^^xsd:dateTime .
            _:x ex:p _:y .
            _:y ex:p ex:s .
            <> ex:self <kin.png> .
            _:g { _:x ex:q "in bnode graph" . }
            ex:g { ex:s ex:q _:y . }
            """;
        File src = dir.resolve("order.trig").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));

        try (ParallelPositionalDictionaryWriter w = new ParallelPositionalDictionaryWriterBuilder()
                .setSource(src)
                .setDestination(dir.resolve("order.unused.h5").toFile())
                .setName(Params.DICTIONARY)
                .buildParallel()) {
            Quad[] quads = w.getQuads();
            for (Index type : Index.values()) {
                Quad[] byNodes = quads.clone();
                Arrays.sort(byNodes, type.getComparator());
                long[][] mapped = Arrays.stream(byNodes)
                        .map(q -> tuple(w, type, q))
                        .toArray(long[][]::new);
                long[][] byIds = mapped.clone();
                Arrays.sort(byIds, Arrays::compare);
                assertTrue(Arrays.deepEquals(mapped, byIds),
                        type + ": sorting by dictionary-id tuple must equal the NodeComparator quad order");
            }
        }
    }

    private static long[] tuple(ParallelPositionalDictionaryWriter w, Index type, Quad q) {
        long[] t = new long[4];
        String order = type.name(); // e.g. "GSPO"
        for (int i = 0; i < 4; i++) {
            t[i] = switch (order.charAt(i)) {
                case 'G' -> w.locateGraph(q.getGraph());
                case 'S' -> w.locateSubject(q.getSubject());
                case 'P' -> w.locatePredicate(q.getPredicate());
                case 'O' -> w.locateObject(q.getObject());
                default -> throw new IllegalStateException();
            };
        }
        return t;
    }
}
