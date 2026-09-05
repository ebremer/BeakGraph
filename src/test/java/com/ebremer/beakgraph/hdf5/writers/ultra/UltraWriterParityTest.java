package com.ebremer.beakgraph.hdf5.writers.ultra;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end parity of the ultra writer against the sequential writer, read
 * back through the UNMODIFIED jHDF-based readers: same graph lists, isomorphic
 * per-graph content, same query answers through both indexes, structurally
 * identical HDF5 metadata and - for single-source builds, where the ultra
 * ingest reuses the sequential blank node labels verbatim - BYTE-identical
 * files (methods 0/2/3 are one format written through one jHDF sequence,
 * SPECIFICATIONS.md). VoID is off by default, so no random labels enter these
 * builds. Multi-source merges use per-document blank-node labels, so the
 * merge test asserts semantic equivalence only.
 */
class UltraWriterParityTest {

    @TempDir
    static Path dir;

    // ------------------------------------------------------------------
    // helpers (the established parity bar of ParallelWriterParityTest)
    // ------------------------------------------------------------------

    private static Path writeBoth(String name, String content, boolean spatial, boolean features) throws Exception {
        File src = dir.resolve(name).toFile();
        Files.write(src.toPath(), content.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve(name + ".seq.h5").toFile();
        File ult = dir.resolve(name + ".ultra.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq)
                .setSpatial(spatial).setFeatures(features).build().write();
        UltraHDF5Writer.Builder().setSource(src).setDestination(ult)
                .setSpatial(spatial).setFeatures(features)
                // deliberately odd core count: shakes out anything that silently
                // assumes the default or an even chunk split
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

    private static void assertStoresEquivalent(Path seqH5, Path ultraH5, String... probes) throws Exception {
        try (BeakGraph a = new BeakGraph(new HDF5Reader(seqH5.toFile()));
             BeakGraph b = new BeakGraph(new HDF5Reader(ultraH5.toFile()))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();

            assertEquals(graphNames(da), graphNames(db), "graph lists must match");

            Model defA = graphModel(da, null);
            Model defB = graphModel(db, null);
            assertTrue(defA.isIsomorphicWith(defB),
                    "default graphs must be isomorphic (seq=" + defA.size() + ", ultra=" + defB.size() + ")");
            for (String g : graphNames(da)) {
                if (!g.startsWith("http") && !g.startsWith("urn")) continue;
                Model ga = graphModel(da, g);
                Model gb = graphModel(db, g);
                assertTrue(ga.isIsomorphicWith(gb),
                        "graph <" + g + "> must be isomorphic (seq=" + ga.size() + ", ultra=" + gb.size() + ")");
            }
            for (String probe : probes) {
                assertEquals(select(da, probe), select(db, probe), "probe results must match: " + probe);
            }
        }
    }

    private static void assertSameStructure(Path seqH5, Path ultraH5) {
        try (HdfFile seq = new HdfFile(seqH5); HdfFile ult = new HdfFile(ultraH5)) {
            compareGroups("/", (Group) seq.getChild(".BG"), (Group) ult.getChild(".BG"));
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


    /**
     * Methods 0/2/3 are one format written through one jHDF sequence
     * (SPECIFICATIONS.md: "byte-identical stores"), so for a single source
     * the files must match byte for byte - not merely in structure. This is
     * what catches a different bit layout or FCD block content that the
     * readers still decode consistently.
     */
    private static void assertSameBytes(Path a, Path b) throws Exception {
        assertArrayEquals(Files.readAllBytes(a), Files.readAllBytes(b),
                "single-source stores must be byte-identical: " + a.getFileName() + " vs " + b.getFileName());
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
        Path seq = dir.resolve("mixed.trig.seq.h5");
        Path ult = dir.resolve("mixed.trig.ultra.h5");
        assertStoresEquivalent(seq, ult,
                "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
                "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
                "SELECT ?s ?o WHERE { GRAPH <http://ex.org/g1> { ?s <http://ex.org/p0> ?o } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o }",
                "SELECT ?g ?s WHERE { GRAPH ?g { ?s <http://ex.org/p0> <http://ex.org/o0> } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/dl> ?o }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/tt> ?o }",
                "SELECT ?x WHERE { <http://ex.org/s0> <http://ex.org/tt2> <<( <http://ex.org/a> <http://ex.org/b> <<( <http://ex.org/x> <http://ex.org/y> ?x )>> )>> }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/list> ?o }");
        assertSameStructure(seq, ult);
        assertSameBytes(seq, ult);
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
        Path ult = dir.resolve("spatial.trig.ultra.h5");
        assertStoresEquivalent(seq, ult,
                "SELECT ?s ?p ?o WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }",
                "SELECT ?g WHERE { GRAPH ?g { <http://ex.org/geo1> ?p ?o } }");
        assertSameStructure(seq, ult);
        assertSameBytes(seq, ult);
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
        Path ult = dir.resolve("stress.nq.ultra.h5");
        assertStoresEquivalent(seq, ult,
                "SELECT ?s ?o WHERE { ?s <http://ex.org/p3> ?o }",
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g",
                "SELECT ?s WHERE { GRAPH <http://ex.org/g7> { ?s <http://ex.org/p1> \"str101\" } }");
        assertSameStructure(seq, ult);
        assertSameBytes(seq, ult);
    }

    /** BG-429: JSON-LD and RDF/XML sources are byte-identical across the engines too. */
    @Test
    void jsonLdAndRdfXmlSourcesAreByteIdentical() throws Exception {
        writeBoth("mixed.jsonld", com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.JSONLD_FIXTURE, false, false);
        writeBoth("mixed.rdf", com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.RDFXML_FIXTURE, false, false);
        for (String name : new String[]{"mixed.jsonld", "mixed.rdf"}) {
            Path seq = dir.resolve(name + ".seq.h5");
            Path ult = dir.resolve(name + ".ultra.h5");
            assertStoresEquivalent(seq, ult,
                    "SELECT ?o WHERE { <http://ex.org/doc> ?p ?o }",
                    "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/tag> ?t }");
            assertSameStructure(seq, ult);
            assertSameBytes(seq, ult);
        }
    }

    @Test
    void singleCoreStillWorks() throws Exception {
        String ttl = "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
                   + "<http://ex.org/a> <http://ex.org/q> \"x\" .\n";
        File src = dir.resolve("one.ttl").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve("one.ttl.seq.h5").toFile();
        File ult = dir.resolve("one.ttl.ultra.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq).build().write();
        UltraHDF5Writer.Builder().setSource(src).setDestination(ult).setCores(1).build().write();
        assertStoresEquivalent(seq.toPath(), ult.toPath(),
                "SELECT ?o WHERE { <http://ex.org/a> ?p ?o }");
        assertSameStructure(seq.toPath(), ult.toPath());
        assertSameBytes(seq.toPath(), ult.toPath());
    }

    /**
     * Merge parity: ultra's PARALLEL multi-document ingest against the
     * sequential writer's one-after-another merge of the same tree. Blank node
     * labels differ by design (per-document scoping vs a global counter), so
     * the bar is semantic: same graphs, isomorphic content, same answers -
     * including two documents that both say {@code _:b0} and must stay two
     * distinct nodes.
     */
    /** BG-112: the ultra engine's own VoID path (concurrent per-document accumulation) matches the sequential writer's. */
    @Test
    void voidStatisticsParityInBothModes() throws Exception {
        File src = dir.resolve("voidparity.trig").toFile();
        Files.write(src.toPath(), com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.VOID_FIXTURE.getBytes(StandardCharsets.UTF_8));
        for (com.ebremer.beakgraph.core.VoidMode mode : new com.ebremer.beakgraph.core.VoidMode[]{
                com.ebremer.beakgraph.core.VoidMode.EXACT, com.ebremer.beakgraph.core.VoidMode.SKETCH}) {
            File seq = dir.resolve("voidparity-" + mode + ".seq.h5").toFile();
            File ult = dir.resolve("voidparity-" + mode + ".ultra.h5").toFile();
            HDF5Writer.Builder().setSource(src).setDestination(seq).setVoidMode(mode).build().write();
            UltraHDF5Writer.Builder().setSource(src).setDestination(ult).setVoidMode(mode).setCores(3).build().write();
            assertStoresEquivalent(seq.toPath(), ult.toPath(),
                    "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g");
            com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.assertVoidParity(seq.toPath(), ult.toPath(), "ultra " + mode);
        }
    }

    @Test
    void mergeParityWithSequentialMerge() throws Exception {
        Path src = Files.createDirectories(dir.resolve("mergesrc"));
        Files.write(src.resolve("m1.ttl"),
                "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o1> .\n_:b0 <http://ex.org/bp> \"v1\" .\n"
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(src.resolve("m2.nq"),
                "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o2> <http://ex.org/gm> .\n"
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(src.resolve("m3.ttl"),
                "_:b0 <http://ex.org/bp> \"v2\" .\n<http://ex.org/s> <http://ex.org/p> <http://ex.org/o3> .\n"
                        .getBytes(StandardCharsets.UTF_8));
        List<File> inputs = List.of(src.resolve("m1.ttl").toFile(), src.resolve("m2.nq").toFile(),
                src.resolve("m3.ttl").toFile());
        File seq = dir.resolve("merge.seq.h5").toFile();
        File ult = dir.resolve("merge.ultra.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(seq).build().write();
        UltraHDF5Writer.Builder().setSources(inputs).setDestination(ult).setCores(3).build().write();
        assertStoresEquivalent(seq.toPath(), ult.toPath(),
                "SELECT ?o WHERE { <http://ex.org/s> <http://ex.org/p> ?o }",
                "SELECT ?o WHERE { GRAPH <http://ex.org/gm> { <http://ex.org/s> ?p ?o } }",
                "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?v }");
        // BG-112: the merge path with statistics too.
        File seqV = dir.resolve("merge.void.seq.h5").toFile();
        File ultV = dir.resolve("merge.void.ultra.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(seqV).setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).build().write();
        UltraHDF5Writer.Builder().setSources(inputs).setDestination(ultV).setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setCores(3).build().write();
        assertStoresEquivalent(seqV.toPath(), ultV.toPath(),
                "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?v }");
        com.ebremer.beakgraph.hdf5.writers.parallel.ParallelWriterParityTest.assertVoidParity(seqV.toPath(), ultV.toPath(), "ultra merge EXACT");
    }
}
