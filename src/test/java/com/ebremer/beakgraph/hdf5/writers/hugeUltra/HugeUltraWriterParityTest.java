package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * End-to-end parity of the hugeUltra writer (-method 4) against the
 * sequential in-memory writer, read back through the unmodified readers. The
 * bar is the huge writer's: same graph lists, isomorphic per-graph content,
 * same query answers (bnode labels legitimately differ - the disk pipeline
 * keeps parsed labels). The interesting part: builds run with TINY spill
 * batches and fan-in 2, so every new mechanism actually executes - background
 * double-buffered spills, multi-level CONCURRENT merges, the grouped term-run
 * format across merge levels, packed radix-sorted quad/row-id runs, and the
 * GSPO-to-GPOS dedup tee (the input contains duplicate quads on purpose).
 */
class HugeUltraWriterParityTest {

    @TempDir
    static Path dir;

    @BeforeAll
    static void requireNativeHdf5() {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
    }

    private static Path writeBoth(String name, String content) throws Exception {
        return writeBoth(name, content, false, false);
    }

    private static Path writeBoth(String name, String content, boolean spatial, boolean features) throws Exception {
        File src = dir.resolve(name).toFile();
        Files.write(src.toPath(), content.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve(name + ".seq.h5").toFile();
        File hu = dir.resolve(name + ".hugeultra.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq).setSpatial(spatial).setFeatures(features).build().write();
        HugeUltraHDF5Writer.Builder()
                .setSpatial(spatial).setFeatures(features)
                .setSource(src)
                .setDestination(hu)
                .setWorkDirectory(Files.createDirectories(dir.resolve("work-" + name)))
                .setCores(3)
                // Deliberately absurd sizing: hundreds of runs, several merge
                // levels, constant background spilling.
                .setTermSpillBatch(64)
                .setIdSpillBatch(128)
                .setMergeFanIn(2)
                .build()
                .write();
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

    private static void assertStoresEquivalent(Path seqH5, Path huH5, String... probes) throws Exception {
        try (BeakGraph a = new BeakGraph(new HDF5Reader(seqH5.toFile()));
             BeakGraph b = new BeakGraph(new HDF5Reader(huH5.toFile()))) {
            org.apache.jena.query.Dataset da = a.getDataset();
            org.apache.jena.query.Dataset db = b.getDataset();
            assertEquals(graphNames(da), graphNames(db), "graph lists must match");
            Model defA = graphModel(da, null);
            Model defB = graphModel(db, null);
            assertTrue(defA.isIsomorphicWith(defB),
                    "default graphs must be isomorphic (seq=" + defA.size() + ", hugeUltra=" + defB.size() + ")");
            for (String g : graphNames(da)) {
                if (!g.startsWith("http") && !g.startsWith("urn")) continue;
                Model ga = graphModel(da, g);
                Model gb = graphModel(db, g);
                assertTrue(ga.isIsomorphicWith(gb),
                        "graph <" + g + "> must be isomorphic (seq=" + ga.size() + ", hugeUltra=" + gb.size() + ")");
            }
            for (String probe : probes) {
                assertEquals(select(da, probe), select(db, probe), "probe results must match: " + probe);
            }
        }
    }

    @Test
    void mixedTypesNamedGraphsAndBnodes() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            ex:s0 ex:p0 ex:o0 .
            ex:s0 ex:p0 ex:o0 .
            ex:s0 ex:name "Alice" .
            ex:s0 ex:name "Alice"@en .
            ex:s0 ex:count "42"^^xsd:int .
            ex:s0 ex:big "9223372036854775806"^^xsd:long .
            ex:s0 ex:f "1.5"^^xsd:float .
            ex:s0 ex:d "-2.25E8"^^xsd:double .
            ex:s0 ex:unbounded "123456789012345678901234567890"^^xsd:integer .
            ex:s0 ex:flag true .
            ex:s0 ex:when "2024-05-06T07:08:09Z"^^xsd:dateTime .
            ex:s0 ex:ill "abc"^^xsd:int .
            ex:s0 ex:dl "hello"@en--ltr .
            ex:s0 ex:dl "hi"@en--rtl .
            ex:s0 ex:tt <<( ex:a ex:b ex:c )>> .
            ex:s0 ex:tt2 <<( ex:a ex:b <<( ex:x ex:y "nested" )>> )>> .
            ex:s0 ex:list "[1, 2]"^^<http://w3id.org/awslabs/neptune/SPARQL-CDTs/List> .
            ex:s0 ex:map "{\\"k\\": 1}"^^<http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map> .
            ex:s0 ex:count "042"^^xsd:int .
            ex:s0 ex:d2 "1.0E1"^^xsd:double .
            ex:s0 ex:d2 "10.0"^^xsd:double .
            <> ex:self <sibling.png> ; ex:up <../up.png> ; ex:up2 <../../up2.png> ; ex:root </root.png> ; ex:frag <#frag> ; ex:query <?q=1> .
            _:b1 ex:p0 _:b2 .
            _:b2 ex:knows ex:s0 .
            ex:g1 {
                ex:s1 ex:p0 ex:o0 .
                _:b1 ex:inGraph ex:g1 .
            }
            ex:g2 { ex:s0 ex:p0 ex:s1 . }
            """;
        writeBoth("hu-mixed.trig", trig);
        assertStoresEquivalent(dir.resolve("hu-mixed.trig.seq.h5"), dir.resolve("hu-mixed.trig.hugeultra.h5"),
                "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
                "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
                "SELECT ?g ?s WHERE { GRAPH ?g { ?s <http://ex.org/p0> <http://ex.org/o0> } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/dl> ?o }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/tt> ?o }",
                "SELECT ?x WHERE { <http://ex.org/s0> <http://ex.org/tt2> <<( <http://ex.org/a> <http://ex.org/b> <<( <http://ex.org/x> <http://ex.org/y> ?x )>> )>> }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/list> ?o }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o }",
                "SELECT (COUNT(?o) AS ?n) WHERE { <http://ex.org/s0> <http://ex.org/d2> ?o }");
    }

    /** BG-182: the SPATIAL graph and the derived features must match the sequential build. */
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
        writeBoth("hu-spatial.trig", trig, true, true);
        assertStoresEquivalent(dir.resolve("hu-spatial.trig.seq.h5"), dir.resolve("hu-spatial.trig.hugeultra.h5"),
                "SELECT ?s ?p ?o WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }",
                "SELECT ?g WHERE { GRAPH ?g { <http://ex.org/geo1> ?p ?o } }",
                "SELECT ?p ?o WHERE { <http://ex.org/geo1> ?p ?o }");
    }

    @Test
    void stressManyQuadsWithDuplicatesThroughTinySpillRuns() throws Exception {
        StringBuilder nq = new StringBuilder(1 << 21);
        for (int i = 0; i < 12_000; i++) {
            String s = "<http://ex.org/s" + (i % 400) + ">";
            String p = "<http://ex.org/p" + (i % 5) + ">";
            String o = switch (i % 4) {
                case 0 -> "<http://ex.org/o" + (i % 250) + ">";
                case 1 -> "\"str" + (i % 300) + "\"";
                case 2 -> "\"" + (i % 500 - 250) + "\"^^<http://www.w3.org/2001/XMLSchema#int>";
                default -> "\"" + ((i % 90) / 4.0) + "\"^^<http://www.w3.org/2001/XMLSchema#double>";
            };
            nq.append(s).append(' ').append(p).append(' ').append(o);
            if (i % 3 != 0) {
                nq.append(" <http://ex.org/g").append(i % 12).append('>');
            }
            nq.append(" .\n");
            // Every 10th quad appears twice: the GSPO->GPOS dedup tee must
            // collapse these without dropping anything else.
            if (i % 10 == 0) {
                nq.append(s).append(' ').append(p).append(' ').append(o).append(" .\n");
            }
        }
        writeBoth("hu-stress.nq", nq.toString());
        assertStoresEquivalent(dir.resolve("hu-stress.nq.seq.h5"), dir.resolve("hu-stress.nq.hugeultra.h5"),
                "SELECT ?s ?o WHERE { ?s <http://ex.org/p3> ?o }",
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g",
                "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
                "SELECT ?s WHERE { GRAPH <http://ex.org/g7> { ?s <http://ex.org/p1> \"str101\" } }");
    }

    @Test
    void mergeKeepsBlankNodesDistinctPerDocument() throws Exception {
        Path src = Files.createDirectories(dir.resolve("hu-mergesrc"));
        Files.write(src.resolve("m1.ttl"),
                "_:b0 <http://ex.org/bp> \"v1\" .\n<http://ex.org/s> <http://ex.org/p> <http://ex.org/o1> .\n"
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(src.resolve("m2.ttl"),
                "_:b0 <http://ex.org/bp> \"v2\" .\n<http://ex.org/s> <http://ex.org/p> <http://ex.org/o2> .\n"
                        .getBytes(StandardCharsets.UTF_8));
        File out = dir.resolve("hu-merged.h5").toFile();
        HugeUltraHDF5Writer.Builder()
                .setSources(java.util.List.of(src.resolve("m1.ttl").toFile(), src.resolve("m2.ttl").toFile()))
                .setDestination(out)
                .setWorkDirectory(Files.createDirectories(dir.resolve("hu-mergework")))
                .setCores(2)
                .setTermSpillBatch(8)
                .setIdSpillBatch(8)
                .setMergeFanIn(2)
                .build()
                .write();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(out))) {
            try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                    .query(QueryFactory.create(
                            "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?v }")).build()) {
                assertEquals(2, qe.execSelect().next().getLiteral("n").getInt(),
                        "_:b0 from two documents must remain two distinct blank nodes");
            }
        }
    }
}
