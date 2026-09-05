package com.ebremer.beakgraph.hdf5.writers.parallel;

import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.jena.sparql.core.Quad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end parity: the parallel writer must produce a store that reads back
 * (through the UNMODIFIED jHDF-based readers) semantically identical to the
 * sequential writer's - same graphs, isomorphic per-graph content, same query
 * answers through both indexes, structurally identical HDF5 metadata (same
 * dataset tree, same sizes, same numEntries / width / FCD attributes
 * everywhere) and, for single-source builds, BYTE-identical files: methods
 * 0/2/3 are one format written through one jHDF sequence (SPECIFICATIONS.md).
 * VoID is off by default (VoidMode.NONE), so no random blank-node labels
 * enter these builds; the one EXACT-mode case asserts structural parity only,
 * because the VoID generator mints fresh labels on every write.
 *
 * <p>{@link #idTupleOrderMatchesNodeComparatorOrder()} separately pins the
 * parallel index's core claim - sorting quads by dictionary-id tuple is
 * exactly the sequential writer's NodeComparator quad order - against real
 * parsed data including randomly-labeled VoID bnodes.
 */
public class ParallelWriterParityTest {

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


    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    @Test
    void mixedTypesAndNamedGraphs() throws Exception {
        String trig = StoreParity.mixedTrig();
        writeBoth("mixed.trig", trig, false, false);
        Path seq = dir.resolve("mixed.trig.seq.h5");
        Path par = dir.resolve("mixed.trig.par.h5");
        StoreParity.assertStoresEquivalent(seq, par,
                "SELECT ?p ?o WHERE { <http://ex.org/s0> ?p ?o }",
                "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }",
                "SELECT ?s ?o WHERE { GRAPH <http://ex.org/g1> { ?s <http://ex.org/p0> ?o } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/count> ?o }",
                "SELECT ?g ?s WHERE { GRAPH ?g { ?s <http://ex.org/p0> <http://ex.org/o0> } }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/dl> ?o }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/tt> ?o }",
                "SELECT ?x WHERE { <http://ex.org/s0> <http://ex.org/tt2> <<( <http://ex.org/a> <http://ex.org/b> <<( <http://ex.org/x> <http://ex.org/y> ?x )>> )>> }",
                "SELECT ?o WHERE { <http://ex.org/s0> <http://ex.org/list> ?o }");
        StoreParity.assertSameStructure(seq, par);
        StoreParity.assertSameBytes(seq, par);
    }

    @Test
    void spatialAndFeaturesParity() throws Exception {
        String trig = StoreParity.SPATIAL_TRIG;
        writeBoth("spatial.trig", trig, true, true);
        Path seq = dir.resolve("spatial.trig.seq.h5");
        Path par = dir.resolve("spatial.trig.par.h5");
        StoreParity.assertStoresEquivalent(seq, par,
                "SELECT ?s ?p ?o WHERE { GRAPH <urn:x-beakgraph:Spatial> { ?s ?p ?o } }",
                "SELECT ?g WHERE { GRAPH ?g { <http://ex.org/geo1> ?p ?o } }");
        StoreParity.assertSameStructure(seq, par);
        StoreParity.assertSameBytes(seq, par);
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
        StoreParity.assertStoresEquivalent(seq, par,
                "SELECT ?s ?o WHERE { ?s <http://ex.org/p3> ?o }",
                "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g",
                "SELECT ?s WHERE { GRAPH <http://ex.org/g7> { ?s <http://ex.org/p1> \"str101\" } }");
        StoreParity.assertSameStructure(seq, par);
        StoreParity.assertSameBytes(seq, par);
    }

    /** BG-112 fixture: several graphs, rdf:type triples, duplicate quads, typed and tagged literals. */
    public static final String VOID_FIXTURE = "@prefix ex: <http://ex.org/> .\n"
            + "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
            + "ex:a rdf:type ex:T1 . ex:a ex:p ex:b . ex:a ex:p \"x\" . ex:a ex:p ex:b .\n"
            + "ex:b rdf:type ex:T2 . ex:b ex:q \"1\"^^<http://www.w3.org/2001/XMLSchema#int> . ex:b ex:q \"2\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
            + "ex:g1 { ex:c rdf:type ex:T1 . ex:c ex:p ex:d . ex:c ex:p ex:d . ex:d ex:q \"y\"@en . ex:d ex:r _:n . }\n"
            + "ex:g2 { ex:e ex:p ex:f . ex:e rdf:type ex:T3 . }\n";

    @Test
    void voidStatisticsParityInBothModes() throws Exception {
        File src = dir.resolve("voidparity.trig").toFile();
        Files.write(src.toPath(), VOID_FIXTURE.getBytes(StandardCharsets.UTF_8));
        for (com.ebremer.beakgraph.core.VoidMode mode : new com.ebremer.beakgraph.core.VoidMode[]{
                com.ebremer.beakgraph.core.VoidMode.EXACT, com.ebremer.beakgraph.core.VoidMode.SKETCH}) {
            File seq = dir.resolve("voidparity-" + mode + ".seq.h5").toFile();
            File par = dir.resolve("voidparity-" + mode + ".par.h5").toFile();
            HDF5Writer.Builder().setSource(src).setDestination(seq).setVoidMode(mode).build().write();
            ParallelHDF5Writer.Builder().setSource(src).setDestination(par).setVoidMode(mode).setCores(3).build().write();
            StoreParity.assertStoresEquivalent(seq.toPath(), par.toPath(),
                    "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY ?g");
            StoreParity.assertVoidParity(seq.toPath(), par.toPath(), "parallel " + mode);
        }
    }

    /** BG-112: the parallel engine's -merge path had no parity test at all. */
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
        java.util.List<File> inputs = java.util.List.of(src.resolve("m1.ttl").toFile(), src.resolve("m2.nq").toFile(),
                src.resolve("m3.ttl").toFile());
        for (com.ebremer.beakgraph.core.VoidMode mode : new com.ebremer.beakgraph.core.VoidMode[]{
                com.ebremer.beakgraph.core.VoidMode.NONE, com.ebremer.beakgraph.core.VoidMode.EXACT}) {
            File seq = dir.resolve("merge-" + mode + ".seq.h5").toFile();
            File par = dir.resolve("merge-" + mode + ".par.h5").toFile();
            HDF5Writer.Builder().setSources(inputs).setDestination(seq).setVoidMode(mode).build().write();
            ParallelHDF5Writer.Builder().setSources(inputs).setDestination(par).setVoidMode(mode).setCores(3).build().write();
            StoreParity.assertStoresEquivalent(seq.toPath(), par.toPath(),
                    "SELECT ?o WHERE { <http://ex.org/s> <http://ex.org/p> ?o }",
                    "SELECT ?o WHERE { GRAPH <http://ex.org/gm> { <http://ex.org/s> ?p ?o } }",
                    "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/bp> ?v }");
            if (mode != com.ebremer.beakgraph.core.VoidMode.NONE) {
                StoreParity.assertVoidParity(seq.toPath(), par.toPath(), "parallel merge " + mode);
            }
        }
    }

    @Test
    void exactVoidModeKeepsStructuralParityOnly() throws Exception {
        // The VoID generator mints fresh blank-node labels per write, so with
        // EXACT statistics the bytes legitimately differ; structure and content
        // must still agree.
        String ttl = "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n"
                   + "<http://ex.org/a> <http://ex.org/q> \"x\" .\n"
                   + "<http://ex.org/g> { <http://ex.org/a> <http://ex.org/p> <http://ex.org/c> . }\n";
        File src = dir.resolve("void.trig").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        File seq = dir.resolve("void.trig.seq.h5").toFile();
        File par = dir.resolve("void.trig.par.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(seq).setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).build().write();
        ParallelHDF5Writer.Builder().setSource(src).setDestination(par).setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setCores(2).build().write();
        StoreParity.assertStoresEquivalent(seq.toPath(), par.toPath(),
                "SELECT ?o WHERE { <http://ex.org/a> ?p ?o }",
                "SELECT (COUNT(*) AS ?n) WHERE { GRAPH <" + Params.VOIDSTRING + "> { ?s ?p ?o } }");
        StoreParity.assertSameStructure(seq.toPath(), par.toPath());
    }

    /**
     * BG-429: JSON-LD (inline context, @list container, @graph, a blank node)
     * and RDF/XML (parseType Collection, rdf:nodeID) go through the same
     * parser configuration as Turtle, so their stores are byte-identical too.
     */
    @Test
    void jsonLdAndRdfXmlSourcesAreByteIdentical() throws Exception {
        writeBoth("mixed.jsonld", JSONLD_FIXTURE, false, false);
        writeBoth("mixed.rdf", RDFXML_FIXTURE, false, false);
        for (String name : new String[]{"mixed.jsonld", "mixed.rdf"}) {
            Path seq = dir.resolve(name + ".seq.h5");
            Path par = dir.resolve(name + ".par.h5");
            StoreParity.assertStoresEquivalent(seq, par,
                    "SELECT ?o WHERE { <http://ex.org/doc> ?p ?o }",
                    "SELECT ?i WHERE { <http://ex.org/doc> <http://ex.org/items> ?l . ?l <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?i }",
                    "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?b <http://ex.org/tag> ?t }");
            StoreParity.assertSameStructure(seq, par);
            StoreParity.assertSameBytes(seq, par);
        }
        try (BeakGraph a = new BeakGraph(new HDF5Reader(dir.resolve("mixed.jsonld.seq.h5").toFile()))) {
            assertEquals(2, StoreParity.select(a.getDataset(), "SELECT ?i WHERE { <http://ex.org/doc> <http://ex.org/items> ?l . ?l <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>*/<http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ?i }").size(), "the @list container became an RDF list");
            assertEquals(1, StoreParity.select(a.getDataset(), "SELECT ?o WHERE { GRAPH <http://ex.org/gjson> { <http://ex.org/s> <http://ex.org/p> ?o } }").size(), "the @graph block became a named graph");
        }
    }

    public static final String JSONLD_FIXTURE = """
        {
          "@context": {"ex": "http://ex.org/", "items": {"@id": "ex:items", "@container": "@list"}},
          "@graph": [
            {"@id": "ex:doc", "items": [{"@id": "ex:i1"}, {"@id": "ex:i2"}],
             "ex:anon": {"ex:tag": "from jsonld"}, "ex:n": {"@value": "7", "@type": "http://www.w3.org/2001/XMLSchema#int"}},
            {"@id": "ex:gjson", "@graph": [{"@id": "ex:s", "ex:p": {"@id": "ex:o4"}}]}
          ]
        }
        """;

    public static final String RDFXML_FIXTURE = """
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://ex.org/">
          <rdf:Description rdf:about="http://ex.org/doc">
            <ex:items rdf:parseType="Collection">
              <rdf:Description rdf:about="http://ex.org/i1"/>
              <rdf:Description rdf:about="http://ex.org/i2"/>
            </ex:items>
            <ex:anon rdf:nodeID="n1"/>
            <ex:n rdf:datatype="http://www.w3.org/2001/XMLSchema#int">007</ex:n>
          </rdf:Description>
          <rdf:Description rdf:nodeID="n1"><ex:tag>from rdfxml</ex:tag></rdf:Description>
        </rdf:RDF>
        """;

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
        StoreParity.assertStoresEquivalent(seq.toPath(), par.toPath(),
                "SELECT ?o WHERE { <http://ex.org/a> ?p ?o }");
        StoreParity.assertSameStructure(seq.toPath(), par.toPath());
        StoreParity.assertSameBytes(seq.toPath(), par.toPath());
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
