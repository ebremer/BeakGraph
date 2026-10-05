package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression test for BG-289: the disk engines (methods 1/4/5) took the
 * entity prefix of the sorted object column up to the first LITERAL.
 * NodeComparator ranks triple terms after literals, so a store whose objects
 * are IRIs/bnodes followed directly by triple terms - no top-level literal
 * object at all - streamed every triple term into the entities dictionary,
 * whose writer has no triple-term support and threw. Valid RDF 1.2 that the
 * in-memory engines built failed on the disk engines. The prefix now ends at
 * the first literal or triple term, whichever comes first.
 */
class TripleTermOnlyObjectsTest {

    private static final String PRE = "PREFIX : <http://ex.org/> ";
    private static final String TTL = """
        @prefix : <http://ex.org/> .
        :r :says <<( :a :b :c )>> .
        :r :nested <<( :a :b <<( :x :y :z )>> )>> .
        _:o :obs <<( _:o :p :q )>> .
        :r :plain :q .
        :r :blank _:o .
        """;

    @TempDir
    static Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    private static long count(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            long n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void literalFreeTripleTermStoreBuildsOnEveryEngine(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        File src = dir.resolve(engine.name() + ".ttl").toFile();
        File h5 = dir.resolve(engine.name() + ".h5").toFile();
        Files.writeString(src.toPath(), TTL, StandardCharsets.UTF_8);
        engine.buildStore(src, h5);

        File ramSrc = dir.resolve("ram-ref.ttl").toFile();
        File ramH5 = dir.resolve("ram-ref-" + engine.name() + ".h5").toFile();
        Files.writeString(ramSrc.toPath(), TTL, StandardCharsets.UTF_8);
        WriterEngines.all().filter(e -> !e.needsNative()).findFirst().orElseThrow().buildStore(ramSrc, ramH5);

        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5)); BeakGraph ref = new BeakGraph(new HDF5Reader(ramH5))) {
            Dataset ds = bg.getDataset();
            assertEquals(5, count(ds, "SELECT * WHERE { ?s ?p ?o }"), "every quad is stored");
            assertEquals(1, count(ds, PRE + "SELECT * WHERE { :r :says <<( :a :b :c )>> }"), "triple term stored term-exactly");
            assertEquals(1, count(ds, PRE + "SELECT * WHERE { :r :nested <<( :a :b <<( :x :y :z )>> )>> }"), "nested triple term");
            assertEquals(1, count(ds, PRE + "SELECT ?b WHERE { ?b :obs <<( ?b :p :q )>> }"), "bnode co-reference into the term");
            // The triple terms are literals-section terms, not entities: the
            // entity dictionary is exactly the in-memory engine's.
            assertEquals(ref.getReader().getDictionary().getSubjects().getNumberOfNodes(),
                    bg.getReader().getDictionary().getSubjects().getNumberOfNodes(), "entity count must match the RAM engine");
            assertEquals(ref.getReader().getDictionary().getObjects().getNumberOfNodes(),
                    bg.getReader().getDictionary().getObjects().getNumberOfNodes(), "object count must match the RAM engine");
            Model a = ds.getDefaultModel();
            Model b = ref.getDataset().getDefaultModel();
            assertTrue(a.isIsomorphicWith(b), "engines must agree");
        }
    }
}
