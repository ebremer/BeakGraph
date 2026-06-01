package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end check that multi-pattern BGPs - which now flow through the join-reorder transform in
 * {@code OpExecutorBG} - return correct results, and that the stats-backed transform (not the fixed
 * fallback) is selected for a file carrying VoID metadata. The data is skewed (many {@code name}
 * triples, few {@code knows} triples) so the reorder actually reorders; results must be unaffected.
 */
class ReorderQueryTest {

    private static final String NS = "http://ex.org/";
    @TempDir static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void build() throws Exception {
        Model m = ModelFactory.createDefaultModel();
        Property knows = m.createProperty(NS + "knows");
        Property name = m.createProperty(NS + "name");
        String[][] edges = {{"alice", "bob"}, {"carol", "dave"}, {"erin", "frank"}};
        for (String[] e : edges) {
            m.createResource(NS + e[0]).addProperty(knows, m.createResource(NS + e[1]));
        }
        for (String who : new String[]{"alice", "bob", "carol", "dave", "erin", "frank"}) {
            m.createResource(NS + who).addProperty(name, Character.toUpperCase(who.charAt(0)) + who.substring(1));
        }
        // Filler names make `name` far more common than `knows`, so the reorder must reorder.
        for (int i = 0; i < 100; i++) {
            m.createResource(NS + "x" + i).addProperty(name, "filler" + i);
        }
        File ttl = dir.resolve("k.ttl").toFile();
        File h5 = dir.resolve("k.ttl.h5").toFile();
        try (OutputStream out = Files.newOutputStream(ttl.toPath())) {
            RDFDataMgr.write(out, m, Lang.TURTLE);
        }
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5), h5.toURI());
        ds = bg.getDataset();
    }

    @AfterAll
    static void close() throws Exception {
        if (ds != null) ds.close();
        if (bg != null) bg.close();
    }

    private static Set<String> select(String q, String var) {
        Set<String> out = new TreeSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                out.add(rs.next().getLiteral(var).getLexicalForm());
            }
        }
        return out;
    }

    @Test
    void statsBackedReorderIsSelected() {
        ReorderTransformation rt = bg.getReorderTransform();
        assertEquals("BGReorderTransform", rt.getClass().getSimpleName(),
            "a file with VoID metadata should use the stats-backed reorder, not the fixed fallback");
    }

    @Test
    void twoPatternJoinReturnsFriendNames() {
        String q = "PREFIX ex: <" + NS + "> "
                 + "SELECT ?friendName WHERE { ?p ex:knows ?f . ?f ex:name ?friendName }";
        assertEquals(new TreeSet<>(Set.of("Bob", "Dave", "Frank")), select(q, "friendName"));
    }

    @Test
    void threePatternChainReturnsCorrectResults() {
        String q = "PREFIX ex: <" + NS + "> "
                 + "SELECT ?fn WHERE { ?p ex:name ?pn . ?p ex:knows ?f . ?f ex:name ?fn }";
        assertEquals(new TreeSet<>(Set.of("Bob", "Dave", "Frank")), select(q, "fn"));
    }
}
