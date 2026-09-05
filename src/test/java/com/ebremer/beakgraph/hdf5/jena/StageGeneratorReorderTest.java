package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.QueryEngineBG;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-445: the ARQ-global {@link StageGeneratorDirectorBG} is the entry point
 * for every BGP over a BeakGraph that the BG executor does not see - here a
 * general Jena dataset holding a BeakGraph view as a named graph, whose
 * default graph is plain memory so {@link QueryEngineBG} declines it. It used
 * to hand the pattern to the solver in written order; it now plans like
 * {@code OpExecutorBG}: the VoID-statistics reorder puts the selective triple
 * first.
 */
class StageGeneratorReorderTest {

    private static final String NS = "http://ex.org/";
    private static final Node A = NodeFactory.createURI("urn:test:a");

    @TempDir
    static Path dir;
    static BeakGraph bg;

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
        for (int i = 0; i < 100; i++) {
            m.createResource(NS + "x" + i).addProperty(name, "filler" + i); // name >> knows
        }
        File ttl = dir.resolve("k.ttl").toFile();
        File h5 = dir.resolve("k.ttl.h5").toFile();
        try (OutputStream out = Files.newOutputStream(ttl.toPath())) {
            RDFDataMgr.write(out, m, Lang.TURTLE);
        }
        HDF5Writer.Builder().setVoidMode(VoidMode.EXACT).setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5), h5.toURI());
        assertEquals("BGReorderTransform", bg.getReorderTransform().getClass().getSimpleName());
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    @Test
    void theGlobalStageGeneratorReordersLikeTheExecutor() {
        DatasetGraph dsg = DatasetGraphFactory.createGeneral();
        dsg.addGraph(A, bg);
        assertFalse(QueryEngineBG.isBeakGraphDataset(dsg), "the premise: this dataset runs on Jena's engine and executor");
        // Written least-selective first: 106 ex:name rows before 3 ex:knows rows.
        String q = "PREFIX ex: <" + NS + "> SELECT ?fn WHERE { GRAPH <" + A + "> { ?f ex:name ?fn . ?p ex:knows ?f } }";
        long solved = PatternMatchBG.HITS.get();
        long reordered = OpExecutorBG.REORDERS.get();
        Set<String> names = new TreeSet<>();
        try (QueryExecution qe = QueryExecution.dataset(org.apache.jena.query.DatasetFactory.wrap(dsg))
                .query(QueryFactory.create(q)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                names.add(rs.next().getLiteral("fn").getLexicalForm());
            }
        }
        assertEquals(new TreeSet<>(Set.of("Bob", "Dave", "Frank")), names);
        assertEquals(1, PatternMatchBG.HITS.get() - solved, "the BGP ran on the id-level solver through the stage generator");
        assertEquals(1, OpExecutorBG.REORDERS.get() - reordered, "the stage generator must reorder the BGP (knows before name)");
    }
}
