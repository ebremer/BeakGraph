package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-55 / BG-330 / BG-444: the GSPO full-scan iterator
 * honoured only CONCRETE subject and object terms. A variable already bound in
 * the parent binding (a join's second pattern, VALUES) left the ranges wide
 * open, so "?y ?p ?o" after "?x :knows ?y" walked the graph's entire subject
 * range per input row and rejected every row in computeNext; and an object
 * range was filtered row by row instead of navigated. The iterator now clamps
 * to the bound id, binary-searches the subject, and seeks/skips objects; the
 * visit counter proves it, and Jena's in-memory answers prove correctness.
 */
class BGIteratorSPO_AllBoundVarTest {

    private static final int SUBJECTS = 500;
    private static final int TAGS = 20;
    private static final String EX = "http://ex.org/";

    @TempDir
    static Path dir;
    private static BeakGraph bg;
    private static HDF5Reader reader;
    private static Dataset ds;
    private static Dataset reference;
    private static long totalRows;

    @BeforeAll
    static void build() throws Exception {
        StringBuilder ttl = new StringBuilder("@prefix ex: <" + EX + "> .\n");
        for (int i = 0; i < SUBJECTS; i++) {
            ttl.append("ex:s").append(i).append(" ex:knows ex:s").append((i + 1) % SUBJECTS).append(" .\n");
            for (int t = 0; t < TAGS; t++) {
                ttl.append("ex:s").append(i).append(" ex:tag \"t").append(String.format("%02d", t)).append("\" .\n");
            }
        }
        totalRows = (long) SUBJECTS * (1 + TAGS);
        File src = dir.resolve("bound.ttl").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        File h5 = dir.resolve("bound.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        reader = new HDF5Reader(h5);
        bg = new BeakGraph(reader);
        ds = bg.getDataset();
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new StringReader(ttl.toString()), null, Lang.TURTLE);
        reference = DatasetFactory.create(m);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    /** The graph node the reader uses for the default graph (what HDF5Reader.read passes on). */
    private static Node defaultGraphNode() throws Exception {
        Field f = HDF5Reader.class.getDeclaredField("defaultGraph");
        f.setAccessible(true);
        return (Node) f.get(reader);
    }

    private static BGIteratorSPO_All scan(BindingNodeId bnid) throws Exception {
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        IndexReader gspo = reader.getIndexReader(Index.GSPO);
        Quad q = new Quad(defaultGraphNode(), Var.alloc("s"), Var.alloc("p"), Var.alloc("o"));
        return new BGIteratorSPO_All(dict, gspo, bnid, q, null, reader.getNodeTable());
    }

    private static int drain(BGIteratorSPO_All it) {
        int n = 0;
        while (it.hasNext()) { it.next(); n++; }
        return n;
    }

    @Test
    void unboundScanVisitsEveryRow() throws Exception {
        BGIteratorSPO_All it = scan(new BindingNodeId());
        assertEquals(totalRows, drain(it));
        assertEquals(totalRows, it.rowsVisited(), "a genuine full scan examines every row");
    }

    @Test
    void boundSubjectVariableSeeksInsteadOfScanning() throws Exception {
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        long sid = dict.getSubjects().locate(NodeFactory.createURI(EX + "s" + (SUBJECTS / 2)));
        assertTrue(sid > 0);
        BindingNodeId bnid = new BindingNodeId();
        bnid.put(Var.alloc("s"), NodeId.pack(NodeType.SUBJECT, sid));
        BGIteratorSPO_All it = scan(bnid);
        assertEquals(1 + TAGS, drain(it), "all rows of the bound subject, nothing else");
        assertTrue(it.rowsVisited() <= 1 + TAGS + 2,
                "a bound subject must binary-search the subject range, not walk it: visited " + it.rowsVisited());
    }

    @Test
    void boundObjectVariableNavigatesObjectBlocks() throws Exception {
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        long oid = dict.getObjects().locate(NodeFactory.createLiteralString("t17"));
        assertTrue(oid > 0);
        BindingNodeId bnid = new BindingNodeId();
        bnid.put(Var.alloc("o"), NodeId.pack(NodeType.OBJECT, oid));
        BGIteratorSPO_All it = scan(bnid);
        assertEquals(SUBJECTS, drain(it), "one t17 row per subject");
        assertTrue(it.rowsVisited() < totalRows / 3,
                "object range must be sought/skipped per block, not filtered row by row: visited " + it.rowsVisited());
    }

    // --- end to end through HDF5Reader.read / BGIteratorMaster: answers match Jena ---

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                rs.getResultVars().forEach(v -> sb.append(v).append('=').append(qs.get(v)).append(' '));
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    @Test
    void joinBoundSubjectAndObjectMatchJena() {
        String prefix = "PREFIX ex: <" + EX + "> ";
        String[] queries = {
            prefix + "SELECT * WHERE { VALUES ?s { ex:s7 ex:s499 } ?s ?p ?o }",
            prefix + "SELECT * WHERE { ex:s3 ex:knows ?y . ?y ?p ?o }",
            prefix + "SELECT * WHERE { ?a ex:knows ?b . ?c ?p ?b FILTER(?a = ex:s10) }",
            prefix + "SELECT * WHERE { VALUES ?o { \"t05\" ex:s2 } ?s ?p ?o }",
            prefix + "SELECT * WHERE { VALUES ?s { ex:nosuch } ?s ?p ?o }",
        };
        for (String q : queries) {
            assertEquals(rows(reference, q), rows(ds, q), q);
        }
    }
}
