package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.fuseki.RelativeIRIResolver;
import com.ebremer.beakgraph.core.lib.RelativeIris;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-390: a merge parsed every source against the ONE
 * sentinel base and re-relativized, so {@code <>} from N documents and
 * {@code <img.png>} from {@code a/x.ttl} and {@code b/y.ttl} - distinct
 * resources per RFC 3986 - collapsed onto one dictionary term, carrying both
 * documents' statements. Each merged document now parses against the
 * sentinel directory plus its path relative to the merge root, so its
 * references are stored relative to that root and the merged store serves
 * them below its own URL with the source tree's layout.
 */
class MergeRelativeIriTest {

    private static final String DOC =
        "<> <http://ex.org/label> \"%s\" ; <http://ex.org/thumb> <img.png> ; <http://ex.org/up> <../shared.png> ; <http://ex.org/frag> <#f> .\n";

    @TempDir
    Path dir;

    private List<File> tree(String name) throws Exception {
        Path root = Files.createDirectories(dir.resolve(name));
        Files.createDirectories(root.resolve("a"));
        Files.createDirectories(root.resolve("b").resolve("c"));
        Path x = root.resolve("a").resolve("x.ttl");
        Path y = root.resolve("b").resolve("c").resolve("y.ttl");
        Files.writeString(x, String.format(DOC, "doc a"), StandardCharsets.UTF_8);
        Files.writeString(y, String.format(DOC, "doc b"), StandardCharsets.UTF_8);
        return List.of(x.toFile(), y.toFile());
    }

    private static List<String> rows(File h5, String query) {
        List<String> out = new ArrayList<>();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5));
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                rs.getResultVars().forEach(v -> sb.append(qs.get(v).asNode().isURI() ? qs.get(v).asNode().getURI() : qs.get(v).toString()).append(' '));
                out.add(sb.toString().trim());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static final List<String> EXPECTED_SPO = List.of(
        "a/x.ttl http://ex.org/frag a/x.ttl#f",
        "a/x.ttl http://ex.org/label doc a",
        "a/x.ttl http://ex.org/thumb a/img.png",
        "a/x.ttl http://ex.org/up shared.png",
        "b/c/y.ttl http://ex.org/frag b/c/y.ttl#f",
        "b/c/y.ttl http://ex.org/label doc b",
        "b/c/y.ttl http://ex.org/thumb b/c/img.png",
        "b/c/y.ttl http://ex.org/up b/shared.png");

    private static final String ALL = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o";

    @Test
    void ramWriterStoresEachDocumentRelativeToTheRoot() throws Exception {
        List<File> inputs = tree("ram");
        File h5 = dir.resolve("ram.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setSourceRoot(dir.resolve("ram").toFile()).setDestination(h5).build().write();
        assertEquals(EXPECTED_SPO, rows(h5, ALL));
    }

    @Test
    void parallelAndUltraWritersAgree() throws Exception {
        List<File> inputs = tree("engines");
        File root = dir.resolve("engines").toFile();
        File par = dir.resolve("par.h5").toFile();
        File ult = dir.resolve("ult.h5").toFile();
        ParallelHDF5Writer.Builder().setSources(inputs).setSourceRoot(root).setDestination(par).setCores(2).build().write();
        UltraHDF5Writer.Builder().setSources(inputs).setSourceRoot(root).setDestination(ult).setCores(2).build().write();
        assertEquals(EXPECTED_SPO, rows(par, ALL));
        assertEquals(EXPECTED_SPO, rows(ult, ALL));
    }

    @Test
    void withoutARootTheCommonAncestorIsUsed() throws Exception {
        List<File> inputs = tree("noroot");
        File h5 = dir.resolve("noroot.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setDestination(h5).build().write();
        // a/x.ttl and b/c/y.ttl share the tree root, so the result is the same.
        assertEquals(EXPECTED_SPO, rows(h5, ALL));
    }

    @Test
    void mergedStoreServesTheSourceTreeLayout() throws Exception {
        List<File> inputs = tree("served");
        File h5 = dir.resolve("served.h5").toFile();
        HDF5Writer.Builder().setSources(inputs).setSourceRoot(dir.resolve("served").toFile()).setDestination(h5).build().write();
        RelativeIRIResolver r = new RelativeIRIResolver("http://host/store/merged.h5");
        assertEquals("http://host/store/a/x.ttl", r.storageToAbsolute().apply(NodeFactory.createURI("a/x.ttl")).getURI());
        assertEquals("http://host/store/b/c/img.png", r.storageToAbsolute().apply(NodeFactory.createURI("b/c/img.png")).getURI());
        assertEquals("http://host/store/shared.png", r.storageToAbsolute().apply(NodeFactory.createURI("shared.png")).getURI());
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            var dict = bg.getReader().getDictionary();
            // A query naming the served IRI of a/x.ttl reaches its stored term.
            assertEquals("a/x.ttl", r.absoluteToStorage(n -> dict.getSubjects().locate(n) >= 1 || dict.getObjects().locate(n) >= 1)
                    .apply(NodeFactory.createURI("http://host/store/a/x.ttl")).getURI());
        }
    }

    @Test
    void singleSourceBuildsAreUnchanged() throws Exception {
        List<File> inputs = tree("single");
        File one = dir.resolve("one.h5").toFile();
        File mergedOne = dir.resolve("merged-one.h5").toFile();
        HDF5Writer.Builder().setSource(inputs.get(0)).setDestination(one).build().write();
        HDF5Writer.Builder().setSources(List.of(inputs.get(0))).setSourceRoot(dir.resolve("single").toFile()).setDestination(mergedOne).build().write();
        List<String> expected = List.of(
            " http://ex.org/frag #f", " http://ex.org/label doc a", " http://ex.org/thumb img.png", " http://ex.org/up ../shared.png");
        List<String> single = rows(one, ALL);
        assertEquals(expected, single.stream().map(s -> s.startsWith("http") ? " " + s : s).toList());
        assertEquals(single, rows(mergedOne, ALL), "one document merged alone parses like a single-source build");
    }

    @Test
    void parseBaseEncodesPathsAndKeepsSingleSourceSentinel() throws Exception {
        File root = dir.resolve("enc").toFile();
        File odd = new File(root, "sub dir/we%ird #1.ttl");
        File other = new File(root, "plain.ttl");
        assertEquals(RelativeIris.SENTINEL_DIR + "sub%20dir/we%25ird%20%231.ttl",
                RelativeIris.parseBase(odd, List.of(odd, other), root));
        assertEquals(RelativeIris.SENTINEL_DIR + "plain.ttl", RelativeIris.parseBase(other, List.of(odd, other), root));
        assertEquals(RelativeIris.SENTINEL_BASE, RelativeIris.parseBase(other, List.of(other), root));
        assertEquals(RelativeIris.SENTINEL_BASE, RelativeIris.parseBase(other, null, null));
        // Common ancestor when no root is given.
        File deep = new File(root, "x/y/z.ttl");
        File deeper = new File(root, "x/w/q.ttl");
        assertEquals(new File(root, "x").toPath().toAbsolutePath().normalize(),
                RelativeIris.commonAncestor(List.of(deep, deeper)));
        assertEquals(RelativeIris.SENTINEL_DIR + "y/z.ttl", RelativeIris.parseBase(deep, List.of(deep, deeper), null));
        assertTrue(RelativeIris.parseBase(deep, List.of(deep, deeper), root).endsWith("x/y/z.ttl"));
    }
}
