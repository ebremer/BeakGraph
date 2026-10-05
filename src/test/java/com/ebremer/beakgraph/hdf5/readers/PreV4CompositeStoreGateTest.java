package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.jhdf.api.WritableGroup;
import io.jhdf.api.WritableNode;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-262: the format gate only rejected files NEWER than
 * the build. BeakGraph 0.17.0 wrote format v3 with a NodeComparator that had
 * no composite branch, so cdt:List / cdt:Map literals were ranked by
 * compareAlways value order; the v4 bump switched them to exact lexical
 * order. Ids are comparator ranks, so such a file opened without complaint
 * and every binary search on a composite term probed with the wrong order
 * and silently missed stored literals. The reader now refuses a pre-v4 file
 * whose literals section carries a composite datatype, and still opens
 * pre-v4 files without one.
 */
class PreV4CompositeStoreGateTest {

    private static final String PREFIXES =
        "@prefix ex: <http://ex.org/> .\n@prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .\n";
    private static final String WITH_CDT = PREFIXES
        + "ex:a ex:p \"[9]\"^^cdt:List .\nex:b ex:p \"[10]\"^^cdt:List .\nex:c ex:p \"{\\\"k\\\":1}\"^^cdt:Map .\nex:d ex:p 5 .\n";
    private static final String WITHOUT_CDT = PREFIXES
        + "ex:a ex:p \"nine\" .\nex:b ex:p 10 .\nex:c ex:p \"2020-01-01\"^^<http://www.w3.org/2001/XMLSchema#date> .\n";

    @TempDir
    Path dir;

    private File build(String name, String ttl) throws Exception {
        File src = dir.resolve(name + ".ttl").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        File h5 = dir.resolve(name + ".h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        return h5;
    }

    private static long storedFormatVersion(File h5) {
        try (HdfFile f = new HdfFile(h5.toPath())) {
            Group bg = (Group) f.getChild(Params.BG);
            return ((Number) bg.getAttribute("formatVersion").getData()).longValue();
        }
    }

    /**
     * Produces a copy of {@code src} whose .BG formatVersion attribute reads
     * {@code version}. jHDF cannot modify a file in place and its version-2
     * object headers are checksummed, so the store is re-emitted through the
     * same jHDF write API the -method 0 writer uses: every group, dataset and
     * attribute is copied verbatim except that one attribute.
     */
    private static File withFormatVersion(File src, int version) throws Exception {
        File dst = new File(src.getParentFile(), src.getName().replace(".h5", "-v" + version + ".h5"));
        try (HdfFile in = new HdfFile(src.toPath()); WritableHdfFile out = HdfFile.write(dst.toPath())) {
            copyChildren(in, out, version);
        }
        assertEquals(version, storedFormatVersion(dst));
        return dst;
    }

    private static void copyChildren(Group from, WritableGroup to, int version) {
        for (Node child : from.getChildren().values()) {
            if (child instanceof Group g) {
                WritableGroup wg = to.putGroup(g.getName());
                copyAttributes(g, wg, g.getName().equals(Params.BG) ? version : null);
                copyChildren(g, wg, version);
            } else if (child instanceof Dataset d) {
                WritableNode wd = to.putDataset(d.getName(), d.getData());
                copyAttributes(d, wd, null);
            }
        }
    }

    private static void copyAttributes(Node from, WritableNode to, Integer formatVersion) {
        for (Map.Entry<String, Attribute> e : from.getAttributes().entrySet()) {
            Object value = e.getValue().getData();
            if (formatVersion != null && e.getKey().equals("formatVersion")) {
                value = formatVersion;
            }
            to.putAttribute(e.getKey(), value);
        }
    }

    private static int count(File h5, String query) {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5));
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void preV4StoreWithCompositeLiteralsIsRefused() throws Exception {
        File built = build("cdt", WITH_CDT);
        assertEquals(Params.FORMAT_VERSION, storedFormatVersion(built));
        File h5 = withFormatVersion(built, 3);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> new HDF5Reader(h5));
        assertTrue(ex.getMessage().contains("cdt:List/cdt:Map"), ex.getMessage());
        assertTrue(ex.getMessage().contains("Rebuild"), ex.getMessage());
        assertTrue(ex.getMessage().contains("version 3"), ex.getMessage());

        // The handle must be released on the failed open (Windows file lock):
        // the file can still be deleted afterwards.
        assertTrue(h5.delete(), "file handle leaked by the refused open");
    }

    @Test
    void legacyUnversionedStoreWithCompositeLiteralsIsRefused() throws Exception {
        // Files written before format versioning carry no attribute and read
        // as version 1; they predate composite ordering just the same.
        assertThrows(IllegalStateException.class,
                () -> new HDF5Reader(withFormatVersion(build("cdt-legacy", WITH_CDT), 1)));
    }

    @Test
    void preV4StoreWithoutCompositeLiteralsStillOpens() throws Exception {
        File h5 = withFormatVersion(build("plain", WITHOUT_CDT), 3);
        assertEquals(3, count(h5, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o }"));
        assertEquals(1, count(h5, "SELECT ?s WHERE { ?s <http://ex.org/p> 10 }"));
    }

    @Test
    void v4StoreWithCompositeLiteralsOpens() throws Exception {
        File h5 = withFormatVersion(build("cdt4", WITH_CDT), Params.CDT_LEXICAL_ORDER_MIN_VERSION);
        assertEquals(1, count(h5, "PREFIX cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> "
                + "SELECT ?s WHERE { ?s <http://ex.org/p> \"[10]\"^^cdt:List }"));
    }

    @Test
    void compositeProbeReadsTheDatatypeTable() throws Exception {
        try (HDF5Reader r = new HDF5Reader(build("probe-cdt", WITH_CDT))) {
            assertTrue(((PositionalDictionaryReader) r.getDictionary()).literalsContainCompositeDatatype());
        }
        try (HDF5Reader r = new HDF5Reader(build("probe-plain", WITHOUT_CDT))) {
            assertFalse(((PositionalDictionaryReader) r.getDictionary()).literalsContainCompositeDatatype());
        }
    }
}
