package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.cmdline.VerifyCommand;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.jena.IndexExport;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The on-disk shape of a store built from an empty source, as SPECIFICATIONS
 * §7.9 now describes it (BG-349): the dictionary group with no sections, and
 * both index groups holding only their seeded rank/select directories - no
 * level datasets. Every engine agrees, the reader treats it as empty, -verify
 * passes it and the index export emits nothing.
 */
class EmptyStoreLayoutTest {

    @TempDir
    static Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void theEmptyStoreHasTheDocumentedLayoutOnEveryEngine(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path src = dir.resolve(engine.name() + "-empty.ttl");
        Files.writeString(src, "");
        File h5 = dir.resolve(engine.name() + "-empty.h5").toFile();
        engine.buildStore(src.toFile(), h5);

        try (HdfFile f = new HdfFile(h5.toPath())) {
            Group bg = (Group) f.getChild(Params.BG);
            Group dict = (Group) bg.getChild(Params.DICTIONARY);
            assertNotNull(dict, "the dictionary group exists");
            for (String section : new String[]{Params.ENTITIES, Params.PREDICATES, Params.LITERALS}) {
                assertNull(dict.getChild(section), "no " + section + " section");
            }
            for (Index idx : Index.values()) {
                Group g = (Group) bg.getChild(idx.name());
                assertNotNull(g, idx + " group exists");
                for (String name : g.getChildren().keySet()) {
                    assertTrue(name.startsWith("SB") || name.startsWith("BB"),
                            idx + " holds only its seeded directories, found " + name);
                }
            }
        }
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            assertTrue(((PositionalDictionaryReader) reader.getDictionary()).isEmpty());
            IndexReader gspo = reader.getIndexReader(Index.GSPO);
            assertNotNull(gspo, "the index group opens");
            assertNull(gspo.getIDBuffer('S'), "no level datasets");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            assertTrue(IndexExport.tryWrite(reader, out, true), "the index export handles the empty store");
            assertEquals(0, out.size());
        }
        assertEquals(0, new VerifyCommand(h5, true).run(), "-verify -deep accepts the legal empty store");
        try (BeakGraph bg = BG.getBeakGraph(h5);
             QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                     .query(QueryFactory.create("SELECT * WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }")).build()) {
            assertFalse(qe.execSelect().hasNext());
        }
    }
}
