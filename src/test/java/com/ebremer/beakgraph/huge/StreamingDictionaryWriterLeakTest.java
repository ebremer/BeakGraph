package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * BG-128: a constructor that opened some spill streams and then failed to
 * open the next one used to leave the first ones open (the half-built object
 * was never returned, so nothing could close them) - on Windows the workspace
 * then could not be deleted.
 */
class StreamingDictionaryWriterLeakTest {

    @TempDir
    Path dir;

    @Test
    void aFailedConstructorReleasesTheStreamsItOpened() throws Exception {
        Path work = Files.createDirectories(dir.resolve("work"));
        // The SECOND stream's path is a directory: offsets opens, datatypes fails.
        Files.createDirectories(work.resolve("dict.entities").resolve("datatypes"));
        Stats stats = new Stats();
        stats.numIRI = 5;
        assertThrows(IOException.class, () -> new StreamingDictionaryWriter(work, "entities", 5, stats,
                Set.of(Types.IRI, Types.BNODE), new TreeSet<>(), new TreeSet<>(), false, null));
        Workspaces.deleteTree(work, LoggerFactory.getLogger(StreamingDictionaryWriterLeakTest.class));
        assertTrue(Files.notExists(work), "every stream the constructor opened is closed, so the workspace is removable");
    }

    @Test
    void aFailedFcdConstructorReleasesItsStringStream() throws Exception {
        Path work = Files.createDirectories(dir.resolve("fcd"));
        Files.createDirectories(work.resolve("iri.fcd.offsets"));   // in the way of the second stream
        assertThrows(IOException.class, () -> new SpillFCDWriter(work, "iri", 16));
        Workspaces.deleteTree(work, LoggerFactory.getLogger(StreamingDictionaryWriterLeakTest.class));
        assertTrue(Files.notExists(work));
    }
}
