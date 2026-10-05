package com.ebremer.beakgraph;

import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder;
import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriter;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-279: every engine's build() names the missing setter instead of
 * dying later on a bare NullPointerException from write() - after the disk
 * engines had already created a workspace.
 */
class BuilderValidationTest {

    @TempDir
    Path dir;

    private List<Supplier<AbstractGraphBuilder<?>>> builders() {
        return List.of(HDF5Writer::Builder, ParallelHDF5Writer::Builder, UltraHDF5Writer::Builder,
                HugeHDF5Writer::Builder, HugeUltraHDF5Writer::Builder, PlaidHDF5Writer::Builder);
    }

    @Test
    void everyEngineNamesTheMissingSetter() throws Exception {
        File src = dir.resolve("in.ttl").toFile();
        Files.writeString(src.toPath(), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        for (Supplier<AbstractGraphBuilder<?>> make : builders()) {
            AbstractGraphBuilder<?> noDest = make.get().setSource(src);
            IllegalStateException a = assertThrows(IllegalStateException.class, noDest::build, noDest.getClass().getName());
            assertTrue(a.getMessage().contains("No destination set"), a.getMessage());
            AbstractGraphBuilder<?> noSource = make.get().setDestination(dir.resolve("out.h5").toFile());
            IllegalStateException b = assertThrows(IllegalStateException.class, noSource::build, noSource.getClass().getName());
            assertTrue(b.getMessage().contains("No source set"), b.getMessage());
            // setSources satisfies the source requirement.
            make.get().setSources(List.of(src)).setDestination(dir.resolve("out.h5").toFile()).build();
        }
        try (var entries = Files.list(dir)) {
            assertEquals(List.of("in.ttl"), entries.map(p -> p.getFileName().toString()).toList(),
                    "build() creates no workspace or output");
        }
    }

    /** BG-102: the ingest builder accumulates state, so a second build() is refused. */
    @Test
    void theIngestBuilderIsSingleUse() throws Exception {
        File src = dir.resolve("once.ttl").toFile();
        Files.writeString(src.toPath(), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        PositionalDictionaryWriterBuilder b = new PositionalDictionaryWriterBuilder()
                .setSource(src).setDestination(dir.resolve("once.h5").toFile()).setName("bg");
        try (PositionalDictionaryWriter first = b.build()) {
            assertEquals(3, b.getEntities().size(), "a, b and the default-graph node");
            assertThrows(UnsupportedOperationException.class, () -> b.getEntities().clear(), "getters are read-only views");
        }
        IllegalStateException ex = assertThrows(IllegalStateException.class, b::build);
        assertTrue(ex.getMessage().contains("single-use"), ex.getMessage());
    }

    /** BG-102: a writer snapshots its builder; mutating the builder afterwards changes nothing. */
    @Test
    void aWriterSnapshotsItsBuilder() throws Exception {
        File src = dir.resolve("snap.ttl").toFile();
        Files.writeString(src.toPath(), "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n");
        File dest = dir.resolve("snap.h5").toFile();
        File elsewhere = dir.resolve("elsewhere.h5").toFile();
        HDF5Writer.Builder builder = HDF5Writer.Builder().setSource(src).setDestination(dest);
        HDF5Writer writer = builder.build();
        builder.setDestination(elsewhere).setSource(dir.resolve("missing.ttl").toFile());
        writer.write();
        assertTrue(dest.isFile(), "the writer keeps the destination it was built with");
        assertFalse(elsewhere.exists());
    }
}
