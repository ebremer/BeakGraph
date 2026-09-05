package com.ebremer.beakgraph;

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
}
