package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelPositionalDictionaryWriterBuilder;
import com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import com.ebremer.beakgraph.huge.AbstractDiskWriterBuilder;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;
import org.junit.jupiter.api.Test;

/**
 * BG-314 / BG-276 / BG-121: the three disk builders share one settings base
 * with each engine's documented defaults; the programmatic core-count
 * defaults are what the javadoc says; and the builders' fluent chains keep
 * their concrete type through every setter.
 */
class WriterBuildersTest {

    @Test
    void diskBuildersCarryTheirDocumentedDefaults() {
        HugeHDF5Writer.Builder huge = HugeHDF5Writer.Builder();
        assertEquals(1 << 18, huge.getTermSpillBatch());
        assertEquals(1 << 21, huge.getIdSpillBatch());
        assertEquals(64, huge.getMergeFanIn());
        for (AbstractDiskWriterBuilder<?> b : new AbstractDiskWriterBuilder<?>[]{HugeUltraHDF5Writer.Builder(), PlaidHDF5Writer.Builder()}) {
            assertEquals(1 << 19, b.getTermSpillBatch(), b.getClass().getName());
            assertEquals(1 << 22, b.getIdSpillBatch(), b.getClass().getName());
            assertEquals(128, b.getMergeFanIn(), b.getClass().getName());
        }
        assertTrue(huge.getTermSpillBytes() >= 16L << 20, "byte budget floor");
        assertTrue(huge.getTermSpillBytes() >= HugeUltraHDF5Writer.Builder().getTermSpillBytes(),
                "one batch live (heap / 8) is at least the two-batch budget (heap / 16)");
    }

    @Test
    void diskBuildersValidateAndResolveTheWorkspace() {
        HugeUltraHDF5Writer.Builder b = HugeUltraHDF5Writer.Builder();
        assertThrows(IllegalArgumentException.class, () -> b.setTermSpillBatch(0));
        assertThrows(IllegalArgumentException.class, () -> b.setIdSpillBatch(0));
        assertThrows(IllegalArgumentException.class, () -> b.setMergeFanIn(1));
        assertThrows(IllegalArgumentException.class, () -> b.setTermSpillBytes(0));
        Path dest = Path.of("out", "store.h5");
        assertEquals(dest.toAbsolutePath().getParent(), b.workspaceBase(dest), "no -workdir: the destination's directory");
        b.setWorkDirectory(Path.of("scratch"));
        assertEquals(Path.of("scratch"), b.workspaceBase(dest));
    }

    @Test
    void coreDefaultsAreWhatTheDocsSay() {
        assertEquals(4, UltraHDF5Writer.DEFAULT_CORES);
        assertEquals(4, ParallelHDF5Writer.DEFAULT_CORES);
        assertEquals(4, UltraHDF5Writer.Builder().getCores());
        assertEquals(4, ParallelHDF5Writer.Builder().getCores());
        int all = Math.max(2, Runtime.getRuntime().availableProcessors());
        assertEquals(all, HugeUltraHDF5Writer.Builder().getCores(), "disk engines: all processors (min 2)");
        assertEquals(all, PlaidHDF5Writer.Builder().getCores());
    }

    @Test
    void fluentChainsKeepTheirConcreteType() {
        // Each line only compiles if every setter returns the concrete builder.
        HugeUltraHDF5Writer.Builder hu = HugeUltraHDF5Writer.Builder().setTermSpillBatch(8).setIdSpillBatch(8)
                .setMergeFanIn(2).setWorkDirectory(Path.of("w")).setCores(2).setMergeConcurrency(1);
        assertNotNull(hu);
        PlaidHDF5Writer.Builder pl = PlaidHDF5Writer.Builder().setTermSpillBytes(1L << 20).setCores(2).setMergeFanIn(3);
        assertNotNull(pl);
        HugeHDF5Writer.Builder hg = HugeHDF5Writer.Builder().setIdSpillBatch(4).setVoidMode(VoidMode.SKETCH).setMergeFanIn(2);
        assertNotNull(hg);
        ParallelPositionalDictionaryWriterBuilder db = new ParallelPositionalDictionaryWriterBuilder()
                .setVoidMode(VoidMode.EXACT).setVoidDatasetIri("urn:x:ds").setPool(ForkJoinPool.commonPool()).setName("bg");
        assertEquals("urn:x:ds", db.getVoidDatasetIri());
    }
}
