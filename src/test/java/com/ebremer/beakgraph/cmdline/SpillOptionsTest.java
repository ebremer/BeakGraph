package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beust.jcommander.JCommander;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import java.io.File;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

/**
 * BG-125: the disk-based writers' spill sizing is reachable from the command
 * line - {@code -spillMB} (the term byte budget), {@code -termSpillBatch},
 * {@code -idSpillBatch} and {@code -mergeFanIn} - and lands on the builder of
 * each of -method 1, 4 and 5; unset options leave the engine defaults alone.
 */
class SpillOptionsTest {

    private static Parameters parse(String... args) {
        Parameters p = new Parameters();
        JCommander.newBuilder().addObject(p).build().parse(args);
        return p;
    }

    private static Object field(Object o, String name) throws Exception {
        for (Class<?> c = o.getClass(); c != null; c = c.getSuperclass()) {
            try {
                Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(o);
            } catch (NoSuchFieldException ignored) {
                // keep climbing
            }
        }
        throw new AssertionError("no field " + name + " on " + o.getClass());
    }

    private static Object builderOf(int method, String... extra) throws Exception {
        String[] args = new String[6 + extra.length];
        System.arraycopy(new String[]{"-src", "in.nq", "-dest", "out.h5", "-method", Integer.toString(method)}, 0, args, 0, 6);
        System.arraycopy(extra, 0, args, 6, extra.length);
        Parameters p = parse(args);
        Object writer = new BeakGraphCLI(p).newWriter(new File("in.nq"), null, new File("out.h5"));
        return field(writer, "builder");
    }

    @Test
    void optionsParse() {
        Parameters p = parse("-spillMB", "64", "-termSpillBatch", "1000", "-idSpillBatch", "2000", "-mergeFanIn", "8");
        assertEquals(64, p.spillMB);
        assertEquals(1000, p.termSpillBatch);
        assertEquals(2000, p.idSpillBatch);
        assertEquals(8, p.mergeFanIn);
        Parameters none = parse();
        assertNull(none.spillMB);
        assertNull(none.termSpillBatch);
    }

    @Test
    void optionsReachEveryDiskBasedBuilder() throws Exception {
        for (int method : new int[]{1, 4, 5}) {
            Object b = builderOf(method, "-spillMB", "64", "-termSpillBatch", "1000", "-idSpillBatch", "2000", "-mergeFanIn", "8");
            assertEquals(64L << 20, field(b, "termSpillBytes"), "-method " + method + " termSpillBytes");
            assertEquals(1000, field(b, "termSpillBatch"), "-method " + method);
            assertEquals(2000, field(b, "idSpillBatch"), "-method " + method);
            assertEquals(8, field(b, "mergeFanIn"), "-method " + method);
        }
        assertTrue(field(new BeakGraphCLI(parse("-src", "in.nq", "-dest", "out.h5", "-method", "1"))
                .newWriter(new File("in.nq"), null, new File("out.h5")), "builder") instanceof HugeHDF5Writer.Builder);
        assertTrue(builderOf(4) instanceof HugeUltraHDF5Writer.Builder);
        assertTrue(builderOf(5) instanceof PlaidHDF5Writer.Builder);
    }

    @Test
    void unsetOptionsKeepTheEngineDefaults() throws Exception {
        assertEquals(1 << 18, field(builderOf(1), "termSpillBatch"));
        assertEquals(64, field(builderOf(1), "mergeFanIn"));
        assertEquals(1 << 19, field(builderOf(4), "termSpillBatch"));
        assertEquals(128, field(builderOf(5), "mergeFanIn"));
        long budget1 = (Long) field(builderOf(1), "termSpillBytes");
        long budget4 = (Long) field(builderOf(4), "termSpillBytes");
        assertTrue(budget1 >= 16L << 20, "the default budget is at least 16 MiB: " + budget1);
        assertTrue(budget4 <= budget1, "background-spilling engines budget per batch in flight: " + budget4 + " vs " + budget1);
    }

    @Test
    void aFanInBelowTwoIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> builderOf(1, "-mergeFanIn", "1"));
    }
}
