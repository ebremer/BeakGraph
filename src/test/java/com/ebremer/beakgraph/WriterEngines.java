package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraphWriter;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.hugeUltra.HugeUltraHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.plaid.PlaidHDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer;
import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import java.io.File;
import java.util.stream.Stream;

/**
 * Test-support enumeration of all six writer engines, so cross-engine suites
 * iterate ONE list instead of mirroring six builder invocations per test class
 * (the same mirror-topology hazard the production code has).
 */
public final class WriterEngines {

    private WriterEngines() {}

    @FunctionalInterface
    public interface Factory {
        BeakGraphWriter create(File src, File dest) throws Exception;
    }

    public record Engine(String name, boolean needsNative, Factory factory) {
        public void assumeAvailable() {
            if (needsNative) {
                NativeTestSupport.assumeNative();
            }
        }

        public void buildStore(File src, File dest) throws Exception {
            factory.create(src, dest).write();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** Names the engine the W3C suites and the triple-term tests build with. */
    public static final String ENGINE_PROPERTY = "beakgraph.test.engine";

    /**
     * The engine named by {@code -Dbeakgraph.test.engine} (a name from
     * {@link #all()}, or its {@code methodN} prefix; default method 0). The
     * vendored RDF 1.2 / SPARQL 1.2 suites and RDF12TripleTermTest build
     * through this, so CI can re-run them on the disk and parallel engines
     * (BG-293) instead of asserting six-engine conformance from method 0 alone.
     */
    public static Engine selected() {
        String name = System.getProperty(ENGINE_PROPERTY, "method0");
        return all().filter(e -> e.name().equals(name) || e.name().startsWith(name + "-"))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown " + ENGINE_PROPERTY + " '" + name
                        + "'; one of " + all().map(Engine::name).toList()));
    }

    public static Stream<Engine> all() {
        return Stream.of(
            new Engine("method0-HDF5Writer", false, (src, dest) ->
                HDF5Writer.Builder().setSource(src).setDestination(dest).build()),
            new Engine("method1-Huge", true, (src, dest) -> {
                HugeHDF5Writer.Builder b = HugeHDF5Writer.Builder().setDestination(dest);
                b.setSource(src);
                return b.build();
            }),
            new Engine("method2-Parallel", false, (src, dest) -> {
                ParallelHDF5Writer.Builder b = ParallelHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            }),
            new Engine("method3-Ultra", false, (src, dest) -> {
                UltraHDF5Writer.Builder b = UltraHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            }),
            new Engine("method4-HugeUltra", true, (src, dest) -> {
                HugeUltraHDF5Writer.Builder b = HugeUltraHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            }),
            new Engine("method5-Plaid", true, (src, dest) -> {
                PlaidHDF5Writer.Builder b = PlaidHDF5Writer.Builder()
                        .setDestination(dest).setCores(2);
                b.setSource(src);
                return b.build();
            })
        );
    }
}
