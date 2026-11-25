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

    /**
     * @param diskBased true for the engines that sort and index in a workspace on disk
     *                  (-method 1/4/5); none needs a native HDF5 library to be found
     */
    public record Engine(String name, boolean diskBased, Factory factory) {
        public void buildStore(File src, File dest) throws Exception {
            factory.create(src, dest).write();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * {@code -Dbeakgraph.test.arrays=minRows,minLeaves}: every engine builds
     * with {@code -arrays} at those settings, so the suites that build through
     * here (the rdf-array contrib suite, the W3C suites) also run their queries
     * over packed sequences (RDFARRY.md §3.6). A third value {@code only}
     * ({@code 1,2,only}) builds with {@code -arrays-only} instead (format v8,
     * step 9), where the engine writes it. Unset: no {@code -arrays}.
     */
    public static final String ARRAYS_PROPERTY = "beakgraph.test.arrays";

    static com.ebremer.beakgraph.hdf5.writers.SequenceArrays.Options testArrays() {
        String v = System.getProperty(ARRAYS_PROPERTY);
        if (v == null || v.isBlank()) {
            return null;
        }
        String[] parts = v.split(",");
        boolean only = parts.length > 2 && parts[2].trim().equals("only");
        return new com.ebremer.beakgraph.hdf5.writers.SequenceArrays.Options(Integer.parseInt(parts[0].trim()),
                Integer.parseInt(parts[1].trim()), false).withOnly(only);
    }

    /**
     * {@code -Dbeakgraph.test.seqindex=true}: every engine builds with {@code -seqindex}, so the suites that
     * build through here also answer their sequence patterns through the element index (RDFARRY.md step 8);
     * run them with {@code -Dbeakgraph.seqindex.ratio=0} too, or the index is read only where it names few
     * of a scan's objects, which a small fixture seldom does.
     */
    public static final String SEQINDEX_PROPERTY = "beakgraph.test.seqindex";

    static boolean testSequenceIndex() {
        return Boolean.getBoolean(SEQINDEX_PROPERTY);
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
                HDF5Writer.Builder().setSource(src).setDestination(dest).setArrays(testArrays()).setSequenceIndex(testSequenceIndex()).build()),
            new Engine("method1-Huge", true, (src, dest) -> {
                HugeHDF5Writer.Builder b = HugeHDF5Writer.Builder().setDestination(dest).setArrays(testArrays()).setSequenceIndex(testSequenceIndex());
                b.setSource(src);
                return b.build();
            }),
            new Engine("method2-Parallel", false, (src, dest) -> {
                ParallelHDF5Writer.Builder b = ParallelHDF5Writer.Builder()
                        .setDestination(dest).setCores(2).setArrays(testArrays()).setSequenceIndex(testSequenceIndex());
                b.setSource(src);
                return b.build();
            }),
            new Engine("method3-Ultra", false, (src, dest) -> {
                UltraHDF5Writer.Builder b = UltraHDF5Writer.Builder()
                        .setDestination(dest).setCores(2).setArrays(testArrays()).setSequenceIndex(testSequenceIndex());
                b.setSource(src);
                return b.build();
            }),
            new Engine("method4-HugeUltra", true, (src, dest) -> {
                HugeUltraHDF5Writer.Builder b = HugeUltraHDF5Writer.Builder()
                        .setDestination(dest).setCores(2).setArrays(testArrays()).setSequenceIndex(testSequenceIndex());
                b.setSource(src);
                return b.build();
            }),
            new Engine("method5-Plaid", true, (src, dest) -> {
                PlaidHDF5Writer.Builder b = PlaidHDF5Writer.Builder()
                        .setDestination(dest).setCores(2).setArrays(testArrays()).setSequenceIndex(testSequenceIndex());
                b.setSource(src);
                return b.build();
            })
        );
    }
}
