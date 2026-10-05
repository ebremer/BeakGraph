package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.ebremer.beakgraph.huge.NativeHdf5File;

/**
 * One rule for tests that need the native HDF5 backend: skip when it is
 * unavailable, unless {@code -Dbeakgraph.test.requireNative=true} says the
 * environment is expected to have it (CI's Linux leg, where the bundled
 * JavaCPP natives must load) - then fail red instead of silently skipping
 * -method 1/4/5 coverage.
 */
public final class NativeTestSupport {

    public static final String REQUIRE_PROPERTY = "beakgraph.test.requireNative";

    private NativeTestSupport() {}

    public static void assumeNative() {
        boolean available = NativeHdf5File.isAvailable();
        String why = "native HDF5 library unavailable: " + NativeHdf5File.getUnavailableCause()
                + " (sought: " + NativeHdf5File.nativeSource() + ")";
        if (Boolean.getBoolean(REQUIRE_PROPERTY)) {
            assertTrue(available, "-D" + REQUIRE_PROPERTY + "=true but " + why);
        } else {
            assumeTrue(available, why);
        }
    }
}
