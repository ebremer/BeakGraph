package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * BG-439: the HDF Group's 2.1.x FFM binding (the {@code -Dhdf5.ffm=true}
 * profile) stubs every typed attribute / dataset helper
 * ({@code H5Awrite_int}, {@code H5Awrite_long}, {@code H5Dwrite_int}, ...,
 * {@code H5Aread_string}, ...) with a throwing "not implemented yet", while the
 * {@code byte[]} entry points are real in both bindings. NativeHdf5File used
 * the typed helpers for its attributes, so under that profile every huge
 * build died at its first attribute - after the whole parse/spill/sort. No CI
 * leg can build the profile (it needs a GitHub Packages PAT), so this pins the
 * portability rule where CI can see it: the class file must reference no
 * typed helper, only the byte[] forms.
 */
class NativeHdf5PortabilityTest {

    /** The outer class plus its nested group / dataset implementations. */
    private static byte[] classBytes() throws Exception {
        java.io.ByteArrayOutputStream all = new java.io.ByteArrayOutputStream();
        for (String name : List.of("NativeHdf5File.class", "NativeHdf5File$NativeGroup.class", "NativeHdf5File$NativeDataset.class")) {
            try (InputStream in = NativeHdf5File.class.getResourceAsStream(name)) {
                assertTrue(in != null, "class resource " + name);
                all.write(in.readAllBytes());
            }
        }
        return all.toByteArray();
    }

    private static boolean contains(byte[] hay, String needle) {
        byte[] n = needle.getBytes(StandardCharsets.US_ASCII);
        outer:
        for (int i = 0; i + n.length <= hay.length; i++) {
            for (int j = 0; j < n.length; j++) {
                if (hay[i + j] != n[j]) continue outer;
            }
            return true;
        }
        return false;
    }

    @Test
    void onlyTheByteArrayEntryPointsAreReferenced() throws Exception {
        byte[] cls = classBytes();
        // Method names live as modified-UTF-8 constant-pool entries, so a
        // plain byte search finds every referenced hdf.hdf5lib.H5 method.
        assertTrue(contains(cls, "H5Awrite"), "attributes are written through H5Awrite");
        assertTrue(contains(cls, "H5Dwrite"), "datasets are written through H5Dwrite");
        List<String> forbidden = new ArrayList<>();
        for (String stub : List.of("H5Awrite_", "H5Dwrite_", "H5Aread_", "H5Dread_", "H5AreadComplex", "H5Acopy", "H5Dcopy", "H5export_")) {
            if (contains(cls, stub)) forbidden.add(stub);
        }
        assertEquals(List.of(), forbidden,
                "typed H5 helpers are throwing stubs in the FFM binding; use the byte[] overloads (BG-439)");
    }
}
