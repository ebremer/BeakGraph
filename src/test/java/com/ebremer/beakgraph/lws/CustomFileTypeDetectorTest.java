package com.ebremer.beakgraph.lws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** BG-319: .hdf5 is the same media type as .h5, in any case. */
class CustomFileTypeDetectorTest {

    @Test
    void hdf5ExtensionsMapToTheHdf5MediaType() {
        CustomFileTypeDetector d = new CustomFileTypeDetector();
        assertEquals("application/vnd.hdfgroup.hdf5", d.probeContentType(Path.of("store.h5")));
        assertEquals("application/vnd.hdfgroup.hdf5", d.probeContentType(Path.of("store.hdf5")));
        assertEquals("application/vnd.hdfgroup.hdf5", d.probeContentType(Path.of("STORE.HDF5")));
        assertEquals("text/turtle", d.probeContentType(Path.of("data.ttl")));
        assertNull(d.probeContentType(Path.of("README")));
        assertEquals(true, com.ebremer.beakgraph.core.BeakGraphFiles.isBeakGraphFileName("x.HDF5"));
        assertEquals("x", com.ebremer.beakgraph.core.BeakGraphFiles.stripExtension("x.hdf5"));
        assertEquals("x", com.ebremer.beakgraph.core.BeakGraphFiles.stripExtension("x.h5"));
        assertEquals("x.txt", com.ebremer.beakgraph.core.BeakGraphFiles.stripExtension("x.txt"));
    }
}
