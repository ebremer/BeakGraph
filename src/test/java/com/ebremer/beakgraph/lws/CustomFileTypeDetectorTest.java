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
        // BG-52: the IANA-registered HDF5 type, and the common RDF / text / image extensions.
        assertEquals("application/x-hdf5", d.probeContentType(Path.of("store.h5")));
        assertEquals("application/x-hdf5", d.probeContentType(Path.of("store.hdf5")));
        assertEquals("application/x-hdf5", d.probeContentType(Path.of("STORE.HDF5")));
        assertEquals("application/x-hdf", d.probeContentType(Path.of("old.h4")));
        assertEquals("text/turtle", d.probeContentType(Path.of("data.ttl")));
        assertEquals("application/trig", d.probeContentType(Path.of("data.trig")));
        assertEquals("application/n-quads", d.probeContentType(Path.of("data.nq")));
        assertEquals("application/rdf+xml", d.probeContentType(Path.of("data.rdf")));
        assertEquals("text/csv", d.probeContentType(Path.of("table.csv")));
        assertEquals("text/html", d.probeContentType(Path.of("page.htm")));
        assertEquals("image/png", d.probeContentType(Path.of("logo.png")));
        assertEquals("application/gzip", d.probeContentType(Path.of("dump.nq.gz")));
        assertNull(d.probeContentType(Path.of("README")));
        assertEquals(true, com.ebremer.beakgraph.core.BeakGraphFiles.isBeakGraphFileName("x.HDF5"));
        assertEquals("x", com.ebremer.beakgraph.core.BeakGraphFiles.stripExtension("x.hdf5"));
        assertEquals("x", com.ebremer.beakgraph.core.BeakGraphFiles.stripExtension("x.h5"));
        assertEquals("x.txt", com.ebremer.beakgraph.core.BeakGraphFiles.stripExtension("x.txt"));
    }
}
