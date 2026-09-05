package com.ebremer.beakgraph.hdf5.writers.ultra;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.junit.jupiter.api.Test;

/** BG-285: the inherited sequential build() would parse into the base class's hidden state; it is refused. */
class UltraIngestContractTest {

    @Test
    void theIngestIsNotAStandaloneBuilder() {
        UltraIngest ingest = new UltraIngest();
        ingest.setSource(new File("whatever.ttl")).setDestination(new File("whatever.h5"));
        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class, ingest::build);
        assertTrue(ex.getMessage().contains("UltraHDF5Writer"), ex.getMessage());
    }
}
