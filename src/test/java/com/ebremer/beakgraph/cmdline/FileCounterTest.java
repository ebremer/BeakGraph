package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class FileCounterTest {

    @Test
    void successfulConversionsAreRdfMinusFailedOnly() {
        FileCounter fc = new FileCounter();
        // The traverse filter rejects zero-length files BEFORE they count as RDF
        // files, so the summary must not subtract them a second time (the old
        // formula printed rdf - zero - failed, which understated the count and
        // went negative when empties outnumbered conversions).
        for (int i = 0; i < 7; i++) fc.incrementRDFFileCount();
        for (int i = 0; i < 3; i++) fc.incrementZeroLengthFileCount();
        for (int i = 0; i < 3; i++) fc.incrementFailedConversionFileCount();
        assertEquals(4, fc.getSuccessfulConversionCount());
        assertTrue(fc.toString().contains("Successful Conversions : 4"), fc.toString());
    }

    @Test
    void manyEmptyFilesCannotDriveTheSummaryNegative() {
        FileCounter fc = new FileCounter();
        for (int i = 0; i < 2; i++) fc.incrementRDFFileCount();
        for (int i = 0; i < 5; i++) fc.incrementZeroLengthFileCount();
        assertEquals(2, fc.getSuccessfulConversionCount());
    }
}
