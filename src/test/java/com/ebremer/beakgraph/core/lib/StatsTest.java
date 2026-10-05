package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Stats reports what the dictionary writer allocates, and "-" for the sections
 * it has no values for - not the MIN/MAX seeds of the running extremes (BG-29).
 */
class StatsTest {

    private static String line(String report, String label) {
        for (String l : report.split("\n")) {
            if (l.trim().startsWith(label)) {
                return l.substring(l.indexOf(':') + 1).trim();
            }
        }
        throw new AssertionError("no line for " + label + " in\n" + report);
    }

    @Test
    void emptySectionsReportDashesAndZeroWidths() {
        Stats s = new Stats();
        String report = s.toString();
        assertFalse(report.contains(String.valueOf(Integer.MIN_VALUE)), "the int seed leaks: " + report);
        assertFalse(report.contains(String.valueOf(Long.MIN_VALUE)), "the long seed leaks: " + report);
        assertEquals("-", line(report, "MaxInteger"));
        assertEquals("-", line(report, "MinLong"));
        assertEquals("-", line(report, "longestStringLength"));
        assertEquals("-", line(report, "shortestStringLength"));
        assertEquals("0", line(report, "IntegerWidth"));
        assertEquals("0", line(report, "LongWidth"));
        assertEquals(0, s.integerWidth());
        assertEquals(0, s.longWidth());
    }

    @Test
    void widthsAreTheWriterAllocation() {
        Stats s = new Stats();
        s.numInteger = 1; s.maxInteger = 5; s.minInteger = 5;
        assertEquals(4, s.integerWidth(), "sign bit + MinBits(5)");
        s.minInteger = -1;
        assertEquals(32, s.integerWidth(), "a negative int forces the full pattern");
        s.numLong = 1; s.maxLong = 100; s.minLong = 0;
        assertEquals(8, s.longWidth(), "sign bit + MinBits(100)");
        s.maxLong = 1L << 60;
        assertEquals(64, s.longWidth(), "58..63 round up to the aligned width");
        s.maxLong = 100; s.minLong = -100;
        assertEquals(64, s.longWidth(), "a negative long forces the full pattern");
        s.numStrings = 2; s.longestStringLength = 7; s.shortestStringLength = 3;
        s.numTripleTerms = 3;
        String report = s.toString();
        assertEquals("5", line(report, "MaxInteger"));
        assertEquals("-1", line(report, "MinInteger"));
        assertEquals("32", line(report, "IntegerWidth"));
        assertEquals("64", line(report, "LongWidth"));
        assertEquals("7", line(report, "longestStringLength"));
        assertEquals("3", line(report, "shortestStringLength"));
        assertEquals("3", line(report, "Number of Triple terms"));
        assertTrue(report.contains("MinLong               : -100"), report);
    }
}
