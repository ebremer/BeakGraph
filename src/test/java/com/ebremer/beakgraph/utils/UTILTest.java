package com.ebremer.beakgraph.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UTIL#isRelativeIRI(String)} - the scheme-based detection
 * that drives relative-IRI storage and resolution.
 */
class UTILTest {

    /** BG-324: the one byte-rounding rule (SPECIFICATIONS.md "Byte-rounded width"). */
    @Test
    void byteRoundedWidthFollowsTheSpecification() {
        assertEquals(8, UTIL.byteRoundedWidth(0));
        assertEquals(8, UTIL.byteRoundedWidth(1));
        assertEquals(8, UTIL.byteRoundedWidth(255));
        assertEquals(16, UTIL.byteRoundedWidth(256));
        assertEquals(16, UTIL.byteRoundedWidth(65535));
        assertEquals(24, UTIL.byteRoundedWidth(65536));
        assertEquals(40, UTIL.byteRoundedWidth(1L << 32));
        assertEquals(64, UTIL.byteRoundedWidth(Long.MAX_VALUE));
        for (long v : new long[]{2, 100, 1000, 1 << 20, 1L << 40}) {
            assertEquals((int) (Math.ceil(UTIL.MinBits(v) / 8.0) * 8), UTIL.byteRoundedWidth(v), "v=" + v);
        }
    }

    @Test
    void emptyStringIsRelative() {
        // the empty same-document reference <> is relative
        assertTrue(UTIL.isRelativeIRI(""));
    }

    @Test
    void relativeReferencesHaveNoScheme() {
        assertTrue(UTIL.isRelativeIRI("image.png"));
        assertTrue(UTIL.isRelativeIRI("sub/dir/file.ttl"));
        assertTrue(UTIL.isRelativeIRI("./sibling"));
        assertTrue(UTIL.isRelativeIRI("../parent"));
        assertTrue(UTIL.isRelativeIRI("#fragment"));
        assertTrue(UTIL.isRelativeIRI("9-cannot-start-a-scheme"));
        assertTrue(UTIL.isRelativeIRI("has space before colon:x"));
    }

    @Test
    void absoluteIrisHaveAScheme() {
        assertFalse(UTIL.isRelativeIRI("http://example.org/x"));
        assertFalse(UTIL.isRelativeIRI("https://example.org/x"));
        assertFalse(UTIL.isRelativeIRI("urn:isbn:0451450523"));
        assertFalse(UTIL.isRelativeIRI("mailto:a@b.example"));
        assertFalse(UTIL.isRelativeIRI("file:/C:/data/x.ttl"));
        assertFalse(UTIL.isRelativeIRI("HTTP://UPPERCASE/x"));   // scheme is case-insensitive
        assertFalse(UTIL.isRelativeIRI("a+b-c.d:rest"));         // all valid scheme characters
    }

    @Test
    void nullIsTreatedAsNotRelative() {
        assertFalse(UTIL.isRelativeIRI(null));
    }
}
