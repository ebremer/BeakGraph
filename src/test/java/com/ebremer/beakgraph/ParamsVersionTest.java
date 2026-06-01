package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ParamsVersionTest {

    @Test
    void versionIsResolvedFromTheBuildNotAPlaceholder() {
        // Params.VERSION is filled from beakgraph-version.properties, which Maven filters
        // with ${project.version}. Guard against filtering being lost (raw placeholder)
        // or the resource being absent (the "unknown" fallback).
        String v = Params.VERSION;
        assertNotNull(v);
        assertFalse(v.startsWith("${"), "version must be filtered, not the raw placeholder: " + v);
        assertNotEquals("unknown", v, "the version resource must be on the classpath");
        assertTrue(v.matches("\\d+\\.\\d+.*"), "should look like a version: " + v);
    }
}
