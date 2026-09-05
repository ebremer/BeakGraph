package com.ebremer.beakgraph.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.riot.Lang;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

/**
 * BG-425: JSON-LD is the one accepted syntax Jena cannot stream (Titanium
 * loads and expands the whole document), so the disk-based engines' bounded
 * RAM promise does not cover it; RdfSources knows which syntaxes stream, maps
 * a file name to its syntax the way {@link RdfSources#open} will, and warns
 * loudly for a materialized source.
 */
class RdfSourcesStreamingTest {

    @Test
    void onlyJsonLdIsMaterialized() {
        for (Lang streaming : List.of(Lang.TURTLE, Lang.NTRIPLES, Lang.NQUADS, Lang.TRIG, Lang.RDFXML)) {
            assertTrue(RdfSources.isStreaming(streaming), streaming.getName());
        }
        assertFalse(RdfSources.isStreaming(Lang.JSONLD));
        assertTrue(RdfSources.isStreaming(null));
    }

    @Test
    void langOfFollowsTheOpenRules() {
        assertEquals(Lang.JSONLD, RdfSources.langOf(new File("data.jsonld")));
        assertEquals(Lang.JSONLD, RdfSources.langOf(new File("data.JSONLD.GZ")));
        assertEquals(Lang.JSONLD, RdfSources.langOf(new File("data.jsonld.zip")));
        assertEquals(Lang.NQUADS, RdfSources.langOf(new File("data.nq.gz")));
        assertEquals(Lang.TURTLE, RdfSources.langOf(new File("data.ttl")));
        assertEquals(Lang.TURTLE, RdfSources.langOf(new File("data.unknown")), "the quad store's fallback is Turtle");
    }

    private static Logger recording(List<String> sink) {
        return (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class<?>[]{Logger.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("warn") && args != null && args.length > 0) {
                        StringBuilder sb = new StringBuilder(String.valueOf(args[0]));
                        for (int i = 1; i < args.length; i++) {
                            if (args[i] instanceof Object[] arr) {
                                for (Object o : arr) sb.append(' ').append(o);
                            } else {
                                sb.append(' ').append(args[i]);
                            }
                        }
                        sink.add(sb.toString());
                        return null;
                    }
                    if (method.getReturnType() == boolean.class) return false;
                    if (method.getReturnType() == String.class) return "recording";
                    return null;
                });
    }

    @Test
    void materializedSourcesAreWarnedAbout() {
        List<String> warnings = new ArrayList<>();
        Logger log = recording(warnings);
        RdfSources.warnIfMaterialized(new File("huge.nq.gz"), Lang.NQUADS, log);
        assertTrue(warnings.isEmpty(), "streaming syntaxes are silent");
        RdfSources.warnIfMaterialized(new File("huge.jsonld.gz"), Lang.JSONLD, log);
        assertEquals(1, warnings.size());
        String w = warnings.get(0);
        assertTrue(w.contains("no streaming parser"), w);
        assertTrue(w.contains("huge.jsonld.gz"), w);
        assertTrue(w.contains("NOT bounded"), w);
        assertTrue(w.contains("riot --output=nq"), w);
        assertTrue(w.contains("compressed"), w);
    }
}
