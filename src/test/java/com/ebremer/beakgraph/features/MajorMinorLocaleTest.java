package com.ebremer.beakgraph.features;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.ns.HAL;
import java.util.ArrayList;
import java.util.Locale;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.WKTReader;

/**
 * Regression test: the WKT literals MajorMinor generates must use '.' as the
 * decimal separator regardless of the JVM default locale. Under de_DE the old
 * String.format calls produced "POINT(5,0000 2,5000)" - invalid WKT.
 */
class MajorMinorLocaleTest {

    private Locale saved;

    @BeforeEach
    void forceCommaDecimalLocale() {
        saved = Locale.getDefault();
        Locale.setDefault(Locale.GERMANY);
    }

    @AfterEach
    void restoreLocale() {
        Locale.setDefault(saved);
    }

    @Test
    void wktUsesDotDecimalSeparatorUnderCommaLocale() throws Exception {
        ArrayList<Quad> quads = new ArrayList<>();
        MajorMinor.add(quads, NodeFactory.createURI("http://ex.org/f"),
            "POLYGON((0 0, 10 0, 10 5, 0 5, 0 0))");
        assertEquals(3, quads.size(), "centroid + major axis + minor axis expected");

        WKTReader reader = new WKTReader();
        String centroid = null;
        for (Quad q : quads) {
            String wkt = q.getObject().getLiteralLexicalForm();
            reader.read(wkt); // throws ParseException on locale-corrupted WKT
            if (q.getPredicate().equals(HAL.centroid.asNode())) centroid = wkt;
        }
        assertEquals("POINT(5.0000 2.5000)", centroid);
    }
}
