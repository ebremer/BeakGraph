package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.beust.jcommander.JCommander;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * BG-358: {@code -export} is case-insensitive on every JVM. Under a Turkish
 * default locale a bare {@code toUpperCase()} turned "trig" into "TRİG"
 * (dotted capital I), which matched no format and was rejected at argument
 * parsing; the fold now uses Locale.ROOT.
 */
class ExportFormatLocaleTest {

    @Test
    void trigIsAcceptedUnderTheTurkishLocale() {
        Locale saved = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertEquals("TRİG", "trig".toUpperCase(), "the premise: the default locale folds i to dotted I");
            assertEquals("TRIG", ExportFormatValidator.normalize("trig"));
            assertEquals("TRIG", ExportFormatValidator.normalize("Trig"));
            assertEquals("TRIG", ExportFormatValidator.normalize(" tRiG "));
            assertEquals("JSONLD", ExportFormatValidator.normalize("json-ld"));
            assertNull(ExportFormatValidator.normalize("xml"));
            Parameters p = new Parameters();
            JCommander.newBuilder().addObject(p).build().parse("-src", "x.h5", "-export", "trig");
            assertEquals("trig", p.export);
            assertEquals("TRIG", ExportFormatValidator.normalize(p.export));
        } finally {
            Locale.setDefault(saved);
        }
    }
}
