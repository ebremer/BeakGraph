package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * BG-365: the repository rule for locale handling, enforced by the build.
 * Case mapping of protocol strings, file names and format names, and every
 * {@code String.format} that prints a number, must pass {@code Locale.ROOT}:
 * under tr-TR {@code "trig".toUpperCase()} is {@code "TRİG"}, and under ar-EG
 * {@code %d} prints Arabic-Indic digits - into stored identifiers, once. The
 * rule was applied ad hoc (five sites had it, fourteen did not) and nothing
 * stopped the next copy-paste regression; this scan does. First-party sources
 * only: the vendored zstd codec under {@code io/} keeps its upstream text.
 */
class LocaleRuleTest {

    private static final Path ROOT = Path.of("src/main/java/com/ebremer");
    private static final Pattern DEFAULT_LOCALE_CASE = Pattern.compile("\\.to(Lower|Upper)Case\\(\\)");
    private static final Pattern FORMAT_CALL = Pattern.compile("String\\.format\\(");
    private static final Pattern LOCALIZED_CONVERSION = Pattern.compile("%[-#+ 0,(]*\\d*(\\.\\d+)?[deEfgG]");

    private static List<Path> sources() throws IOException {
        try (Stream<Path> s = Files.walk(ROOT)) {
            return s.filter(p -> p.toString().endsWith(".java")).toList();
        }
    }

    @Test
    void noDefaultLocaleCaseMapping() throws IOException {
        List<String> offenders = new ArrayList<>();
        for (Path p : sources()) {
            List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i).strip();
                if (line.startsWith("//") || line.startsWith("*") || line.startsWith("/*")) continue;   // prose
                if (DEFAULT_LOCALE_CASE.matcher(lines.get(i)).find() && !lines.get(i).contains("// locale-ok")) {
                    offenders.add(p + ":" + (i + 1) + ": " + lines.get(i).trim());
                }
            }
        }
        assertEquals(List.of(), offenders, "toLowerCase()/toUpperCase() without Locale.ROOT (README \"Design decisions\")");
    }

    @Test
    void numberFormatsPassLocaleRoot() throws IOException {
        List<String> offenders = new ArrayList<>();
        for (Path p : sources()) {
            String text = Files.readString(p, StandardCharsets.UTF_8);
            Matcher m = FORMAT_CALL.matcher(text);
            while (m.find()) {
                int start = m.end();
                int lineStart = text.lastIndexOf('\n', m.start()) + 1;
                String prefix = text.substring(lineStart, m.start()).strip();
                if (prefix.startsWith("//") || prefix.startsWith("*") || prefix.startsWith("/*")) continue;   // prose
                // The format literal: a text block or a plain string starting the argument list.
                int q = text.indexOf('"', start);
                if (q < 0) continue;
                String head = text.substring(start, q);
                String literal;
                if (text.startsWith("\"\"\"", q)) {
                    int end = text.indexOf("\"\"\"", q + 3);
                    literal = text.substring(q + 3, end < 0 ? text.length() : end);
                } else {
                    int end = q + 1;
                    while (end < text.length() && text.charAt(end) != '"') {
                        if (text.charAt(end) == '\\') end++;
                        end++;
                    }
                    literal = text.substring(q + 1, Math.min(end, text.length()));
                }
                if (LOCALIZED_CONVERSION.matcher(literal).find() && !head.contains("Locale.ROOT")) {
                    int line = 1 + (int) text.substring(0, m.start()).chars().filter(c -> c == '\n').count();
                    offenders.add(p + ":" + line + ": String.format with a localized conversion and no Locale.ROOT: " + literal.strip().split("\n")[0]);
                }
            }
        }
        assertEquals(List.of(), offenders);
    }

    @Test
    void theScanSeesTheSources() throws IOException {
        assertTrue(sources().size() > 100, "the rule must scan the real source tree, found " + sources().size());
    }
}
