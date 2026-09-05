package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * BG-270 / BG-411: {@code -help} drifted from the code (method 5 missing
 * from the -cores and -workdir texts, flags with no description at all, a
 * tautological -endpoint line). The usage text is generated from
 * {@link Parameters}, so pin what it must say about the six engines and
 * require every option to explain itself.
 */
class HelpTextDriftTest {

    private static String usage() {
        StringBuilder sb = new StringBuilder();
        JCommander.newBuilder().addObject(new Parameters()).build().getUsageFormatter().usage(sb);
        return sb.toString();
    }

    /** JCommander builds its descriptions lazily, on parse (or usage). */
    private static java.util.List<ParameterDescription> parameters() {
        JCommander jc = JCommander.newBuilder().addObject(new Parameters()).build();
        jc.parse();
        return jc.getParameters();
    }

    private static String description(String option) {
        for (ParameterDescription pd : parameters()) {
            for (String name : pd.getNames().split(",\\s*")) {
                if (name.equals(option)) return pd.getDescription();
            }
        }
        throw new AssertionError("no such option: " + option);
    }

    @Test
    void everyEngineIsDescribed() {
        String method = description("-method");
        for (int m = 0; m <= 5; m++) {
            assertTrue(method.contains(m + " = "), "-method help must describe engine " + m + ": " + method);
        }
        // MethodValidator's range and the help text agree.
        MethodValidator v = new MethodValidator();
        for (int m = 0; m <= 5; m++) v.validate("-method", Integer.toString(m));
        assertTrue(usage().contains("-method"));
    }

    @Test
    void coresAndWorkdirNameTheEnginesThatUseThem() {
        String cores = description("-cores");
        assertTrue(cores.contains("2/3/4/5"), "-cores applies to methods 2, 3, 4 and 5: " + cores);
        String workdir = description("-workdir");
        assertTrue(workdir.contains("1/4/5"), "-workdir applies to the disk-based methods 1, 4 and 5: " + workdir);
        String endpoint = description("-endpoint");
        assertTrue(endpoint.contains("directory") && endpoint.contains("LWS") && endpoint.contains("beakgraph.ttl.gz"),
                "-endpoint must describe directory (LWS) mode and its cache file: " + endpoint);
    }

    /** BG-288 / BG-319: the defaults and rules INSTRUCTIONS.md states are in the usage text too. */
    @Test
    void defaultsAndRulesAreInTheUsageText() {
        assertTrue(description("-port").contains("8888"), "-port names its default: " + description("-port"));
        assertTrue(description("-dest").contains("merged.h5"), "-dest explains the -merge rule: " + description("-dest"));
        assertTrue(description("-force").contains("rebuild"), description("-force"));
        assertTrue(description("-src").contains("-export"), "-src covers export mode: " + description("-src"));
        assertTrue(description("-cores").contains("2/3/4/5"), description("-cores"));
        assertTrue(description("-verify").contains(".hdf5"), description("-verify"));
    }

    @Test
    void everyOptionHasADescription() {
        List<String> undocumented = new ArrayList<>();
        assertTrue(parameters().size() > 20, "every option is described: " + parameters().size());
        for (ParameterDescription pd : parameters()) {
            if (pd.getDescription() == null || pd.getDescription().isBlank() || pd.getDescription().equals("# of Threads")) {
                undocumented.add(pd.getNames());
            }
        }
        assertEquals(List.of(), undocumented, "options without a real description");
    }
}
