package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import me.tongfei.progressbar.ProgressBarStyle;
import org.junit.jupiter.api.Test;

/**
 * BG-278 / BG-155: the entry point never exits 0 having done nothing - a bare
 * invocation, a parseable but mode-less one, an option without its -src, an
 * out-of-range -threads or -port all print why and return 1. BG-361: the OS
 * fold for the progress-bar style does not depend on the default locale.
 */
class CliEntryPointTest {

    private record Run(int code, String out, String err) {}

    private static Run run(String... args) throws Exception {
        PrintStream out0 = System.out, err0 = System.err;
        ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
            int code = BeakGraphCLI.run(args);
            return new Run(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
        } finally {
            System.setOut(out0);
            System.setErr(err0);
        }
    }

    @Test
    void bareInvocationPrintsUsageAndFails() throws Exception {
        Run r = run();
        assertEquals(1, r.code());
        assertTrue(r.err().contains("no arguments"), r.err());
        assertTrue(r.out().contains("-src") && r.out().contains("-verify"), "usage on stdout: " + r.out());
    }

    @Test
    void modeLessInvocationFails() throws Exception {
        Run r = run("-threads", "4");
        assertEquals(1, r.code());
        assertTrue(r.err().contains("no operation given"), r.err());
        Run cores = run("-cores", "2", "-status");
        assertEquals(1, cores.code());
    }

    @Test
    void optionsWithoutTheirSourceFail() throws Exception {
        Run export = run("-export", "NT");
        assertEquals(1, export.code());
        assertTrue(export.err().contains("-src is required"), export.err());
        Run merge = run("-merge", "-dest", "somewhere");
        assertEquals(1, merge.code());
        assertTrue(merge.err().contains("-src is required"), merge.err());
    }

    @Test
    void rangeValidatorsRejectBadValues() throws Exception {
        Run threads = run("-threads", "0", "-src", "x", "-dest", "y");
        assertEquals(1, threads.code());
        assertTrue(threads.err().contains("-threads"), threads.err());
        Run port = run("-port", "70000", "-endpoint", "x");
        assertEquals(1, port.code());
        assertTrue(port.err().contains("TCP port"), port.err());
        Run negative = run("-port", "-1", "-endpoint", "x");
        assertEquals(1, negative.code());
        Run text = run("-port", "http", "-endpoint", "x");
        assertEquals(1, text.code());
    }

    @Test
    void versionAndHelpSucceed() throws Exception {
        Run v = run("-version");
        assertEquals(0, v.code());
        assertTrue(v.out().contains("Version"), v.out());
        Run h = run("-help");
        assertEquals(0, h.code());
        assertTrue(h.out().contains("-verify"), h.out());
    }

    @Test
    void missingPathsFail() throws Exception {
        Run src = run("-src", "no-such-dir-here", "-dest", "out");
        assertEquals(1, src.code());
        assertTrue(src.err().contains("does not exist"), src.err());
        Run verify = run("-verify", "no-such-file.h5");
        assertEquals(1, verify.code());
    }

    @Test
    void progressBarStyleDoesNotDependOnTheLocale() {
        Locale saved = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertEquals("wındows 11", "WINDOWS 11".toLowerCase(), "the premise: the default locale folds I to dotless i");
            assertEquals(ProgressBarStyle.ASCII, BeakGraphCLI.styleFor("Windows 11"));
            assertEquals(ProgressBarStyle.ASCII, BeakGraphCLI.styleFor("WINDOWS SERVER 2022"));
            assertEquals(ProgressBarStyle.COLORFUL_UNICODE_BLOCK, BeakGraphCLI.styleFor("Linux"));
            assertEquals(ProgressBarStyle.COLORFUL_UNICODE_BLOCK, BeakGraphCLI.styleFor(null));
        } finally {
            Locale.setDefault(saved);
        }
    }
}
