package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import hdf.hdf5lib.H5;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-438: the disk-based writers loaded the HDF5 JNI
 * bindings through the stock {@code H5.loadH5Lib()}, which resolves
 * {@code hdf5_java} from {@code java.library.path} only. JavaCPP's bundled
 * natives are extracted solely by {@code org.bytedeco.javacpp.Loader}, which
 * nothing called, so the ~40 MB of natives in the jar were dead weight and
 * -method 1/4/5 silently required a system HDF5 install.
 * {@link NativeHdf5File#loadNatives()} now bootstraps the bundled natives
 * when no system install is on the library path.
 * <p>
 * The proof needs a JVM with no system HDF5 reachable, so a child JVM is
 * started with every PATH / LD_LIBRARY_PATH / DYLD_LIBRARY_PATH entry that
 * holds an HDF5 library removed and an empty {@code java.library.path}.
 */
@Timeout(180)
class NativeBootstrapTest {

    @TempDir
    Path dir;

    /** Child-JVM entry point: reports what {@link NativeHdf5File} found. */
    public static final class Probe {
        public static void main(String[] args) {
            boolean available = NativeHdf5File.isAvailable();
            System.out.println("AVAILABLE=" + available);
            System.out.println("SOURCE=" + NativeHdf5File.nativeSource());
            if (available) {
                int[] v = new int[3];
                H5.H5get_libversion(v);
                System.out.println("VERSION=" + v[0] + "." + v[1] + "." + v[2]);
            } else {
                System.out.println("CAUSE=" + NativeHdf5File.getUnavailableCause());
            }
        }
    }

    private static boolean holdsHdf5(String dirName) {
        File d = new File(dirName);
        if (!d.isDirectory()) return false;
        String[] names = d.list();
        if (names == null) return false;
        for (String n : names) {
            String l = n.toLowerCase();
            if (l.contains("hdf5") && (l.endsWith(".dll") || l.contains(".so") || l.endsWith(".dylib"))) return true;
        }
        return false;
    }

    private static String stripHdf5(String pathValue) {
        List<String> keep = new ArrayList<>();
        for (String entry : pathValue.split(java.util.regex.Pattern.quote(File.pathSeparator))) {
            if (!holdsHdf5(entry)) keep.add(entry);
        }
        return String.join(File.pathSeparator, keep);
    }

    private Map<String, String> runProbeWithoutSystemHdf5() throws Exception {
        Path emptyLibDir = Files.createDirectories(dir.resolve("nolibs"));
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder pb = new ProcessBuilder(java,
                "--enable-native-access=ALL-UNNAMED",
                "-Djava.library.path=" + emptyLibDir,
                "-cp", System.getProperty("java.class.path"),
                Probe.class.getName());
        Map<String, String> env = pb.environment();
        for (String key : new ArrayList<>(env.keySet())) {
            String k = key.toUpperCase();
            if (k.equals("PATH")) {
                env.put(key, stripHdf5(env.get(key)));
            } else if (k.equals("LD_LIBRARY_PATH") || k.equals("DYLD_LIBRARY_PATH") || k.equals("DYLD_FALLBACK_LIBRARY_PATH")
                    || k.startsWith("HDF5")) {
                env.remove(key);
            }
        }
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(150, TimeUnit.SECONDS), "probe JVM did not finish:\n" + out);
        Map<String, String> report = new HashMap<>();
        for (String line : out.split("\\R")) {
            int eq = line.indexOf('=');
            if (line.startsWith("AVAILABLE=") || line.startsWith("SOURCE=") || line.startsWith("VERSION=") || line.startsWith("CAUSE=")) {
                report.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }
        assertTrue(report.containsKey("AVAILABLE"), "probe produced no report:\n" + out);
        report.put("OUTPUT", out);
        return report;
    }

    @Test
    void bundledNativesLoadWithoutASystemInstall() throws Exception {
        Map<String, String> r = runProbeWithoutSystemHdf5();
        boolean windows = System.getProperty("os.name", "").toLowerCase().contains("win");
        if (windows) {
            // The preset's Windows jar ships hdf5_java.dll importing a hdf5.dll it
            // does not carry: the bundled natives cannot load, and the failure
            // must say so rather than pretend a system install is the only way.
            assertEquals("false", r.get("AVAILABLE"), r.get("OUTPUT"));
            assertTrue(r.get("SOURCE").startsWith("java.library.path (bundled natives not usable here"), r.get("OUTPUT"));
            assertTrue(r.get("SOURCE").contains("UnsatisfiedLinkError"), r.get("OUTPUT"));
        } else {
            assertEquals("true", r.get("AVAILABLE"), r.get("OUTPUT"));
            assertTrue(r.get("SOURCE").startsWith("bundled JavaCPP natives"), r.get("OUTPUT"));
            assertEquals("1.14.3", r.get("VERSION"), "the bundled preset's HDF5, not a system one: " + r.get("OUTPUT"));
        }
    }

    @Test
    void inProcessLoadReportsItsSource() {
        assumeTrue(NativeHdf5File.isAvailable(), "no native HDF5 in this JVM: " + NativeHdf5File.getUnavailableCause());
        String source = NativeHdf5File.nativeSource();
        assertNotNull(source);
        assertFalse(source.isBlank());
        // A system install on the library path is preferred over the bundle.
        String file = System.mapLibraryName("hdf5_java");
        boolean onPath = false;
        for (String d : System.getProperty("java.library.path", "").split(java.util.regex.Pattern.quote(File.pathSeparator))) {
            onPath |= !d.isEmpty() && new File(d, file).isFile();
        }
        if (onPath) {
            assertTrue(source.startsWith("system install"), source);
        }
    }
}
