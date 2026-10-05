package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.jhdf.HdfFile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-419: the native library receives the output path through JNI. A BMP
 * non-ASCII path must simply work; a supplementary-plane character (which
 * crosses as CESU-8) must either work too or be REPORTED - never silently
 * create a differently named file that the final move cannot find. The
 * writers probe the exact temp path before parsing.
 */
class NativeHdf5PathTest {

    @TempDir
    Path dir;

    @Test
    void bmpAccentedPathRoundTrips() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Path sub = Files.createDirectories(dir.resolve("Jos\u00E9 \u00DCml\u00E4ut"));
        Path h5 = sub.resolve("accent.h5");
        StreamingHdf5.probeFile(h5);
        assertTrue(Files.notExists(h5), "the probe removes its file");
        try (StreamingHdf5File f = NativeHdf5File.create(h5)) {
            f.rootGroup().putGroup(".BG").putAttribute("numQuads", 7L);
        }
        assertTrue(Files.exists(h5), "created under its own name");
        try (HdfFile hdf = new HdfFile(h5)) {
            assertEquals(7L, ((io.jhdf.api.Group) hdf.getChild(".BG")).getAttribute("numQuads").getData());
        }
    }

    @Test
    void supplementaryCharacterPathWorksOrIsReported() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        Path sub = Files.createDirectories(dir.resolve("emoji-\uD83D\uDE00"));
        Path h5 = sub.resolve("smile-\uD83D\uDC26.h5");
        try {
            StreamingHdf5.probeFile(h5);
            // Worked: the probe was created under its exact name and removed again.
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("cannot write a file in"), e.getMessage());
            assertTrue(String.valueOf(e.getCause().getMessage()).contains("different name")
                    || String.valueOf(e.getCause().getMessage()).contains("H5Fcreate failed"), String.valueOf(e.getCause()));
        }
        try (var entries = Files.list(sub)) {
            assertEquals(List.of(), entries.toList(), "no stray file under any spelling of the name");
        }
    }
}
