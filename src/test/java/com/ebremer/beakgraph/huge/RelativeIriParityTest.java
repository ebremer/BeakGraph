package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-432: fragment, query, child, parent, path-absolute and triple-term-nested
 * relative references store identically through the RAM and the disk engine
 * (both now delegate to RelativeIris).
 */
class RelativeIriParityTest {

    @TempDir
    Path dir;

    private static TreeSet<String> triples(File h5) throws Exception {
        TreeSet<String> out = new TreeSet<>();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            bg.find().forEachRemaining(t -> out.add(t.toString()));
        }
        return out;
    }

    @Test
    void relativeReferencesStoreIdenticallyOnBothEngines() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        String ttl = "@prefix : <http://ex.org/> .\n"
                + "<> :p <x.png>, <#frag>, <?q=1>, <../up.png>, <../../up2.png>, </root.png>, <a/b.png> .\n"
                + "<> :tt <<( <> :q <#f> )>> .\n";
        File src = dir.resolve("rel.ttl").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        File ram = dir.resolve("rel.ram.h5").toFile();
        File huge = dir.resolve("rel.huge.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(ram).setSpatial(false).setFeatures(false).build().write();
        HugeHDF5Writer.Builder().setSource(src).setDestination(huge).setSpatial(false).setFeatures(false)
                .setWorkDirectory(Files.createDirectories(dir.resolve("work"))).build().write();
        TreeSet<String> a = triples(ram);
        TreeSet<String> b = triples(huge);
        assertEquals(a, b);
        String all = String.join("\n", a);
        for (String form : new String[]{"x.png", "#frag", "?q=1", "../up.png", "../../up2.png", "/root.png", "a/b.png"}) {
            assertTrue(all.contains(form), "stored form " + form + " in\n" + all);
        }
        assertTrue(a.stream().noneMatch(t -> t.contains("beakgraph.invalid")), "no sentinel survives, even inside the triple term");
    }
}
