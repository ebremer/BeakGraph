package com.ebremer.beakgraph.lws;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LWSMetadataGeneratorTest {

    @TempDir
    Path root;

    @Test
    void regenerationDoesNotIndexTheMetadataCacheItself() throws Exception {
        Files.write(root.resolve("data.h5"), new byte[]{1, 2, 3});
        // A previous generation's cache is on disk - the regeneration walk must
        // not turn it into a listed DataResource: the raw model carries the
        // owl:sameAs file:/// server paths the servlet redacts from clients.
        Files.write(root.resolve(LWSMetadataGenerator.CACHE_FILE_NAME),
                "stale cache".getBytes(StandardCharsets.UTF_8));

        Model model = LWSMetadataGenerator.generateLWSModel(root);

        String base = LWSMetadataGenerator.CANONICAL_BASE;
        assertTrue(model.containsResource(model.createResource(base + "/data.h5")),
                "real files must be indexed");
        assertFalse(model.containsResource(
                        model.createResource(base + "/" + LWSMetadataGenerator.CACHE_FILE_NAME)),
                "the metadata cache file must never index itself");
    }
}
