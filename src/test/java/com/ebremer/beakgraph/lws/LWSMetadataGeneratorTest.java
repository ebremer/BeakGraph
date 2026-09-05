package com.ebremer.beakgraph.lws;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    void unreadableEntriesAreSkippedNotFatal() throws Exception {
        // BG-39: the walk's failure hooks continue instead of rethrowing.
        LWSMetadataGenerator.LenientVisitor v = new LWSMetadataGenerator.LenientVisitor();
        assertEquals(java.nio.file.FileVisitResult.CONTINUE,
                v.visitFileFailed(root.resolve("locked"), new java.nio.file.AccessDeniedException("locked")));
        assertEquals(java.nio.file.FileVisitResult.CONTINUE,
                v.postVisitDirectory(root.resolve("dir"), new java.io.IOException("iteration failed")));
        assertEquals(java.nio.file.FileVisitResult.CONTINUE, v.postVisitDirectory(root.resolve("dir"), null));

        // The real thing where the platform can express it: a directory nobody may read.
        Files.write(root.resolve("a.h5"), new byte[]{1});
        Path priv = Files.createDirectories(root.resolve("private"));
        Files.write(priv.resolve("secret.h5"), new byte[]{2});
        boolean posix = Files.getFileStore(root).supportsFileAttributeView(java.nio.file.attribute.PosixFileAttributeView.class);
        org.junit.jupiter.api.Assumptions.assumeTrue(posix, "needs POSIX permissions to make a directory unreadable");
        java.util.Set<java.nio.file.attribute.PosixFilePermission> original = Files.getPosixFilePermissions(priv);
        Files.setPosixFilePermissions(priv, java.util.EnumSet.noneOf(java.nio.file.attribute.PosixFilePermission.class));
        try {
            Model model = LWSMetadataGenerator.generateLWSModel(root);
            assertTrue(model.containsResource(model.createResource(LWSMetadataGenerator.CANONICAL_BASE + "/a.h5")),
                    "readable entries must still be indexed");
            assertFalse(LWSMetadataGenerator.treeSignature(root).isEmpty(), "the signature walk tolerates it too");
        } finally {
            Files.setPosixFilePermissions(priv, original);
        }
    }

    @Test
    void cacheIsWrittenThroughATempFileAndLeavesNoneBehind() throws Exception {
        Files.write(root.resolve("data.h5"), new byte[]{1, 2, 3});
        Model model = LWSMetadataGenerator.generateLWSModel(root);
        Path cache = root.resolve(LWSMetadataGenerator.CACHE_FILE_NAME);
        LWSMetadataGenerator.writeModelToGZ(model, cache);
        assertTrue(Files.size(cache) > 0);
        assertFalse(Files.exists(root.resolve(LWSMetadataGenerator.CACHE_FILE_NAME + ".tmp")));
        try (java.io.InputStream in = new java.util.zip.GZIPInputStream(Files.newInputStream(cache))) {
            Model back = org.apache.jena.rdf.model.ModelFactory.createDefaultModel();
            org.apache.jena.riot.RDFDataMgr.read(back, in, org.apache.jena.riot.Lang.TURTLE);
            assertTrue(back.isIsomorphicWith(model));
        }
    }
}
