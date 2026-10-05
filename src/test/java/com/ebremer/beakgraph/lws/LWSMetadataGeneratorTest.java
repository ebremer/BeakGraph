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
    void reservedAndHiddenEntriesAreNotIndexedAndBothWalksAgree() throws Exception {
        Files.write(root.resolve("good.txt"), new byte[]{1});
        Files.write(root.resolve("description"), new byte[]{2});     // shadowed by the fixed /description route
        Files.write(root.resolve("sparql"), new byte[]{3});
        Files.write(Files.createDirectories(root.resolve("rdf")).resolve("in-rdf.txt"), new byte[]{4});
        Files.write(Files.createDirectories(root.resolve("HalcyonStorage")).resolve("aliased.txt"), new byte[]{5});
        Files.write(Files.createDirectories(root.resolve("HalcyonStorageArchive")).resolve("kept.txt"), new byte[]{6});
        Files.write(Files.createDirectories(root.resolve("sub")).resolve("description"), new byte[]{7}); // only the ROOT names are reserved
        Files.write(root.resolve(".hidden"), new byte[]{8});
        Files.write(Files.createDirectories(root.resolve(".git")).resolve("HEAD"), new byte[]{9});
        Path dosHidden = Files.write(root.resolve("attr-hidden.txt"), new byte[]{10});
        boolean dosHiddenSet = false;
        try {
            Files.setAttribute(dosHidden, "dos:hidden", true);
            dosHiddenSet = Files.isHidden(dosHidden);
        } catch (Exception notWindows) {
            // no DOS attributes here: the dot-name cases cover the rule
        }
        Model model = LWSMetadataGenerator.generateLWSModel(root);
        String base = LWSMetadataGenerator.CANONICAL_BASE;
        java.util.Set<String> items = model.getResource(base).listProperties(com.ebremer.ns.LWS.items)
                .mapWith(st -> st.getResource().getURI().substring(base.length() + 1)).toSet();
        assertTrue(items.contains("good.txt"), items.toString());
        assertTrue(items.contains("HalcyonStorageArchive"), "a name merely sharing the alias prefix is content: " + items);
        assertTrue(items.contains("sub"), items.toString());
        for (String reserved : LWSMetadataGenerator.RESERVED_ROOT_NAMES) {
            assertFalse(items.contains(reserved), reserved + " is shadowed by a fixed route: " + items);
        }
        assertFalse(items.contains(".hidden"), items.toString());
        assertFalse(items.contains(".git"), items.toString());
        if (dosHiddenSet) {
            assertFalse(items.contains("attr-hidden.txt"), "the platform's hidden attribute counts too: " + items);
        }
        assertFalse(model.containsResource(model.createResource(base + "/rdf/in-rdf.txt")), "a reserved directory's subtree is skipped");
        assertTrue(model.containsResource(model.createResource(base + "/sub/description")), "reserved names apply at the root only");
        assertEquals(LWSMetadataGenerator.treeSignature(root), LWSMetadataGenerator.modelSignature(model),
                "the signature walk must exclude exactly what the model walk excludes, or the refresher regenerates forever");
    }

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
