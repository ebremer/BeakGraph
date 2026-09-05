package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-418: the -src walk survives what real trees contain. A subdirectory
 * that cannot be listed (ACL-denied on Windows, 0000 on POSIX) is logged,
 * counted as unreadable and skipped - every other source still converts and
 * the summary still prints; Files.walk used to throw an UncheckedIOException
 * from the middle of the stream that traverse()/merge() did not catch, ending
 * the run with exit code 1. A junction that points back at an ancestor is cut
 * instead of recursed into until the path length fails.
 */
class SourceScanTest {

    @TempDir
    Path dir;

    private static final String TTL = "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n";
    private static final boolean WINDOWS = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");

    /** Makes {@code locked} unlistable; returns an undo action, or null when this platform cannot. */
    private static Runnable lock(Path locked) throws IOException {
        if (WINDOWS) {
            AclFileAttributeView view = Files.getFileAttributeView(locked, AclFileAttributeView.class);
            if (view == null) return null;
            List<AclEntry> original = new ArrayList<>(view.getAcl());
            UserPrincipal me;
            try {
                me = locked.getFileSystem().getUserPrincipalLookupService()
                        .lookupPrincipalByName(System.getProperty("user.name"));
            } catch (IOException | UnsupportedOperationException e) {
                me = view.getOwner();
            }
            AclEntry deny = AclEntry.newBuilder().setType(AclEntryType.DENY).setPrincipal(me)
                    .setPermissions(EnumSet.of(AclEntryPermission.READ_DATA, AclEntryPermission.LIST_DIRECTORY)).build();
            List<AclEntry> acl = new ArrayList<>(original);
            acl.add(0, deny);
            view.setAcl(acl);
            return () -> {
                try { view.setAcl(original); } catch (IOException e) { throw new RuntimeException(e); }
            };
        }
        try {
            Files.setPosixFilePermissions(locked, EnumSet.noneOf(PosixFilePermission.class));
        } catch (UnsupportedOperationException e) {
            return null;
        }
        return () -> {
            try {
                Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rwxr-xr-x"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static boolean listable(Path p) {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(p)) {
            ds.iterator().hasNext();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    void unreadableSubtreeIsSkippedNotFatal() throws Exception {
        Path src = Files.createDirectories(dir.resolve("src"));
        Files.writeString(src.resolve("good.ttl"), TTL, StandardCharsets.UTF_8);
        Files.writeString(src.resolve("zzz-late.ttl"), TTL, StandardCharsets.UTF_8);
        Path locked = Files.createDirectories(src.resolve("locked"));
        Files.writeString(locked.resolve("hidden.ttl"), TTL, StandardCharsets.UTF_8);
        Runnable undo = lock(locked);
        assumeTrue(undo != null, "no way to make a directory unlistable on this platform");
        try {
            assumeTrue(!listable(locked), "this account can still list a directory it was denied (elevated?)");
            FileCounter fc = new FileCounter();
            List<Path> sources = BeakGraphCLI.scanSources(src, fc);
            assertEquals(List.of(src.resolve("good.ttl"), src.resolve("zzz-late.ttl")), sources,
                    "every readable source, in order, none from the locked subtree");
            assertEquals(1, fc.getUnreadableCount(), "the locked directory is counted, not fatal");
            assertEquals(2, fc.getRDFFileCount());
            assertTrue(fc.toString().contains("Unreadable (skipped)   : 1"), fc.toString());

            // End to end: the run completes, converts what it can, and reports.
            Parameters p = new Parameters();
            p.src = src.toFile();
            p.dest = dir.resolve("out").toFile();
            BeakGraphCLI cli = new BeakGraphCLI(p);
            cli.traverse();
            assertEquals(2, cli.getFileCounter().getSuccessfulConversionCount());
            assertEquals(1, cli.getFileCounter().getUnreadableCount());
            assertTrue(Files.size(dir.resolve("out/good.h5")) > 0);
            assertTrue(Files.size(dir.resolve("out/zzz-late.h5")) > 0);
        } finally {
            undo.run();
        }
    }

    @Test
    void junctionOrLinkBackToAnAncestorIsCut() throws Exception {
        Path src = Files.createDirectories(dir.resolve("src2"));
        Files.writeString(src.resolve("good.ttl"), TTL, StandardCharsets.UTF_8);
        Path sub = Files.createDirectories(src.resolve("sub"));
        Path loop = sub.resolve("loop");
        if (WINDOWS) {
            // A directory junction needs no privilege; Files.walk descended into
            // it (junctions are not symbolic links to the JDK) until the path
            // length failed.
            Process mklink = new ProcessBuilder("cmd", "/c", "mklink", "/J", loop.toString(), src.toString())
                    .redirectErrorStream(true).start();
            mklink.getInputStream().readAllBytes();
            assumeTrue(mklink.waitFor() == 0 && Files.isDirectory(loop), "mklink /J unavailable");
        } else {
            try {
                Files.createSymbolicLink(loop, src);
            } catch (UnsupportedOperationException | IOException e) {
                assumeTrue(false, "symbolic links unavailable: " + e);
            }
        }
        FileCounter fc = new FileCounter();
        List<Path> sources = BeakGraphCLI.scanSources(src, fc);
        assertEquals(List.of(src.resolve("good.ttl")), sources, "each source once, the loop never entered");
        assertEquals(2, fc.getDirectoryCount(), "src and sub; the loop is not a directory of its own");
        if (WINDOWS) {
            Files.delete(loop);   // removes the junction, not the target
        }
    }
}
