package com.ebremer.beakgraph.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-448: datasets over the FFM threshold used to be mapped by RE-OPENING
 * the file path, while index datasets are mapped lazily at first use. A
 * store rebuilt in place between open and first query therefore mapped the
 * NEW file's bytes under the OLD file's metadata (address, numEntries,
 * width) - wrong ids, or an out-of-range map. The mapping now goes through
 * jHDF's own open channel, i.e. the file the metadata describes.
 * <p>
 * The witness works on every platform without replacing the file: once the
 * reader is open, the path is made unreadable (an ACL deny on Windows, mode
 * 000 elsewhere). The already-open channel is unaffected - access is checked
 * at open time - so a lazily mapped index only works if it is mapped through
 * that channel; a fresh open by path fails.
 */
class FfmMappingConsistencyTest {

    private static final boolean WINDOWS = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");

    // NEVER + manual delete: auto-arena mappings unmap at GC, which on Windows
    // can outlive JUnit's cleanup and fail the run over a locked temp file.
    @TempDir(cleanup = CleanupMode.NEVER)
    static Path dir;

    @AfterAll
    static void tryCleanup() {
        try (var files = Files.walk(dir)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                p.toFile().deleteOnExit();
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        } catch (Exception ignored) {}
    }

    /** Makes {@code file} unopenable for reading; returns an undo action, or null when this platform cannot. */
    private static Runnable lock(Path file) throws IOException {
        if (WINDOWS) {
            AclFileAttributeView view = Files.getFileAttributeView(file, AclFileAttributeView.class);
            if (view == null) return null;
            List<AclEntry> original = new ArrayList<>(view.getAcl());
            UserPrincipal me;
            try {
                me = file.getFileSystem().getUserPrincipalLookupService()
                        .lookupPrincipalByName(System.getProperty("user.name"));
            } catch (IOException | UnsupportedOperationException e) {
                me = view.getOwner();
            }
            AclEntry deny = AclEntry.newBuilder().setType(AclEntryType.DENY).setPrincipal(me)
                    .setPermissions(EnumSet.of(AclEntryPermission.READ_DATA)).build();
            List<AclEntry> acl = new ArrayList<>(original);
            acl.add(0, deny);
            view.setAcl(acl);
            return () -> {
                try { view.setAcl(original); } catch (IOException e) { throw new RuntimeException(e); }
            };
        }
        try {
            Files.setPosixFilePermissions(file, EnumSet.noneOf(PosixFilePermission.class));
        } catch (UnsupportedOperationException e) {
            return null;
        }
        return () -> {
            try {
                Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static boolean openableByPath(Path file) {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static int count(BeakGraph bg, String query) {
        try (QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void lazilyMappedIndexesComeFromTheOpenChannelNotFromThePath() throws Exception {
        File src = dir.resolve("ffm.trig").toFile();
        File h5 = dir.resolve("ffm.trig.h5").toFile();
        Files.write(src.toPath(), """
            @prefix ex: <http://ex.org/> .
            ex:s0 ex:p0 ex:o0 . ex:s1 ex:p0 ex:o0 . ex:s2 ex:p1 ex:o1 . ex:s2 ex:name "two" .
            ex:g1 { ex:s3 ex:p0 ex:o0 . ex:s3 ex:p1 "x" . }
            """.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();

        long previous = DatasetBytes.setFfmThreshold(0); // every dataset takes the FFM path
        Runnable undo = null;
        BeakGraph bg = null;
        try {
            bg = new BeakGraph(new HDF5Reader(h5)); // the dictionary is mapped now; the indexes at first use
            undo = lock(h5.toPath());
            assumeTrue(undo != null, "this platform cannot make the file unreadable");
            assumeTrue(!openableByPath(h5.toPath()), "the premise: a fresh open by path must fail here");
            // Both indexes are touched for the first time with the path unreadable.
            assertEquals(4, count(bg, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"), "GSPO, mapped through the open channel");
            assertEquals(2, count(bg, "SELECT ?s WHERE { ?s <http://ex.org/p0> <http://ex.org/o0> }"), "GPOS, mapped through the open channel");
            assertEquals(2, count(bg, "SELECT ?s ?p ?o WHERE { GRAPH <http://ex.org/g1> { ?s ?p ?o } }"));
        } finally {
            if (undo != null) undo.run();
            DatasetBytes.setFfmThreshold(previous);
            if (bg != null) bg.close();
        }
    }
}
