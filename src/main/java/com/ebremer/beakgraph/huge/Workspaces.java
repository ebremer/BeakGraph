package com.ebremer.beakgraph.huge;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/**
 * Build-workspace housekeeping shared by the disk-based writers.
 *
 * @author Erich Bremer
 */
public final class Workspaces {

    private Workspaces() {}

    /**
     * Stops {@code pool} and WAITS for its tasks before the caller touches the
     * workspace: a spill or merge still running after a failed build kept its
     * run file open (undeletable on Windows) and outlived {@code write()}
     * (BG-134, BG-110).
     */
    public static void drain(ExecutorService pool, Logger log) {
        pool.shutdownNow();
        boolean interrupted = false;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                if (pool.awaitTermination(1, TimeUnit.MINUTES)) {
                    break;
                }
                log.warn("Background sort/spill tasks still running {} minute(s) after the build ended", attempt + 1);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Removes a workspace tree, tolerating what an aborted build can leave: an
     * entry that cannot be deleted or read is logged and skipped instead of
     * aborting the walk and stranding the whole directory (BG-134).
     */
    public static void deleteTree(Path dir, Logger log) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try {
                        Files.deleteIfExists(file);
                    } catch (IOException e) {
                        log.warn("Could not delete {}: {}", file, e.toString());
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    log.warn("Could not read {} while removing the workspace: {}", file, exc.toString());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path d, IOException exc) {
                    try {
                        Files.deleteIfExists(d);
                    } catch (IOException e) {
                        log.warn("Could not delete {}: {}", d, e.toString());
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.warn("Failed to remove workspace {}", dir, e);
        }
    }
}
