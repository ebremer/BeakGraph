package com.ebremer.beakgraph.sniff;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableGroup;
import io.jhdf.api.WritableNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

public class DirectoryToHdf5Writer {

    private static final Map<String, String> MIME_TYPES = new HashMap<>();
    static {
        MIME_TYPES.put("jpg", "image/jpeg");
        MIME_TYPES.put("jpeg", "image/jpeg");
        MIME_TYPES.put("png", "image/png");
        MIME_TYPES.put("h5", "application/x-hdf5");
        MIME_TYPES.put("hdf5", "application/x-hdf5");
        MIME_TYPES.put("txt", "text/plain");
        MIME_TYPES.put("json", "application/json");
        MIME_TYPES.put("xml", "application/xml");
        MIME_TYPES.put("mp3", "audio/mpeg");
    }

    public static void main(String[] args) {
        File hdf5File = new File("/beakgraph/dXX.h5");
        Path sourcePath = Path.of("/path/to/your/source/data");
        System.out.println("Creating HDF5 file at: " + hdf5File.getAbsolutePath());
        System.out.println("Importing from: " + sourcePath);
        try {
            // Create a new HDF5 file (overwrites if exists)
            try (WritableHdfFile writableHdf = HdfFile.write(hdf5File.toPath())) {
                processDirectory(writableHdf, sourcePath);
            }
            System.out.println("Successfully wrote HDF5 file with lws:mimetype attributes.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processDirectory(WritableGroup rootGroup, Path startPath) throws IOException {
        Files.walkFileTree(startPath, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path relative = startPath.relativize(dir);
                if (relative.toString().isEmpty()) {
                    return FileVisitResult.CONTINUE;
                }
                System.out.println("Creating Group: " + relative);
                WritableGroup parent = resolveGroup(rootGroup, relative.getParent());
                parent.putGroup(relative.getFileName().toString());

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path relative = startPath.relativize(file);
                String fileName = relative.getFileName().toString();
                System.out.println("Writing File: " + relative + " (" + attrs.size() + " bytes)");
                WritableGroup parent = resolveGroup(rootGroup, relative.getParent());
                byte[] data = Files.readAllBytes(file);
                WritableNode dataset = parent.putDataset(fileName, data);
                String mimeType = getMimeType(fileName);
                dataset.putAttribute("lws:mimetype", mimeType);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static String getMimeType(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0 && dotIndex < fileName.length() - 1) {
            String ext = fileName.substring(dotIndex + 1).toLowerCase();
            return MIME_TYPES.getOrDefault(ext, "application/octet-stream");
        }
        return "application/octet-stream";
    }

    private static WritableGroup resolveGroup(WritableGroup root, Path relativePath) {
        if (relativePath == null || relativePath.toString().isEmpty()) {
            return root;
        }
        WritableGroup current = root;
        for (Path component : relativePath) {
            current = (WritableGroup) current.getChild(component.toString());
        }
        return current;
    }
}
