package com.ebremer.beakgraph.lws;

import java.nio.file.Path;
import java.nio.file.spi.FileTypeDetector;
import java.util.Map;

public class CustomFileTypeDetector extends FileTypeDetector {

    private static final Map<String, String> TYPE_MAP = Map.of(
        "h5",       "application/vnd.hdfgroup.hdf5",
        "h4",       "application/vnd.hdfgroup.hdf4",
        "json",     "application/json",
        "avro",     "application/avro",
        "parquet",  "application/vnd.apache.parquet",
        "ttl",      "text/turtle",
        "nt",       "application/n-triples",
        "jsonld",   "application/ld+json"
    );

    @Override
    public String probeContentType(Path path) {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        if (dot == -1) return null;
        String ext = name.substring(dot + 1).toLowerCase();
        return TYPE_MAP.get(ext);
    }
}
