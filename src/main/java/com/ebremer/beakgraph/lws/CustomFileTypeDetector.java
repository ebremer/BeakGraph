package com.ebremer.beakgraph.lws;

import java.nio.file.Path;
import java.nio.file.spi.FileTypeDetector;
import java.util.Map;

/**
 * Media types by file extension for the served storage tree (registered as a
 * {@link FileTypeDetector} service, so {@code Files.probeContentType} answers
 * the same on every platform). HDF5 is the IANA-registered
 * {@code application/x-hdf5} (HDF4: {@code application/x-hdf}); the common
 * RDF, text, image and archive extensions are covered so a listing does not
 * label them {@code application/octet-stream} (BG-52).
 */
public class CustomFileTypeDetector extends FileTypeDetector {

    /** Media type of a BeakGraph / HDF5 file. */
    public static final String HDF5 = "application/x-hdf5";

    private static final Map<String, String> TYPE_MAP = Map.ofEntries(
        Map.entry("h5",       HDF5),
        Map.entry("hdf5",     HDF5),
        Map.entry("h4",       "application/x-hdf"),
        Map.entry("hdf",      "application/x-hdf"),
        Map.entry("json",     "application/json"),
        Map.entry("jsonld",   "application/ld+json"),
        Map.entry("avro",     "application/avro"),
        Map.entry("parquet",  "application/vnd.apache.parquet"),
        Map.entry("ttl",      "text/turtle"),
        Map.entry("nt",       "application/n-triples"),
        Map.entry("nq",       "application/n-quads"),
        Map.entry("trig",     "application/trig"),
        Map.entry("rdf",      "application/rdf+xml"),
        Map.entry("xml",      "application/xml"),
        Map.entry("csv",      "text/csv"),
        Map.entry("tsv",      "text/tab-separated-values"),
        Map.entry("txt",      "text/plain"),
        Map.entry("md",       "text/markdown"),
        Map.entry("html",     "text/html"),
        Map.entry("htm",      "text/html"),
        Map.entry("png",      "image/png"),
        Map.entry("jpg",      "image/jpeg"),
        Map.entry("jpeg",     "image/jpeg"),
        Map.entry("gif",      "image/gif"),
        Map.entry("svg",      "image/svg+xml"),
        Map.entry("tif",      "image/tiff"),
        Map.entry("tiff",     "image/tiff"),
        Map.entry("pdf",      "application/pdf"),
        Map.entry("zip",      "application/zip"),
        Map.entry("gz",       "application/gzip")
    );

    @Override
    public String probeContentType(Path path) {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        if (dot == -1) return null;
        String ext = name.substring(dot + 1).toLowerCase(java.util.Locale.ROOT);
        return TYPE_MAP.get(ext);
    }
}
