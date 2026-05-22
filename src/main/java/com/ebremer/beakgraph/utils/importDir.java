package com.ebremer.beakgraph.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.Quad;

/**
 * Recursively scans a source directory for RDF files (.ttl, .nt, .ttl.gz,
 * .nt.gz) and streams every triple they contain into a single, gzip-compressed
 * Turtle file.
 *
 * The import is fully streaming: input files are parsed one triple at a time
 * and forwarded directly to the destination writer, so neither the input nor
 * the merged output is ever held entirely in memory.
 *
 * Usage:
 *   new importDIR(new File("D:/data/rdf"), new File("D:/data/merged.ttl.gz"));
 *
 * @author Erich Bremer
 */
public class importDir {

    /**
     * Recursively imports all RDF found under {@code dir} into the single
     * gzip-compressed Turtle file {@code dest}.
     *
     * @param dir  source directory to scan recursively
     * @param dest destination file; written as gzip-compressed Turtle
     * @throws IOException if the directory cannot be read or dest cannot be written
     */
    public importDir(File dir, File dest) throws IOException {
        if (dir == null || !dir.isDirectory()) {
            throw new IllegalArgumentException("Source must be an existing directory: " + dir);
        }
        if (dest == null) {
            throw new IllegalArgumentException("Destination file must not be null");
        }
        File destCanonical = dest.getCanonicalFile();
        List<File> files = new ArrayList<>();
        collect(dir, destCanonical, files);
        files.sort(Comparator.comparing(File::getPath));
        File parent = destCanonical.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }
        long triples;
        int imported = 0;
        int skipped = 0;
        try (OutputStream fos = new FileOutputStream(destCanonical);
             GZIPOutputStream gz = new GZIPOutputStream(new BufferedOutputStream(fos), 1 << 16)) {
            StreamRDF target = StreamRDFWriter.getWriterStream(gz, RDFFormat.TURTLE_BLOCKS);
            Sink sink = new Sink(target);
            // start()/finish() are driven here, once, rather than per parsed file.
            target.start();
            for (File f : files) {
                Lang lang = langFor(f);
                if (lang == null) {
                    continue;
                }
                IO.println("Importing [" + lang.getName() + "] " + f);
                try (InputStream in = open(f)) {
                    RDFParser.source(in)
                             .lang(lang)
                             .base(f.toURI().toString())
                             .parse(sink);
                    imported++;
                } catch (RuntimeException ex) {
                    skipped++;
                    IO.println("  !! skipped (parse error): " + ex.getMessage());
                }
            }
            target.finish();
            triples = sink.count();
        }
        IO.println("Done. Imported " + imported + " file(s), skipped " + skipped
                + ", wrote " + triples + " triple(s) -> " + destCanonical);
    }

    /**
     * Recursively collects every importable RDF file under {@code dir},
     * excluding the destination file itself (so re-running over a directory
     * that contains a previous output does not feed it back in).
     */
    private static void collect(File dir, File destCanonical, List<File> out) throws IOException {
        File[] kids = dir.listFiles();
        if (kids == null) {
            return;
        }
        for (File k : kids) {
            if (k.isDirectory()) {
                collect(k, destCanonical, out);
            } else if (langFor(k) != null && !k.getCanonicalFile().equals(destCanonical)) {
                out.add(k);
            }
        }
    }

    /**
     * Returns the RDF language for a file based on its extension, ignoring a
     * trailing {@code .gz}, or {@code null} if the file is not importable RDF.
     */
    private static Lang langFor(File f) {
        String n = f.getName().toLowerCase();
        if (n.endsWith(".ttl") || n.endsWith(".ttl.gz")) {
            return Lang.TURTLE;
        }
        if (n.endsWith(".nt") || n.endsWith(".nt.gz")) {
            return Lang.NTRIPLES;
        }
        return null;
    }

    /**
     * Opens a file for reading, transparently decompressing it when the name
     * ends in {@code .gz}.
     */
    private static InputStream open(File f) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(f), 1 << 16);
        if (f.getName().toLowerCase().endsWith(".gz")) {
            return new GZIPInputStream(in, 1 << 16);
        }
        return in;
    }

    /**
     * Forwards triples/quads/prefixes to a single shared writer while
     * suppressing the per-file start()/finish() calls each RDFParser run
     * issues, so that every parsed file lands in one continuous Turtle
     * document. Also tallies the number of statements written.
     */
    private static final class Sink extends StreamRDFWrapper {
        private long count = 0;

        Sink(StreamRDF target) {
            super(target);
        }

        @Override
        public void start() {
            // no-op: lifecycle is managed by the enclosing importDIR
        }

        @Override
        public void finish() {
            // no-op: lifecycle is managed by the enclosing importDIR
        }

        @Override
        public void triple(Triple triple) {
            count++;
            super.triple(triple);
        }

        @Override
        public void quad(Quad quad) {
            count++;
            super.quad(quad);
        }

        long count() {
            return count;
        }
    }

    public static void main(String[] args) throws IOException {
        
        String[] tmp = {"E:\\rpi\\aw\\src", "e:\\all.ttl.gz"};
        args = tmp;
        if (args.length != 2) {
            IO.println("Usage: importDIR <source-directory> <dest.ttl.gz>");
            return;
        }
        new importDir(new File(args[0]), new File(args[1]));
    }
}
