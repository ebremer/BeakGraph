package com.ebremer.beakgraph.cmdline;

import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * {@code -verify}: integrity check for BeakGraph HDF5 files - one file or a
 * directory tree ({@code *.h5}/{@code *.hdf5}, recursive). Meant as the gate a
 * pipeline runs before publishing files into served storage, where a
 * truncated or partially copied file would otherwise surface lazily as a
 * query-time failure.
 *
 * <p>The default (structural) pass opens each file with the real reader stack
 * and forces everything a query would eventually need: dictionaries load,
 * every index present must construct (a present-but-unreadable index is the
 * signature of truncation), and the graph list must enumerate. {@code -deep}
 * additionally materializes every triple of every graph - resolving all terms
 * through the dictionaries - and reconciles the total against the
 * index-derived count, catching data-region corruption that structure alone
 * passes over.
 *
 * <p>One verdict line per file ({@code OK}/{@code FAIL} plus reasons), then a
 * summary. Exit codes follow the CLI convention: 0 all files pass, 1 nothing
 * to verify, 2 at least one file is damaged (verification continues past
 * failures).
 *
 * @author Erich Bremer
 */
public final class VerifyCommand {

    private final File root;
    private final boolean deep;
    private final PrintStream out;

    public VerifyCommand(File root, boolean deep) {
        this(root, deep, System.out);
    }

    VerifyCommand(File root, boolean deep, PrintStream out) {
        this.root = root;
        this.deep = deep;
        this.out = out;
    }

    /** Runs the verification and returns the process exit code (0/1/2). */
    public int run() {
        List<Path> files;
        try {
            files = collect();
        } catch (IOException e) {
            out.println("Error scanning " + root + ": " + e.getMessage());
            return 1;
        }
        if (files.isEmpty()) {
            out.println("No BeakGraph (.h5/.hdf5) files found under " + root);
            return 1;
        }
        int failed = 0;
        for (Path f : files) {
            List<String> problems = verifyOne(f);
            if (problems.isEmpty()) {
                out.println("OK    " + f);
            } else {
                failed++;
                out.println("FAIL  " + f);
                for (String problem : problems) {
                    out.println("      - " + problem);
                }
            }
        }
        out.println("Verified " + files.size() + " file(s)" + (deep ? " (deep)" : "")
                + ": " + (files.size() - failed) + " OK, " + failed + " FAILED");
        return (failed > 0) ? 2 : 0;
    }

    private List<Path> collect() throws IOException {
        Path path = root.toPath();
        if (Files.isRegularFile(path)) {
            // An explicitly named file is verified regardless of its extension.
            return List.of(path);
        }
        try (Stream<Path> walk = Files.walk(path)) {
            return walk.filter(Files::isRegularFile)
                    .filter(VerifyCommand::isHdf5Name)
                    .sorted()
                    .toList();
        }
    }

    private static boolean isHdf5Name(Path p) {
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".h5") || name.endsWith(".hdf5");
    }

    /** Verifies one file; an empty list means it passed. */
    private List<String> verifyOne(Path f) {
        List<String> problems = new ArrayList<>();
        long size;
        try {
            size = Files.size(f);
        } catch (IOException e) {
            problems.add("unreadable: " + rootMessage(e));
            return problems;
        }
        if (size == 0) {
            problems.add("empty file");
            return problems;
        }
        try (HDF5Reader reader = new HDF5Reader(f.toFile())) {
            checkStructure(reader, problems);
            if (deep && problems.isEmpty()) {
                deepScan(reader, problems);
            }
        } catch (Exception e) {
            // The check methods record their own failures, so what lands here is
            // construction: not HDF5, not a BeakGraph, unsupported format
            // version, or metadata/dictionaries cut off mid-file.
            problems.add("cannot open: " + rootMessage(e));
        }
        return problems;
    }

    /**
     * Structural pass: every index present must construct (this walks the
     * index groups and maps every dataset the readers use), and the graph
     * list must enumerate. An absent index is legal - only a present index
     * that fails to load is damage.
     */
    private static void checkStructure(HDF5Reader reader, List<String> problems) {
        int present = 0;
        int broken = 0;
        for (Index idx : Index.values()) {
            try {
                if (reader.getIndexReader(idx) != null) {
                    present++;
                }
            } catch (Exception e) {
                broken++;
                problems.add("index " + idx + ": " + rootMessage(e));
            }
        }
        if (present == 0 && broken == 0) {
            problems.add("no indexes present (GSPO and GPOS both absent)");
        }
        try {
            Iterator<Node> graphs = reader.listGraphNodes();
            while (graphs.hasNext()) {
                graphs.next();
            }
        } catch (Exception e) {
            problems.add("graph enumeration: " + rootMessage(e));
        }
    }

    /**
     * Data pass: stream every triple of every graph, which resolves every id
     * through the dictionaries, and reconcile against the index-derived
     * count. The reader silently drops rows whose terms cannot be resolved,
     * so a shortfall against the structural count is exactly the corruption
     * signal this pass exists to catch.
     */
    private void deepScan(HDF5Reader reader, List<String> problems) {
        List<Node> graphs = new ArrayList<>();
        graphs.add(Quad.defaultGraphIRI);
        Iterator<Node> it = reader.listGraphNodes();
        while (it.hasNext()) {
            Node g = it.next();
            if (!Quad.defaultGraphIRI.equals(g)) {
                graphs.add(g);
            }
        }
        for (Node g : graphs) {
            try {
                long expected = reader.countTriples(g); // -1 = not index-answerable
                long actual = 0;
                ExtendedIterator<Triple> triples =
                        reader.graphBaseFind(g, Triple.create(Node.ANY, Node.ANY, Node.ANY));
                try {
                    while (triples.hasNext()) {
                        triples.next();
                        actual++;
                    }
                } finally {
                    triples.close();
                }
                if (expected >= 0 && actual != expected) {
                    problems.add("graph " + g + ": materialized " + actual + " of " + expected
                            + " indexed triples (unresolvable terms or index damage)");
                }
            } catch (Exception e) {
                problems.add("graph " + g + " scan: " + rootMessage(e));
            }
        }
    }

    /** The deepest cause's message - where the actual I/O failure is named. */
    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return (message == null || message.isBlank()) ? root.getClass().getSimpleName() : message;
    }
}
