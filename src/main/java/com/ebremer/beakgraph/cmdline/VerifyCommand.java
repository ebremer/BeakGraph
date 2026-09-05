package com.ebremer.beakgraph.cmdline;

import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexCounts;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
 * BOTH indexes must be present (a non-empty store without GSPO or GPOS fails
 * the first bound-predicate query, BG-292) and construct - every level's
 * bitmap and id datasets exist, have the same row count and declare sizes
 * their bytes can hold (BG-345) - the graph list must enumerate, and a sample
 * of every dictionary section must be in NodeComparator order and findable
 * by the readers' binary search (a store sorted under a drifted comparator
 * answered every concrete-term pattern with nothing, BG-343). {@code -deep}
 * additionally checks the whole dictionary order, materializes every triple
 * of every graph through GSPO - resolving all terms through the dictionaries
 * - reconciles the total against the index-derived count, and then walks
 * GPOS as well: its row count, one predicate-bound scan per predicate and a
 * sample of {@code ?s p o} probes must agree with what GSPO produced, so
 * damage inside the second index no longer passes (BG-147).
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
            Verdict v = verifyOne(f);
            // The format version rides on the verdict line: it is how an
            // operator learns which stores predate a format change and must be
            // rebuilt (see docs/INSTRUCTIONS.md, "Format versions").
            String version = (v.formatVersion() > 0) ? "  (format v" + v.formatVersion() + ")" : "";
            if (v.problems().isEmpty()) {
                out.println("OK    " + f + version);
            } else {
                failed++;
                out.println("FAIL  " + f + version);
                for (String problem : v.problems()) {
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
        return com.ebremer.beakgraph.core.BeakGraphFiles.isBeakGraphFileName(p.getFileName().toString());
    }

    /** One file's verdict: its problems (none = passed) and its format version (-1 when it could not be opened). */
    record Verdict(List<String> problems, long formatVersion) {}

    /** Verifies one file; an empty problem list means it passed. */
    private Verdict verifyOne(Path f) {
        List<String> problems = new ArrayList<>();
        long formatVersion = -1;
        long size;
        try {
            size = Files.size(f);
        } catch (IOException e) {
            problems.add("unreadable: " + rootMessage(e));
            return new Verdict(problems, formatVersion);
        }
        if (size == 0) {
            problems.add("empty file");
            return new Verdict(problems, formatVersion);
        }
        try (HDF5Reader reader = new HDF5Reader(f.toFile())) {
            formatVersion = reader.getFormatVersion();
            checkStructure(reader, problems);
            checkDictionaryOrder(reader, problems);
            if (deep && problems.isEmpty()) {
                deepScan(reader, problems);
            }
        } catch (Exception e) {
            // The check methods record their own failures, so what lands here is
            // construction: not HDF5, not a BeakGraph, unsupported format
            // version, or metadata/dictionaries cut off mid-file.
            problems.add("cannot open: " + rootMessage(e));
        }
        return new Verdict(problems, formatVersion);
    }

    /**
     * Structural pass: both indexes must be present (an empty store - no
     * dictionary sections at all - legitimately has neither) and construct,
     * which walks the index groups and maps every dataset the readers use
     * (the bit-packed views reject declared sizes their bytes cannot hold);
     * every level of a present index must have its bitmap and id datasets
     * with matching row counts (SPECIFICATIONS §8.1); and the graph list
     * must enumerate.
     */
    private static void checkStructure(HDF5Reader reader, List<String> problems) {
        boolean empty = ((PositionalDictionaryReader) reader.getDictionary()).isEmpty();
        for (Index idx : Index.values()) {
            IndexReader ir;
            try {
                ir = reader.getIndexReader(idx);
            } catch (Exception e) {
                problems.add("index " + idx + ": " + rootMessage(e));
                continue;
            }
            if (ir == null) {
                if (!empty) {
                    problems.add("index " + idx + " absent (a conformant file must contain both GSPO and GPOS)");
                }
                continue;
            }
            if (reader.getFormatVersion() < com.ebremer.beakgraph.Params.RANK_DIRECTORY_MIN_VERSION
                    && ir.hasDirectoryDatasets()) {
                // A v3+ store whose formatVersion attribute was lost reads as v1:
                // the reader ignores the directories it carries and every lookup
                // degrades to the linear select1 scan - a performance cliff that
                // used to be published as "OK" (BG-350).
                problems.add("formatVersion " + (reader.hasFormatVersionAttribute()
                        ? "is " + reader.getFormatVersion() : "attribute is missing (treated as v1)")
                        + " but index " + idx + " carries rank directories (SB*/BB*), which need format v"
                        + com.ebremer.beakgraph.Params.RANK_DIRECTORY_MIN_VERSION
                        + ": the reader disables them and every lookup falls back to the linear select1 scan");
            }
            if (empty) {
                continue;
            }
            for (int i = 1; i < 4; i++) {
                char c = idx.name().charAt(i);
                String suffix = String.valueOf(c).toLowerCase(Locale.ROOT);
                BitPackedUnSignedLongBuffer bitmap = ir.getBitmapBuffer(c);
                BitPackedUnSignedLongBuffer ids = ir.getIDBuffer(c);
                if (bitmap == null) {
                    problems.add("index " + idx + ": dataset B" + suffix + " missing");
                }
                if (ids == null) {
                    problems.add("index " + idx + ": dataset S" + suffix + " missing");
                }
                if (bitmap != null && ids != null && bitmap.getNumEntries() != ids.getNumEntries()) {
                    problems.add("index " + idx + ": B" + suffix + " has " + bitmap.getNumEntries()
                            + " rows but S" + suffix + " has " + ids.getNumEntries());
                }
            }
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

    /** Ids probed per dictionary section in the structural pass (all of them under -deep). */
    static final int ORDER_SAMPLE = 128;

    /**
     * Dictionary order: term ids are ranks under NodeComparator and every
     * concrete-term lookup is a binary search over them, so a section that is
     * not sorted under the CURRENT comparator (written by a build whose Jena
     * value ordering differed, or by a foreign writer) extracts perfectly and
     * answers every lookup with nothing (BG-343). Structural: a sample of ids
     * per section, each compared with its successor and searched back by
     * value; deep: every adjacent pair, plus the sampled searches.
     */
    private void checkDictionaryOrder(HDF5Reader reader, List<String> problems) {
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        Dictionary entities = dict.getSubjects();     // graphs and subjects share it
        Dictionary predicates = dict.getPredicates();
        Dictionary objects = dict.getObjects();       // entities, then the literals section
        long entityCount = entities.getNumberOfNodes();
        checkSection("entities", entities, 1, entityCount, deep, problems);
        checkSection("predicates", predicates, 1, predicates.getNumberOfNodes(), deep, problems);
        checkSection("literals", objects, entityCount + 1, objects.getNumberOfNodes(), deep, problems);
    }

    /**
     * Probes ids {@code first..last} of one section; records at most one
     * problem per section.
     */
    static void checkSection(String name, Dictionary d, long first, long last, boolean deep, List<String> problems) {
        if (last < first) {
            return;
        }
        long n = last - first + 1;
        try {
            if (deep) {
                Node prev = d.extract(first);
                for (long id = first + 1; id <= last; id++) {
                    Node cur = d.extract(id);
                    if (NodeComparator.INSTANCE.compare(prev, cur) >= 0) {
                        problems.add("dictionary " + name + ": ids " + (id - 1) + "," + id
                                + " out of order under NodeComparator (writer/comparator drift): "
                                + prev + " >= " + cur);
                        return;
                    }
                    prev = cur;
                }
            }
            int k = (int) Math.min(n, ORDER_SAMPLE);
            for (int i = 0; i < k; i++) {
                long id = (k == 1) ? first : first + (long) ((n - 1) * (double) i / (k - 1));
                Node node = d.extract(id);
                if (!deep && id < last) {
                    Node next = d.extract(id + 1);
                    if (NodeComparator.INSTANCE.compare(node, next) >= 0) {
                        problems.add("dictionary " + name + ": ids " + id + "," + (id + 1)
                                + " out of order under NodeComparator (writer/comparator drift): "
                                + node + " >= " + next);
                        return;
                    }
                }
                long found = d.search(node);
                if (found != id) {
                    problems.add("dictionary " + name + ": id " + id + " (" + node
                            + ") not searchable - binary search answers " + found + " (comparator/order drift)");
                    return;
                }
            }
        } catch (Exception e) {
            problems.add("dictionary " + name + ": " + rootMessage(e));
        }
    }

    /** Triples sampled per graph for the {@code ?s p o} probes through GPOS. */
    static final int PROBE_SAMPLE = 64;

    /**
     * Data pass. GSPO: stream every triple of every graph, which resolves
     * every id through the dictionaries, and reconcile against the
     * index-derived count - the reader silently drops rows whose terms cannot
     * be resolved, so a shortfall is exactly the corruption signal. GPOS
     * (BG-147, BG-292): its structure-derived row count must equal GSPO's;
     * the graph's distinct-predicate count and one predicate-bound scan per
     * predicate (BGIteratorPOS over GPOS's P, O and S levels) must reproduce
     * the per-predicate counts of the GSPO stream; and a sample of the
     * streamed triples must be found again by an object-bound
     * {@code ?s p o} lookup, which searches the dictionary by value.
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
        long sourceRows = 0;   // rows outside the urn:x-beakgraph:* metadata graphs
        for (Node g : graphs) {
            Map<Node, Long> perPredicate = new LinkedHashMap<>();
            List<Triple> sample = new ArrayList<>();
            long actual = 0;
            try {
                long expected = reader.countTriples(g); // -1 = not index-answerable
                long stride = Math.max(1, expected / PROBE_SAMPLE);
                ExtendedIterator<Triple> triples =
                        reader.graphBaseFind(g, Triple.create(Node.ANY, Node.ANY, Node.ANY));
                try {
                    while (triples.hasNext()) {
                        Triple t = triples.next();
                        perPredicate.merge(t.getPredicate(), 1L, Long::sum);
                        if (actual % stride == 0 && sample.size() < PROBE_SAMPLE && !t.getObject().isBlank()) {
                            sample.add(t);
                        }
                        actual++;
                    }
                } finally {
                    triples.close();
                }
                if (expected >= 0 && actual != expected) {
                    problems.add("graph " + g + ": materialized " + actual + " of " + expected
                            + " indexed triples (unresolvable terms or index damage)");
                }
                if (!(g.isURI() && g.getURI().startsWith("urn:x-beakgraph:"))) {
                    sourceRows += actual;
                }
                long viaGpos = IndexCounts.quads(reader, g, Index.GPOS);
                if (expected >= 0 && viaGpos >= 0 && viaGpos != expected) {
                    problems.add("graph " + g + ": GPOS holds " + viaGpos + " rows, GSPO holds " + expected
                            + " (the two indexes disagree)");
                }
            } catch (Exception e) {
                problems.add("graph " + g + " scan: " + rootMessage(e));
                continue;
            }
            try {
                long distinct = IndexCounts.distinctPredicates(reader, g);
                if (distinct >= 0 && distinct != perPredicate.size()) {
                    problems.add("graph " + g + ": GPOS lists " + distinct + " predicates, the GSPO stream saw "
                            + perPredicate.size());
                }
                for (Map.Entry<Node, Long> e : perPredicate.entrySet()) {
                    long n = count(reader.graphBaseFind(g, Triple.create(Node.ANY, e.getKey(), Node.ANY)));
                    if (n != e.getValue()) {
                        problems.add("graph " + g + ": predicate " + e.getKey() + " answers " + n
                                + " rows through GPOS but " + e.getValue() + " through GSPO");
                        break;
                    }
                }
                for (Triple t : sample) {
                    boolean found = false;
                    ExtendedIterator<Triple> hits =
                            reader.graphBaseFind(g, Triple.create(Node.ANY, t.getPredicate(), t.getObject()));
                    try {
                        while (hits.hasNext()) {
                            if (hits.next().getSubject().equals(t.getSubject())) {
                                found = true;
                                break;
                            }
                        }
                    } finally {
                        hits.close();
                    }
                    if (!found) {
                        problems.add("graph " + g + ": " + t + " is in GSPO but not found through GPOS (?s p o)");
                        break;
                    }
                }
            } catch (Exception e) {
                problems.add("graph " + g + " GPOS scan: " + rootMessage(e));
            }
        }
        // numQuads counts SOURCE quads before de-duplication (SPECIFICATIONS
        // §9.6) and the derived quads all live in urn:x-beakgraph:* graphs,
        // so it bounds the rows of every other graph from above (BG-350).
        long numQuads = reader.getNumQuads();
        if (numQuads >= 0 && sourceRows > numQuads) {
            problems.add("the data graphs hold " + sourceRows + " triples but numQuads records only " + numQuads
                    + " source quads (numQuads counts source quads before de-duplication, so it is an upper bound)");
        }
    }

    private static long count(ExtendedIterator<Triple> triples) {
        long n = 0;
        try {
            while (triples.hasNext()) {
                triples.next();
                n++;
            }
        } finally {
            triples.close();
        }
        return n;
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
