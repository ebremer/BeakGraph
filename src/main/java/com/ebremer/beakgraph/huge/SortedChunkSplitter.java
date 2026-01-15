package com.ebremer.beakgraph.huge;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.*;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.*;
import org.apache.jena.riot.writer.NQuadsWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.FmtUtils;

/**
 * High-performance splitter that reads a (possibly gzipped) Turtle/N-Triples file,
 * divides it into sorted, gzipped N-Quads chunks of fixed size while limiting
 * concurrent chunk processing to NTHREADS to control memory usage.
 */
public class SortedChunkSplitter {

    private static final int CHUNKSIZE = 100_000;
    private static final Path OUTPUT_DIRECTORY = Path.of("chunks");
    private static final int DEFAULT_NTHREADS = Runtime.getRuntime().availableProcessors();

    // Empty prefix mapping – ensures no prefix abbreviation in string output
    private static final PrefixMapping EMPTY_PREFIXES = new PrefixMappingImpl();

    public static void main(String[] args) throws IOException, InterruptedException {
        int nThreads = DEFAULT_NTHREADS;
        File src = new File("/data/beakgraph/sorted.nq.gz");
        if (nThreads < 1) nThreads = 1;

        Files.createDirectories(OUTPUT_DIRECTORY);
        AtomicInteger chunkCounter = new AtomicInteger(0);
        ExecutorService writerPool = Executors.newFixedThreadPool(nThreads);

        ThreadLocal<List<Quad>> localBuffer = ThreadLocal.withInitial(() -> new ArrayList<>(CHUNKSIZE + 10_000));

        try (InputStream in = src.getName().endsWith(".gz")
                ? new GZIPInputStream(new FileInputStream(src))
                : new FileInputStream(src)) {

            AsyncParserBuilder parserBuilder = AsyncParser.of(in, Lang.TURTLE, null);
            parserBuilder.mutateSources(rdfBuilder ->
                    rdfBuilder.labelToNode(LabelToNode.createUseLabelAsGiven()));

            parserBuilder.streamQuads()
                    .parallel()
                    .forEach(quad -> {
                        List<Quad> buffer = localBuffer.get();
                        buffer.add(quad);
                        if (buffer.size() >= CHUNKSIZE) {
                            List<Quad> toSubmit = new ArrayList<>(buffer);
                            buffer.clear();
                            writerPool.submit(() -> writeSortedChunk(toSubmit, chunkCounter.incrementAndGet()));
                        }
                    });
        }

        // Drain remaining quads from the current thread's buffer
        List<Quad> remainder = localBuffer.get();
        if (!remainder.isEmpty()) {
            writerPool.submit(() -> writeSortedChunk(new ArrayList<>(remainder), chunkCounter.incrementAndGet()));
        }

        writerPool.shutdown();
        writerPool.awaitTermination(7, TimeUnit.DAYS);

        System.out.println("Splitting and sorting completed.");
        System.out.println("   Total chunks written   : " + chunkCounter.get());
        System.out.println("   Output directory       : " + OUTPUT_DIRECTORY.toAbsolutePath());
        System.out.println("   Max concurrent writers : " + nThreads);
    }

    private static void writeSortedChunk(List<Quad> unsorted, int chunkNumber) {
        unsorted.sort(SortedChunkSplitter::compareQuads);

        String fileName = String.format("CHUNK-%08d.nq.gz", chunkNumber);
        Path outFile = OUTPUT_DIRECTORY.resolve(fileName);

        try (OutputStream fos = Files.newOutputStream(outFile);
             GZIPOutputStream gz = new GZIPOutputStream(fos);
             BufferedOutputStream bos = new BufferedOutputStream(gz)) {

            NQuadsWriter.write(bos, unsorted.iterator(), CharSpace.UTF8);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write chunk " + outFile, e);
        }
    }

    private static int compareQuads(Quad a, Quad b) {
        int c = compareNode(a.getGraph(), b.getGraph());
        if (c != 0) return c;
        c = compareNode(a.getSubject(), b.getSubject());
        if (c != 0) return c;
        c = compareNode(a.getPredicate(), b.getPredicate());
        if (c != 0) return c;
        return compareNode(a.getObject(), b.getObject());
    }

    private static int compareNode(Node n1, Node n2) {
        if (n1 == n2) return 0;
        if (n1 == null) return -1;
        if (n2 == null) return 1;

        // Stable N-Triples string representation using FmtUtils (Jena 5.6.0 compatible)
        String s1 = FmtUtils.stringForNode(n1, EMPTY_PREFIXES);
        String s2 = FmtUtils.stringForNode(n2, EMPTY_PREFIXES);
        return s1.compareTo(s2);
    }
}