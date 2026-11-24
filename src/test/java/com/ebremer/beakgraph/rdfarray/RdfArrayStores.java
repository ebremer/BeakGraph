package com.ebremer.beakgraph.rdfarray;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.core.BeakGraph;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RiotException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.QueryExecBuilder;
import org.apache.jena.sparql.exec.QueryExec;

/**
 * The stores the fork's rdf-array tests run over once ported to BeakGraph
 * ({@code TestUndefSlots}, {@code TestSequenceQueries},
 * {@code TestArraySyntaxExamples}): where a test parsed a document into an
 * in-memory graph, it builds a BeakGraph store from the document and reads it
 * back, and its queries run through BeakGraph's own dataset (and so its
 * executor), not a wrapper around a graph. One store per document, kept open
 * until {@link #closeAll}.
 */
final class RdfArrayStores {

    private final Path dir;
    private final Map<String, BeakGraph> open = new HashMap<>();
    private final List<BeakGraph> all = new ArrayList<>();

    RdfArrayStores(Path dir) {
        this.dir = dir;
    }

    /** The engine named by -Dbeakgraph.test.engine (method 0 by default). */
    private static WriterEngines.Engine engine() {
        return WriterEngines.selected();
    }

    /**
     * The store built from {@code text}, which is in {@code lang}. A document
     * the fork's parser refuses throws the parser's {@link RiotException}, as
     * the in-memory parse did; one BeakGraph refuses throws the guard's.
     */
    synchronized BeakGraph store(String text, Lang lang) {
        String key = lang.getName() + "\n" + text;
        BeakGraph bg = open.get(key);
        if (bg != null) {
            return bg;
        }
        String stem = "s" + open.size();
        String ext = lang.getFileExtensions().get(0);
        Path src = dir.resolve(stem + "." + ext);
        File dest = dir.resolve(stem + ".h5").toFile();
        try {
            Files.writeString(src, text, StandardCharsets.UTF_8);
            engine().buildStore(src.toFile(), dest);
        } catch (Exception e) {
            // The writer reports a parse or guard failure wrapped ("Failed while
            // parsing/processing RDF source"); the test asked about the cause.
            for (Throwable c = e; c != null; c = c.getCause()) {
                if (c instanceof RiotException riot) {
                    throw riot;
                }
            }
            for (Throwable c = e; c != null; c = c.getCause()) {
                if (c instanceof IllegalStateException guard) {
                    throw guard;
                }
            }
            throw (e instanceof IOException io) ? new UncheckedIOException(io) : new IllegalStateException(e);
        }
        try {
            bg = BG.getBeakGraph(dest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        open.put(key, bg);
        all.add(bg);
        return bg;
    }

    /** The store's default graph: the BeakGraph itself. */
    Graph graph(String text, Lang lang) {
        return store(text, lang);
    }

    /** The store's dataset, named graphs included. */
    DatasetGraph dataset(String text, Lang lang) {
        return store(text, lang).getDataset().asDatasetGraph();
    }

    /** A query over {@code graph}'s store, through its own dataset (BeakGraph's executor). */
    static QueryExecBuilder exec(Graph graph) {
        if (graph instanceof BeakGraph bg) {
            return QueryExec.dataset(bg.getDataset().asDatasetGraph());
        }
        return QueryExec.graph(graph);
    }

    void closeAll() throws Exception {
        for (BeakGraph bg : all) {
            bg.close();
        }
        all.clear();
        open.clear();
    }
}
