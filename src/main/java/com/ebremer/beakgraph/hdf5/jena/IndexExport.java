package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.jena.atlas.io.IndentedLineBuffer;
import org.apache.jena.atlas.lib.CharSpace;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming NT/NQ export straight off the GSPO index.
 *
 * <p>The generic path pulls every quad through the full query stack - iterator
 * construction, per-row bindings, node-table materialization, then Jena's
 * stream writer re-serializes every Node OCCURRENCE. But an export visits rows
 * in index order, where terms repeat massively (a subject's text is reused for
 * its whole P/O sub-tree, predicates number a few dozen, objects recur), so
 * this walker keeps GSPO cursors directly and emits MEMOIZED per-id text:
 * each distinct id is dictionary-decoded and formatted once, and each row is
 * just cursor advancement plus text writes.
 *
 * <p>Output is byte-compatible with the generic path: terms are rendered by
 * Jena's own {@link NodeFormatterNT} (UTF-8 char space, exactly what
 * {@code StreamRDFWriter} uses for NTRIPLES/NQUADS), lines are
 * {@code s p o [g] .\n}, default-graph quads carry no graph term, and the
 * BeakGraph-internal metadata graphs ({@code urn:x-beakgraph:*}) are excluded.
 * Disable with {@code -Dbeakgraph.export.fastpath=false} to fall back to the
 * generic writer.
 */
public final class IndexExport {

    private static final Logger logger = LoggerFactory.getLogger(IndexExport.class);

    /** Fast-path activations; observability for tests and diagnostics. */
    public static final AtomicLong HITS = new AtomicLong();

    /**
     * Object-text memo capacity (entries; -Dbeakgraph.export.textcache). A plain
     * map cleared wholesale when full: an access-ordered LRU paid linked-list
     * surgery on EVERY lookup and still thrashed on high-cardinality stores,
     * costing more than it saved.
     */
    private static final int OBJECT_TEXT_CACHE =
            Integer.getInteger("beakgraph.export.textcache", 1 << 18);

    private IndexExport() {}

    /**
     * Streams the store to {@code os} as NT ({@code quads} false: default graph
     * only) or NQ (default graph plus user named graphs). Returns false - with
     * nothing written - when this store cannot take the fast path (no GSPO
     * index, foreign dictionary) or it is disabled; the caller then uses the
     * generic writer. The stream is flushed but not closed.
     */
    public static boolean tryWrite(HDF5Reader reader, OutputStream os, boolean quads) throws IOException {
        return tryWrite(reader, os, quads, java.util.function.UnaryOperator.identity());
    }

    /**
     * As {@link #tryWrite(HDF5Reader, OutputStream, boolean)}, mapping every
     * emitted term through {@code termMap} first (the -export base resolution
     * of document-relative IRIs, or a guard that rejects them). Applied inside
     * the per-id memo, so it costs one call per distinct term.
     */
    public static boolean tryWrite(HDF5Reader reader, OutputStream os, boolean quads,
                                   java.util.function.UnaryOperator<Node> termMap) throws IOException {
        if (!Boolean.parseBoolean(System.getProperty("beakgraph.export.fastpath", "true"))) {
            return false;
        }
        if (!(reader.getDictionary() instanceof PositionalDictionaryReader dict)) {
            return false;
        }
        IndexReader gspo = reader.getIndexReader(Index.GSPO);
        if (gspo == null) {
            return false;
        }
        HITS.incrementAndGet();
        Writer w = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8), 1 << 16);
        Emitter emitter = new Emitter(dict, gspo, w, termMap);
        long defaultGi = dict.getGraphs().locate(Quad.defaultGraphIRI);
        long rows = emitter.emitGraph(defaultGi, null);
        if (quads) {
            long[] gids = dict.streamGraphIds().toArray();
            for (long gid : gids) {
                if (gid == defaultGi || gid < 1) {
                    continue;
                }
                Node graphNode = dict.getGraphs().extract(gid);
                if (Params.isInternalGraph(graphNode)) {
                    continue; // internal metadata graphs (VoID, Spatial, grid tiles) never leave the store
                }
                rows += emitter.emitGraph(gid, graphNode);
            }
        }
        w.flush();
        logger.debug("Index export emitted {} rows ({})", rows, quads ? "NQ" : "NT");
        return true;
    }

    /** GSPO cursor walk + memoized term text for one store. Single-threaded. */
    private static final class Emitter {
        private final Dictionary entities;
        private final Dictionary predicates;
        private final Dictionary objects;
        private final BitPackedUnSignedLongBuffer Bs, Ss, Bp, Sp, Bo, So;
        private final HDTBitmapDirectory dirS, dirP, dirO;
        private final long spNum, soNum;
        private final Writer w;

        private final NodeFormatterNT fmt = new NodeFormatterNT(CharSpace.UTF8);
        private final java.util.function.UnaryOperator<Node> termMap;
        private final Map<Long, String> predicateText = new HashMap<>();
        private final LongTextMap objectText = new LongTextMap(OBJECT_TEXT_CACHE);

        Emitter(PositionalDictionaryReader dict, IndexReader gspo, Writer w, java.util.function.UnaryOperator<Node> termMap) {
            this.termMap = termMap;
            this.entities = dict.getSubjects();
            this.predicates = dict.getPredicates();
            this.objects = dict.getObjects();
            this.Bs = gspo.getBitmapBuffer('S');
            this.Ss = gspo.getIDBuffer('S');
            this.Bp = gspo.getBitmapBuffer('P');
            this.Sp = gspo.getIDBuffer('P');
            this.Bo = gspo.getBitmapBuffer('O');
            this.So = gspo.getIDBuffer('O');
            this.dirS = gspo.getDirectory('S');
            this.dirP = gspo.getDirectory('P');
            this.dirO = gspo.getDirectory('O');
            this.spNum = Sp.getNumEntries();
            this.soNum = So.getNumEntries();
            this.w = w;
        }

        /**
         * Emits every quad of graph {@code gi}; {@code graphNode} null means the
         * default graph (no graph term on the line). Returns the row count.
         */
        long emitGraph(long gi, Node graphNode) throws IOException {
            if (gi < 1) {
                return 0;
            }
            long sStart = RangeSelect.blockStart(dirS, Bs, gi);
            if (sStart == -1) {
                return 0;
            }
            long sEnd = RangeSelect.blockEnd(dirS, Bs, gi, sStart);
            if (sStart > sEnd || Ss.get(sStart) == 0) {
                return 0; // absent or padding-only (empty) graph block
            }
            String graphText = (graphNode == null) ? null : text(graphNode);

            long idxS = sStart;
            long idxP = RangeSelect.blockStart(dirP, Bp, idxS + 1);
            if (idxP == -1) {
                return 0;
            }
            long idxO = RangeSelect.blockStart(dirO, Bo, idxP + 1);
            if (idxO == -1) {
                return 0;
            }
            BitPackedUnSignedLongBuffer.BitReader bpBit = Bp.bitReader();
            BitPackedUnSignedLongBuffer.BitReader boBit = Bo.bitReader();

            long rows = 0;
            // Subjects are unique within a graph: the text is computed once and
            // reused across the subject's whole P/O sub-tree, no cache needed.
            String sText = text(entities.extract(Ss.get(idxS)));
            String pText = predicateText(Sp.get(idxP));
            while (idxS <= sEnd) {
                w.write(sText);
                w.write(' ');
                w.write(pText);
                w.write(' ');
                w.write(objectText(So.get(idxO)));
                if (graphText != null) {
                    w.write(' ');
                    w.write(graphText);
                }
                w.write(" .\n");
                rows++;

                // Cursor advance, mirroring BGIteratorSPO_All: a set bit at the
                // next position means the current block ended there. Text refreshes
                // happen only while the walk continues - past sEnd the next slot may
                // be an empty graph's padding row (id 0), which must not be decoded.
                idxO++;
                if (idxO >= soNum || boBit.bit(idxO)) {
                    idxP++;
                    if (idxP >= spNum || bpBit.bit(idxP)) {
                        idxS++;
                        if (idxS > sEnd) {
                            break;
                        }
                        sText = text(entities.extract(Ss.get(idxS)));
                    }
                    pText = predicateText(Sp.get(idxP));
                }
            }
            return rows;
        }

        private String predicateText(long pid) {
            return predicateText.computeIfAbsent(pid, id -> text(predicates.extract(id)));
        }

        private String objectText(long oid) {
            String t = objectText.get(oid);
            if (t == null) {
                t = text(objects.extract(oid));
                objectText.put(oid, t);
            }
            return t;
        }

        private String text(Node n) {
            IndentedLineBuffer buff = new IndentedLineBuffer();
            fmt.format(buff, termMap.apply(n));
            return buff.asString();
        }
    }

    /**
     * Open-addressing long -> String memo, cleared wholesale when full: no
     * boxing on the per-row lookup and no per-entry eviction bookkeeping (an
     * access-ordered LRU cost more per hit than it saved). Load factor <= 0.5.
     * Single-threaded (one per export).
     */
    private static final class LongTextMap {
        private final long[] keys;
        private final String[] vals;
        private final int mask;
        private final int maxEntries;
        private int size;

        LongTextMap(int maxEntries) {
            this.maxEntries = maxEntries;
            int capacity = Integer.highestOneBit(Math.max(1024, maxEntries) * 2 - 1) << 1;
            this.keys = new long[capacity];
            this.vals = new String[capacity];
            this.mask = capacity - 1;
        }

        /** splitmix64 finalizer. */
        private static long mix(long z) {
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        String get(long key) {
            int i = (int) (mix(key) & mask);
            while (vals[i] != null) {
                if (keys[i] == key) {
                    return vals[i];
                }
                i = (i + 1) & mask;
            }
            return null;
        }

        void put(long key, String value) {
            if (size >= maxEntries) {
                java.util.Arrays.fill(vals, null);
                size = 0;
            }
            int i = (int) (mix(key) & mask);
            while (vals[i] != null) {
                i = (i + 1) & mask;
            }
            keys[i] = key;
            vals[i] = value;
            size++;
        }
    }
}
