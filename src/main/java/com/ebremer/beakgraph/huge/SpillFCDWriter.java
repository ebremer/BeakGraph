package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.DictionarySinks;
import static com.ebremer.beakgraph.Params.COMPRESSION_THRESHOLD;
import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.utils.StringUtils;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * Disk-backed twin of {@link com.ebremer.beakgraph.hdf5.writers.FCDWriter}: the
 * same front-coded block format (block heads, VByte prefix lengths, Zstd
 * compression above {@code COMPRESSION_THRESHOLD}, surrogate-safe prefixes) and
 * the same HDF5 group shape (stringbuffer/offsets/compressed datasets plus
 * blockSize/numBlocks/numEntries/compression_threshold attributes), but the
 * string buffer grows in a temp file, so dictionary size is bounded by disk.
 *
 * <p>The strings are encoded a batch at a time - a stretch of the stream, of
 * about {@link #BATCH_CHARS} characters - into the bytes the string buffer
 * takes for them. What a batch's bytes are depends on nothing but its strings,
 * the place of its first one in a block and the string before it, all known
 * when the batch is handed over; so, given a pool, the batches are encoded
 * (prefixes found, UTF-8 made, fragments compressed) by its threads while the
 * caller goes on adding, and written in their order as they are done. The
 * heap holds {@code window} batches at most.
 *
 * @author Erich Bremer
 */
final class SpillFCDWriter implements AutoCloseable, DictionarySinks.StringSink {

    /** Characters after which the strings added so far are encoded as a batch, */
    static final int BATCH_CHARS = 1 << 17;
    /** or strings, should they be very short. */
    static final int BATCH_STRINGS = 1 << 14;

    private final String name;
    private final int blockSize;
    private final Path stringFile;
    private final OutputStream stringOut;
    private long numEntries = 0;
    private long position = 0;
    private final SpillDataBuffer offsets;
    private final SpillBitPackedBuffer compressed;
    private boolean completed = false;

    // The batch being gathered: its strings, where its first one stands in a
    // block, and the string before it (null at a block's head).
    private ArrayList<String> pending = new ArrayList<>();
    private long pendingChars = 0;
    private int pendingPhase = 0;
    private String pendingBefore = null;

    /** Null: the adding thread encodes each batch itself. */
    private final Executor pool;
    private final int window;
    private final ArrayDeque<PoolTask<Batch>> inFlight = new ArrayDeque<>();
    // A compressor serves one thread at a time: taken for a batch, put back after it.
    private final ConcurrentLinkedQueue<StringUtils> compressors = new ConcurrentLinkedQueue<>();

    /** The string-buffer bytes of a stretch of strings, where its blocks begin in them, and which fragments are compressed. */
    private record Batch(byte[] bytes, int[] blockStarts, boolean[] compressed) {}

    SpillFCDWriter(Path workDir, String name, int blockSize) throws IOException {
        this(workDir, name, blockSize, null, 0);
    }

    /**
     * @param pool   encodes the batches, or null for the adding thread to
     * @param window batches encoded or waiting to be written at once
     */
    SpillFCDWriter(Path workDir, String name, int blockSize, Executor pool, int window) throws IOException {
        if (blockSize < 2) {
            // Same guard as FCDWriter: blockSize 1 never closes a block and the
            // offsets dataset would be unreadable.
            throw new IllegalArgumentException("FCD blockSize must be >= 2, got " + blockSize);
        }
        this.name = name;
        this.blockSize = blockSize;
        this.pool = pool;
        this.window = Math.max(1, window);
        this.stringFile = workDir.resolve(name + ".fcd.strings");
        this.stringOut = new BufferedOutputStream(Files.newOutputStream(stringFile), 1 << 16);
        SpillDataBuffer offs = null;
        try {
            offs = new SpillDataBuffer(workDir.resolve(name + ".fcd.offsets"));
            this.compressed = new SpillBitPackedBuffer(workDir.resolve(name + ".fcd.compressed"), 1);
        } catch (IOException | RuntimeException ex) {
            // Release the streams already open (BG-128).
            try { stringOut.close(); } catch (IOException ignored) { }
            if (offs != null) {
                try { offs.close(); } catch (IOException ignored) { }
            }
            throw ex;
        }
        this.offsets = offs;
    }

    public void add(String item) throws IOException {
        if (completed) {
            throw new IllegalStateException("FCD dictionary '" + name + "' already completed");
        }
        numEntries++;
        pending.add(item);
        pendingChars += item.length();
        if (pendingChars >= BATCH_CHARS || pending.size() >= BATCH_STRINGS) {
            hand();
        }
    }

    /** Hands the gathered strings on to be encoded, and writes what is ready (or due) of the batches before. */
    private void hand() throws IOException {
        if (pending.isEmpty()) {
            return;
        }
        final String[] items = pending.toArray(String[]::new);
        final int phase = pendingPhase;
        final String before = pendingBefore;
        pending = new ArrayList<>();
        pendingChars = 0;
        pendingPhase = (int) ((phase + (long) items.length) % blockSize);
        pendingBefore = (pendingPhase == 0) ? null : items[items.length - 1];
        if (pool == null) {
            write(encode(items, phase, before));
            return;
        }
        inFlight.add(PoolTask.submit(pool, () -> encode(items, phase, before)));
        while (!inFlight.isEmpty() && (inFlight.size() > window || inFlight.peek().isDone())) {
            write(inFlight.poll().join("encoding the strings of dictionary '" + name + "'"));
        }
    }

    /**
     * The bytes {@code FCDWriter.add} appends for {@code items}, the first of
     * which is string number {@code phase} of its block and follows
     * {@code before}: a block's head whole, every other string as the length
     * of the prefix it shares with the one before it and the rest; each
     * fragment as its length and its UTF-8 bytes, Zstd-compressed from
     * {@code COMPRESSION_THRESHOLD} bytes up (byte-identical to FCDWriter,
     * BG-105). Runs on any thread.
     */
    private Batch encode(String[] items, int phase, String before) throws IOException {
        StringUtils su = compressors.poll();
        if (su == null) {
            su = new StringUtils();
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(1 << 16);
            int[] blockStarts = new int[(phase + items.length + blockSize - 1) / blockSize];
            int blocks = 0;
            boolean[] flags = new boolean[items.length];
            String prev = before;
            for (int i = 0; i < items.length; i++) {
                String item = items[i];
                byte[] data;
                if ((phase + i) % blockSize == 0) {
                    blockStarts[blocks++] = bytes.size();
                    data = item.getBytes(StandardCharsets.UTF_8);
                } else {
                    int prefixLength = commonPrefixLength(prev, item);
                    VByte.encode(bytes, prefixLength);
                    data = item.substring(prefixLength).getBytes(StandardCharsets.UTF_8);
                }
                if (data.length >= COMPRESSION_THRESHOLD) {
                    data = su.compress(data);
                    flags[i] = true;
                }
                VByte.encode(bytes, data.length);
                bytes.write(data, 0, data.length);
                prev = item;
            }
            return new Batch(bytes.toByteArray(), java.util.Arrays.copyOf(blockStarts, blocks), flags);
        } finally {
            compressors.add(su);
        }
    }

    /** Appends an encoded batch: the batches are written in the order their strings were added. */
    private void write(Batch batch) throws IOException {
        for (int start : batch.blockStarts) {
            offsets.writeLong(position + start);
        }
        stringOut.write(batch.bytes);
        position += batch.bytes.length;
        for (boolean flag : batch.compressed) {
            compressed.writeLong(flag ? 1 : 0);
        }
    }

    private static int commonPrefixLength(String s1, String s2) {
        int minLength = Math.min(s1.length(), s2.length());
        int i = 0;
        while (i < minLength && s1.charAt(i) == s2.charAt(i)) i++;
        // Never split a UTF-16 surrogate pair (see FCDWriter).
        if (i > 0 && Character.isHighSurrogate(s1.charAt(i - 1))) i--;
        return i;
    }

    public long getNumEntries() {
        return numEntries;
    }

    /** Encodes what is still gathered, writes every batch, and closes the temp files for writing. */
    void finish() throws IOException {
        if (completed) return;
        hand();
        while (!inFlight.isEmpty()) {
            write(inFlight.poll().join("encoding the strings of dictionary '" + name + "'"));
        }
        completed = true;
        pending = null;
        stringOut.close();
        offsets.complete();
        compressed.complete();
    }

    /**
     * Writes the whole dictionary as a subgroup named after this writer,
     * mirroring {@code FCDWriter.add()} exactly (attribute set, dataset names,
     * numBlocks accounting). Temp files are deleted afterwards.
     */
    void transferTo(StreamingHdf5Group parent) throws IOException {
        finish();
        StreamingHdf5Group strings = parent.putGroup(name);
        strings.putAttribute(Params.BLOCK_SIZE, blockSize);
        // Whole blocks, and the one being filled - which there is, empty, after a last whole one.
        long numBlocks = numEntries / blockSize;
        long validBlocks = (numEntries % blockSize == 0 && numEntries > 0) ? numBlocks : numBlocks + 1;
        strings.putAttribute(Params.NUM_BLOCKS, (numEntries == 0) ? 0 : validBlocks);
        strings.putAttribute(Params.NUM_ENTRIES, numEntries);
        strings.putAttribute("compression_threshold", COMPRESSION_THRESHOLD);
        long size = Files.size(stringFile);
        if (size > 0) {
            try (StreamingHdf5Dataset ds = strings.createByteDataset("stringbuffer", size)) {
                HugeIO.copyFileIntoDataset(stringFile, ds);
            }
        }
        Files.deleteIfExists(stringFile);
        offsets.transferTo(strings, "offsets");
        compressed.transferTo(strings, "compressed");
    }

    /**
     * Releases the temp files. A writer closed before it was finished (a
     * failed build) encodes nothing more: batches not begun never run, and a
     * running one is waited for.
     */
    @Override
    public void close() throws IOException {
        if (!completed) {
            completed = true;
            for (PoolTask<Batch> batch : inFlight) {
                batch.abandon();
            }
            inFlight.clear();
            pending = null;
            stringOut.close();
        }
        Files.deleteIfExists(stringFile);
        offsets.close();
        compressed.close();
    }
}
