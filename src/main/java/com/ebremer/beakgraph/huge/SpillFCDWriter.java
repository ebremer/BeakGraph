package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.hdf5.DictionarySinks;
import static com.ebremer.beakgraph.Params.COMPRESSION_THRESHOLD;
import com.ebremer.beakgraph.core.lib.VByte;
import com.ebremer.beakgraph.utils.StringUtils;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Disk-backed twin of {@link com.ebremer.beakgraph.hdf5.writers.FCDWriter}: the
 * same front-coded block format (block heads, VByte prefix lengths, Zstd
 * compression above {@code COMPRESSION_THRESHOLD}, surrogate-safe prefixes) and
 * the same HDF5 group shape (stringbuffer/offsets/compressed datasets plus
 * blockSize/numBlocks/numEntries/compression_threshold attributes), but the
 * string buffer grows in a temp file, so dictionary size is bounded by disk.
 *
 * @author Erich Bremer
 */
final class SpillFCDWriter implements AutoCloseable, DictionarySinks.StringSink {

    private final String name;
    private final int blockSize;
    private int stringsInCurrentBlock = 0;
    private String prevString = null;
    private final Path stringFile;
    private final OutputStream stringOut;
    private long numBlocks = 0;
    private long numEntries = 0;
    private long position = 0;
    private final SpillDataBuffer offsets;
    private final SpillBitPackedBuffer compressed;
    private final StringUtils su = new StringUtils();
    private boolean completed = false;

    SpillFCDWriter(Path workDir, String name, int blockSize) throws IOException {
        if (blockSize < 2) {
            // Same guard as FCDWriter: blockSize 1 never closes a block and the
            // offsets dataset would be unreadable.
            throw new IllegalArgumentException("FCD blockSize must be >= 2, got " + blockSize);
        }
        this.name = name;
        this.blockSize = blockSize;
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

    private void writeFragment(byte[] data) throws IOException {
        boolean shouldCompress = data.length >= COMPRESSION_THRESHOLD;
        byte[] finalData;
        if (shouldCompress) {
            // Byte-identical to FCDWriter: decode to String, Zstd-compress that.
            finalData = su.compress(new String(data, StandardCharsets.UTF_8));
            compressed.writeLong(1);
        } else {
            finalData = data;
            compressed.writeLong(0);
        }
        position += VByte.encode(stringOut, finalData.length);
        stringOut.write(finalData);
        position += finalData.length;
    }

    public void add(String item) throws IOException {
        numEntries++;
        if (stringsInCurrentBlock == 0) {
            offsets.writeLong(position);
            writeFragment(item.getBytes(StandardCharsets.UTF_8));
            prevString = item;
            stringsInCurrentBlock = 1;
        } else {
            int prefixLength = commonPrefixLength(prevString, item);
            position += VByte.encode(stringOut, prefixLength);
            String suffix = item.substring(prefixLength);
            writeFragment(suffix.getBytes(StandardCharsets.UTF_8));
            prevString = item;
            stringsInCurrentBlock++;
            if (stringsInCurrentBlock == blockSize) {
                stringsInCurrentBlock = 0;
                numBlocks++;
            }
        }
    }

    private int commonPrefixLength(String s1, String s2) {
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

    private void complete() throws IOException {
        if (completed) return;
        completed = true;
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
        complete();
        StreamingHdf5Group strings = parent.putGroup(name);
        strings.putAttribute("blockSize", blockSize);
        long validBlocks = (stringsInCurrentBlock == 0 && numEntries > 0) ? numBlocks : numBlocks + 1;
        strings.putAttribute("numBlocks", (numEntries == 0) ? 0 : validBlocks);
        strings.putAttribute("numEntries", numEntries);
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

    @Override
    public void close() throws IOException {
        complete();
        Files.deleteIfExists(stringFile);
        offsets.close();
        compressed.close();
    }
}
