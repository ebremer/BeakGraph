package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.utils.StringUtils;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.jhdf.api.WritableGroup;
import io.jhdf.api.WritableNode;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-79: the front-coded dictionary reader allocated a
 * fragment's {@code byte[]} straight from the on-disk VByte length, truncated
 * to int, before any bounds check - so a corrupt or hostile store (which the
 * reader accepts from arbitrary remote files) produced a
 * NegativeArraySizeException or a 2 GB allocation from one dictionary lookup.
 * The zstd path repeated the pattern with its four-byte uncompressed-length
 * header. Lengths are now validated against the buffer (and, for zstd, the
 * bytes present) before allocating, and reported as corruption.
 */
@Timeout(120)
class FCDCorruptionTest {

    private static final String STRINGS = Params.BG + "/" + Params.DICTIONARY + "/literals/strings";

    @TempDir
    static Path dir;
    static File store;

    @BeforeAll
    static void build() throws Exception {
        String ttl = "@prefix ex: <http://ex.org/> .\n"
                + "ex:a ex:p \"alphabet soup\" .\nex:b ex:p \"alphabetical order\" .\nex:c ex:p \"alpine meadow\" .\n"
                + "ex:d ex:p \"" + "compressible text ".repeat(40) + "\" .\n";
        File src = dir.resolve("s.ttl").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        store = dir.resolve("s.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(store).setSpatial(false).setFeatures(false).build().write();
    }

    /** VByte encoding as FCDWriter emits it: 7 bits per byte, low first, high bit marks the last byte. */
    private static byte[] vbyte(long value) {
        ByteBuffer b = ByteBuffer.allocate(10);
        while (value > 0x7F) { b.put((byte) (value & 0x7F)); value >>>= 7; }
        b.put((byte) (value | 0x80));
        byte[] out = new byte[b.position()];
        b.flip(); b.get(out);
        return out;
    }

    /** A copy of the store with {@code mutate} applied to the literals string buffer. */
    private static File corruptedCopy(String name, UnaryOperator<byte[]> mutate) throws Exception {
        File dst = dir.resolve(name + ".h5").toFile();
        try (HdfFile in = new HdfFile(store.toPath()); WritableHdfFile out = HdfFile.write(dst.toPath())) {
            copy(in, out, "", mutate);
        }
        return dst;
    }

    private static void copy(Group from, WritableGroup to, String path, UnaryOperator<byte[]> mutate) {
        for (Node child : from.getChildren().values()) {
            String childPath = path.isEmpty() ? child.getName() : path + "/" + child.getName();
            if (child instanceof Group g) {
                WritableGroup wg = to.putGroup(g.getName());
                copyAttributes(g, wg);
                copy(g, wg, childPath, mutate);
            } else if (child instanceof Dataset d) {
                Object data = d.getData();
                if (childPath.equals(STRINGS + "/stringbuffer")) {
                    data = mutate.apply((byte[]) data);
                }
                copyAttributes(d, to.putDataset(d.getName(), data));
            }
        }
    }

    private static void copyAttributes(Node from, WritableNode to) {
        for (Map.Entry<String, Attribute> e : from.getAttributes().entrySet()) {
            to.putAttribute(e.getKey(), e.getValue().getData());
        }
    }

    private static long firstBlockOffset() {
        try (HdfFile f = new HdfFile(store.toPath())) {
            byte[] offsets = (byte[]) ((Dataset) f.getByPath(STRINGS + "/offsets")).getData();
            return ByteBuffer.wrap(offsets, 0, 8).getLong();
        }
    }

    private static byte[] overwrite(byte[] buf, long at, byte[] with) {
        byte[] out = buf.clone();
        System.arraycopy(with, 0, out, (int) at, with.length);
        return out;
    }

    private static FCDReader reader(File h5) {
        HdfFile f = new HdfFile(h5.toPath());   // kept open for the reader's lifetime (test scope)
        return new FCDReader((Group) f.getByPath(STRINGS));
    }

    @Test
    void oversizedFragmentLengthIsReportedAsCorruption() throws Exception {
        long start = firstBlockOffset();
        File h5 = corruptedCopy("huge-len", buf -> overwrite(buf, start, vbyte(0x7FFF_FFFFL)));
        FCDReader r = reader(h5);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> r.get(0));
        assertTrue(ex.getMessage().contains("fragment length 2147483647"), ex.getMessage());
    }

    @Test
    void lengthOverflowingIntIsReportedAsCorruption() throws Exception {
        // 2^31 truncated to int is negative: new byte[-2147483648] before the fix.
        long start = firstBlockOffset();
        File h5 = corruptedCopy("neg-len", buf -> overwrite(buf, start, vbyte(1L << 31)));
        FCDReader r = reader(h5);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> r.get(0));
        assertTrue(ex.getMessage().contains("fragment length 2147483648"), ex.getMessage());
    }

    @Test
    void oversizedPrefixLengthIsReportedAsCorruption() throws Exception {
        long start = firstBlockOffset();
        byte[] original;
        try (HdfFile f = new HdfFile(store.toPath())) {
            original = (byte[]) ((Dataset) f.getByPath(STRINGS + "/stringbuffer")).getData();
        }
        // First fragment: one-byte length (short string), then its bytes; the
        // second entry's shared-prefix VByte follows.
        int len = original[(int) start] & 0x7F;
        assertTrue((original[(int) start] & 0x80) != 0, "fixture's first length is a single VByte byte");
        long prefixAt = start + 1 + len;
        File h5 = corruptedCopy("huge-prefix", buf -> overwrite(buf, prefixAt, vbyte(0x7FFF_FFF0L)));
        FCDReader r = reader(h5);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> r.get(1));
        assertTrue(ex.getMessage().contains("prefix length"), ex.getMessage());
    }

    @Test
    void intactStoreStillDecodes() {
        FCDReader r = reader(store);
        assertEquals("alphabet soup", r.get(0));
        assertEquals("alphabetical order", r.get(1));
        assertEquals("alpine meadow", r.get(2));
        assertTrue(r.get(3).startsWith("compressible text "));
    }

    @Test
    void decompressRejectsAnOversizedHeaderBeforeAllocating() {
        StringUtils su = new StringUtils();
        byte[] hostile = ByteBuffer.allocate(12).putInt(0x7FFF_FFF0).put(new byte[8]).array();
        long t0 = System.nanoTime();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> su.decompress(hostile));
        assertTrue((System.nanoTime() - t0) < 2_000_000_000L, "must fail on the header, not after a 2 GB allocation");
        assertTrue(ex.getMessage().contains("Corrupt compressed fragment"), ex.getMessage());
        assertThrows(IllegalArgumentException.class, () -> su.decompress(ByteBuffer.wrap(hostile)));

        // A genuine round trip is untouched, and a lying header on real data is caught.
        String text = "x".repeat(500) + "y".repeat(500);
        byte[] good = su.compress(text);
        assertEquals(text, su.decompress(good));
        byte[] lying = good.clone();
        ByteBuffer.wrap(lying).putInt(text.length() + 1);
        assertThrows(IllegalArgumentException.class, () -> su.decompress(lying));
    }

    @Test
    void truncatedCompressedPayloadIsCorruptionNotEmptyString() {
        // BG-165: fewer than 4 bytes used to decode as "" and silently corrupt
        // the front-coded chain (an empty suffix) instead of failing.
        StringUtils su = new StringUtils();
        assertThrows(IllegalArgumentException.class, () -> su.decompress(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> su.decompress(new byte[]{1, 2, 3}));
        assertThrows(IllegalArgumentException.class, () -> su.decompress(ByteBuffer.wrap(new byte[]{1, 2})));
    }

    @Test
    void prefixOnePastThePreviousStringIsCorruptionNotPadding() throws Exception {
        // BG-80: "alphabet soup" is 13 chars; a shared prefix of 14 has no
        // meaning. StringBuilder.setLength(14) would silently NUL-pad and the
        // rest of the block would inherit the damage.
        long start = firstBlockOffset();
        byte[] original;
        try (HdfFile f = new HdfFile(store.toPath())) {
            original = (byte[]) ((Dataset) f.getByPath(STRINGS + "/stringbuffer")).getData();
        }
        int len = original[(int) start] & 0x7F;
        long prefixAt = start + 1 + len;
        assertEquals("alphabet soup".length(), len, "fixture: the first entry is the shortest string");
        assertTrue((original[(int) prefixAt] & 0x80) != 0, "fixture: the second entry's prefix is one VByte byte");
        File h5 = corruptedCopy("prefix-plus-one", buf -> overwrite(buf, prefixAt, vbyte(len + 1)));
        FCDReader r = reader(h5);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> r.get(1));
        assertTrue(ex.getMessage().contains("prefix length " + (len + 1)), ex.getMessage());
        assertTrue(ex.getMessage().contains("exceeds the previous string's " + len), ex.getMessage());
        // The intact copy decodes the same entry without a NUL anywhere.
        assertEquals(-1, reader(store).get(1).indexOf('\u0000'));
    }
}
