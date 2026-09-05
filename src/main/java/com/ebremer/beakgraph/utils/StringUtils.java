package com.ebremer.beakgraph.utils;

import io.airlift.compress.v3.zstdFFM.ZstdCompressor;
import io.airlift.compress.v3.zstdFFM.ZstdDecompressor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Utility class for String compression using Zstd.
 * Use as: byte[] compressed = StringUtils.compress("my data");
 */
public final class StringUtils {
    private final ZstdCompressor COMPRESSOR;
    private final ZstdDecompressor DECOMPRESSOR;
    private static final int HEADER_SIZE = 4; // To store uncompressed length
    /**
     * Most a zstd frame can expand: one block yields at most 128 KiB from a
     * 3-byte block header plus one RLE byte. A header claiming more than that
     * for the compressed bytes present is corrupt (or hostile), and is
     * rejected before the output buffer is allocated - a 2 GB {@code new byte[]}
     * from a four-byte on-disk value was otherwise reachable from one lookup.
     */
    private static final long MAX_ZSTD_EXPANSION = 131072L / 4;
    /** Hard cap on one decoded fragment (a single RDF term), tunable via beakgraph.fcd.maxFragmentBytes. */
    private static final long MAX_FRAGMENT_BYTES = Long.getLong("beakgraph.fcd.maxFragmentBytes", 256L << 20);

    /** Validates a declared uncompressed length against what {@code compressedLength} bytes can hold. */
    private long checkedLength(int uncompressedLength, byte[] compressed, int offset, int compressedLength) {
        if (uncompressedLength < 0) {
            throw new IllegalArgumentException("Invalid uncompressed length: " + uncompressedLength);
        }
        if (uncompressedLength > MAX_FRAGMENT_BYTES) {
            throw new IllegalArgumentException("Corrupt compressed fragment: declared length " + uncompressedLength
                    + " exceeds beakgraph.fcd.maxFragmentBytes (" + MAX_FRAGMENT_BYTES + ")");
        }
        if (uncompressedLength > compressedLength * MAX_ZSTD_EXPANSION) {
            throw new IllegalArgumentException("Corrupt compressed fragment: declared length " + uncompressedLength
                    + " exceeds what " + compressedLength + " compressed bytes can expand to");
        }
        // The frame header usually states the size itself; a disagreement is corruption.
        long declared = DECOMPRESSOR.getDecompressedSize(compressed, offset, compressedLength);
        if (declared >= 0 && declared != uncompressedLength) {
            throw new IllegalArgumentException("Corrupt compressed fragment: header says " + uncompressedLength
                    + " bytes, the zstd frame says " + declared);
        }
        return uncompressedLength;
    }

    /**
     * Selects the Zstd implementation via the vendored zstdFFM package (see
     * io/airlift/compress/v3/zstdFFM/README.md): the native binding when its
     * bundled library loads, otherwise the FFM-based pure-Java port. Neither
     * path touches sun.misc.Unsafe (deprecated for removal).
     */
    public StringUtils() {
        COMPRESSOR = ZstdCompressor.create();
        DECOMPRESSOR = ZstdDecompressor.create();
    }

    /**
     * Compresses a String into a byte array with a 4-byte length header.
     * @param source
     * @return 
     */
    public byte[] compress(String source) {
        if (source == null || source.isEmpty()) {
            return new byte[0];
        }
        byte[] inputBytes = source.getBytes(StandardCharsets.UTF_8);
        int uncompressedLength = inputBytes.length;
        int maxOutputLength = COMPRESSOR.maxCompressedLength(uncompressedLength);
        byte[] outputBuffer = new byte[maxOutputLength + HEADER_SIZE];
        // Store length header
        ByteBuffer.wrap(outputBuffer).putInt(uncompressedLength);
        int compressedSize = COMPRESSOR.compress(
                inputBytes, 0, uncompressedLength,
                outputBuffer, HEADER_SIZE, maxOutputLength
        );
        return Arrays.copyOf(outputBuffer, compressedSize + HEADER_SIZE);
    }

    /**
     * Decompresses a byte array (with header) back into a String.
     * @param source
     * @return 
     */
    public String decompress(byte[] source) {
        if (source == null || source.length < HEADER_SIZE) {
            // A stored compressed fragment always carries the 4-byte length header
            // (compress() writes it even for ""); anything shorter is a damaged
            // store. Answering "" here silently corrupted the front-coded chain.
            throw new IllegalArgumentException("Corrupt compressed fragment: "
                    + (source == null ? "null" : source.length + " bytes") + ", shorter than its 4-byte length header");
        }
        // Read length header to allocate exactly what we need - after checking it.
        int uncompressedLength = ByteBuffer.wrap(source).getInt();
        checkedLength(uncompressedLength, source, HEADER_SIZE, source.length - HEADER_SIZE);
        byte[] outputBuffer = new byte[uncompressedLength];
        int actualDecompressedSize = DECOMPRESSOR.decompress(
                source, HEADER_SIZE, source.length - HEADER_SIZE,
                outputBuffer, 0, uncompressedLength
        );
        if (actualDecompressedSize != uncompressedLength) {
            throw new IllegalArgumentException("Decompressed size mismatch");
        }
        return new String(outputBuffer, 0, actualDecompressedSize, StandardCharsets.UTF_8);
    }
        
    /**
     * Decompresses a Zstd-compressed string directly from a ByteBuffer.
     * Assumes the buffer's current position is at the start of the 4-byte length header.
     * * @param buffer The ByteBuffer containing the header and compressed data.
     * @param buffer
     * @return The decompressed String.
     */
    public String decompress(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < HEADER_SIZE) {
            throw new IllegalArgumentException("Corrupt compressed fragment: "
                    + (buffer == null ? "null" : buffer.remaining() + " bytes") + ", shorter than its 4-byte length header");
        }

        // 1. Read the uncompressed length header (4 bytes)
        int uncompressedLength = buffer.getInt();
        if (uncompressedLength == 0) {
            return "";
        }
        
        // 2. Prepare the input (compressed) and output (decompressed) arrays
        // We only read what is remaining in the buffer
        int compressedSize = buffer.remaining();
        byte[] compressedInput = new byte[compressedSize];
        buffer.get(compressedInput);

        checkedLength(uncompressedLength, compressedInput, 0, compressedSize);
        byte[] outputBuffer = new byte[uncompressedLength];

        // 3. Decompress
        int actualDecompressedSize = DECOMPRESSOR.decompress(
                compressedInput, 0, compressedSize,
                outputBuffer, 0, uncompressedLength
        );

        if (actualDecompressedSize != uncompressedLength) {
            throw new IllegalArgumentException("Decompressed size mismatch. Expected " + uncompressedLength + " but got " + actualDecompressedSize);
        }

        return new String(outputBuffer, 0, actualDecompressedSize, StandardCharsets.UTF_8);
    }
    
}
