package com.ebremer.beakgraph.utils;

import io.airlift.compress.v3.zstd.ZstdJavaCompressor;
import io.airlift.compress.v3.zstd.ZstdJavaDecompressor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Utility class for String compression using Zstd.
 * Use as: byte[] compressed = StringUtils.compress("my data");
 */
public final class StringUtils {
    private final ZstdJavaCompressor COMPRESSOR = new ZstdJavaCompressor();
    private final ZstdJavaDecompressor DECOMPRESSOR = new ZstdJavaDecompressor();
    private static final int HEADER_SIZE = 4; // To store uncompressed length

    public StringUtils() {}

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
            return "";
        }
        // Read length header to allocate exactly what we need
        int uncompressedLength = ByteBuffer.wrap(source).getInt();
        if (uncompressedLength < 0) {
            throw new IllegalArgumentException("Invalid uncompressed length");
        }
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
            return "";
        }

        // 1. Read the uncompressed length header (4 bytes)
        int uncompressedLength = buffer.getInt();
        if (uncompressedLength == 0) {
            return "";
        }
        
        if (uncompressedLength < 0) {
            throw new IllegalArgumentException("Invalid uncompressed length: " + uncompressedLength);
        }

        // 2. Prepare the input (compressed) and output (decompressed) arrays
        // We only read what is remaining in the buffer
        int compressedSize = buffer.remaining();
        byte[] compressedInput = new byte[compressedSize];
        buffer.get(compressedInput);

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
