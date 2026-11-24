package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The arrays of {@code /.BG/arrays} are written a piece at a time, one pass
 * per array, so that a disk engine can write classes larger than its memory.
 * Pieces of three elements, against Zarr chunks of a few rows, must give the
 * very bytes one piece gives: in both containers, raw and compressed.
 */
class ArrayPiecesTest {

    @TempDir
    Path dir;

    static final String DATA = """
        @prefix : <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        :w1 :hours ( 8 8 7 0 9 0 0 ) .
        :w2 :hours ( 6 undef 7 7 8 0 0 ) .
        :w3 :hours ( 9 9 9 9 9 0 0 ) .
        :p1 :points ( ( 0.0E0 0.0E0 ) ( 3.0E0 0.0E0 ) ( 3.0E0 4.0E0 ) ( 1.0E0 1.0E0 ) ( 2.0E0 undef ) ) .
        :p2 :points ( ( -0.0E0 1.5E0 ) ) .
        :p3 :points ( ( 5.0E0 5.0E0 ) ( 6.0E0 6.0E0 ) ) .
        :obs1 :component ( ( "8480-6" "systolic" ( 120 "mm[Hg]" ) ) ( "8462-4" "diastolic" ( 80 "mm[Hg]" ) ) ) .
        :obs2 :component ( ( "8480-6" "systolic" ( 150 undef ) ) ( "8462-4" "diastolic" ( 95 "mm[Hg]" ) ) ( "x" "y" ( 1 "z" ) ) ) .
        :n1 :names ( "chat"@fr "cat"@en "Katze"@de "gato"@es ) .
        :n2 :names ( "dog"@en undef ) .
        :e1 :emb ( "first" ( 1.0E0 2.0E0 3.0E0 4.0E0 5.0E0 ) ) .
        :e2 :emb ( "second" ( 4.0E0 undef 6.0E0 7.0E0 8.0E0 ) ) .
        """;

    @ParameterizedTest
    @CsvSource({".h5,false", ".h5,true", ".zarr.zip,false", ".zarr.zip,true"})
    void piecesWriteTheBytesOnePieceWrites(String ext, boolean compress) throws Exception {
        Path src = dir.resolve("pieces.ttl");
        Files.writeString(src, DATA);
        Method chunkMax = Class.forName("com.ebremer.beakgraph.store.ZarrStoreWriter")
                .getDeclaredMethod("setChunkMax", long.class);
        chunkMax.setAccessible(true);
        long previousChunk = (Long) chunkMax.invoke(null, 48L);   // 6 int64s: chunks of a few rows
        int previousPiece = SequenceArrays.pieceElements;
        try {
            Path whole = dir.resolve("whole" + ext);
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(whole.toFile())
                    .setArrays(new SequenceArrays.Options(1, 2, compress)).build().write();
            SequenceArrays.pieceElements = 3;
            Path pieces = dir.resolve("pieces" + ext);
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(pieces.toFile())
                    .setArrays(new SequenceArrays.Options(1, 2, compress)).build().write();
            assertArrayEquals(Files.readAllBytes(whole), Files.readAllBytes(pieces));
        } finally {
            SequenceArrays.pieceElements = previousPiece;
            chunkMax.invoke(null, previousChunk);
        }
    }
}
